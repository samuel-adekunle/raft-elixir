# distributed algorithms, n.dulay, 8 feb 2022
# coursework, raft consensus, v2
# modified by Samuel Adekunle (sja119) and Neel Dugar (nd419), 20 feb 2022

defmodule AppendEntries do

  # s = server process state (c.f. this/self)
  def handle_append_entries_reply(s, msg) do
    if msg.success do
      s =
        s
        |> State.next_index(msg.followerP, msg.last_index + 1)
        |> State.match_index(msg.followerP, msg.last_index)

      # check for majority and commit and then send to the client if we've commit it
      # If there exists an N such that N > commitIndex, a majority
      # of matchIndex[i] ≥ N, and log[N].term == currentTerm:
      # set commitIndex = N (§5.3, §5.4).
      new_commit_index = Enum.reduce_while(
        s.commit_index + 1..Log.last_index(s)//1,
        s.commit_index,
        fn current_index, acc ->
          if commit(s, current_index), do: {:cont, current_index}, else: {:halt, acc}
        end
      )

      for index <- s.commit_index + 1..new_commit_index//1 do
        send s.databaseP, {:DB_REQUEST, Log.entry_at(s, index).request}
      end

      s
      |> State.commit_index(new_commit_index)
      |> handle_timeout(s.curr_term, msg.followerP)
    else
      if msg.term > s.curr_term do
        # step down
        Server.print(s, "#{s.server_num} is evicted")
        s
        |> State.role(:FOLLOWER)
        |> Timer.cancel_all_append_entries_timers()
        |> Timer.restart_election_timer()
      else
        # send app entries req with dec. nextIndex
        s = s
            |> State.next_index(msg.followerP, s.next_index[msg.followerP] - 1)
        handle_timeout(s, s.curr_term, msg.followerP)
      end
    end
  end

  # try to commit an index
  defp commit(s, index) do
    count = Enum.reduce(
      s.match_index,
      0,
      fn {_, m_index}, acc ->
        if m_index >= index, do: acc + 1, else: acc
      end
    )
    is_maj_replicated = count >= s.majority
    is_of_term = Log.term_at(s, index) == s.curr_term
    is_maj_replicated and is_of_term
  end

  # Assumes you're a leader
  def handle_timeout(s, _term, followerP) do
    #    Server.print(s, "#{s.server_num}'s log #{inspect s.log}, next index is #{inspect s.next_index}")
    {entries, prev_log_index} =
      if Log.last_index(s) >= s.next_index[followerP] do
        new_entries =
          for index <- s.next_index[followerP]..Log.last_index(s), into: Map.new, do: {index, Log.entry_at(s, index)}
        {new_entries, s.next_index[followerP] - 1}
      else
        {%{}, s.next_index[followerP] - 1}
      end

    if map_size(entries) > 0 do
      Server.print(s, "#{s.server_num} has next index #{inspect s.next_index}")
    end

    s
    |> send_append_entries_request(entries, prev_log_index, followerP)
    |> Timer.restart_append_entries_timer(followerP)
  end

  # _________________________________________________________ send_append_entries_request
  defp send_append_entries_request(s, entries, prev_log_index, followerP) do
    if map_size(entries) > 0 do
      Server.print(s, "#{s.server_num} sends append #{inspect entries} from index #{prev_log_index}")
    end
    request = {
      :APPEND_ENTRIES_REQUEST,
      %{
        entries: entries,
        term: s.curr_term,
        leaderP: s.selfP,
        leader_commit_index: s.commit_index,
        prev_log_index: prev_log_index,
        prev_log_term: Log.term_at(s, prev_log_index)
      }
    }
    send followerP, request
    s
  end # send_append_entries_request

  # _________________________________________________________ send_append_entries_request
  defp send_append_entries_reply(s, msg, success) do
    send msg.leaderP, {
      :APPEND_ENTRIES_REPLY,
      %{
        followerP: s.selfP,
        term: s.curr_term,
        last_index: Log.last_index(s),
        success: success
      }
    }
    s
  end # send_append_entries_request


  # _________________________________________________________ handle_request_send_reply
  def handle_request_send_reply(s, msg) do
    leader_term_behind = msg.term < s.curr_term
    entry_at_prev_index = Log.entry_at(s, msg.prev_log_index)
    prev_log_mismatch = (entry_at_prev_index != nil) and (entry_at_prev_index.term != msg.prev_log_term)

    if leader_term_behind do
      s
      |> send_append_entries_reply(msg, false)
    else
      if prev_log_mismatch do
        s
        |> handle_heartbeat(msg.leaderP, max(msg.term, s.curr_term))
        |> send_append_entries_reply(msg, false)
      else
        s
        |> handle_append_entries_request(msg)
        |> handle_heartbeat(msg.leaderP, msg.term)
        |> send_append_entries_reply(msg, true)
      end
    end
  end # handle_request_send_reply

  defp handle_append_entries_request(s, msg) do
    if map_size(msg.entries) > 0 do
      Server.print(s, "#{s.server_num} received #{inspect msg}")
      Server.print(s, "#{s.server_num} has log #{inspect s.log}")
    end

    conflicting_index = Enum.find(
      Enum.to_list(msg.prev_log_index + 1..Log.last_index(s)//1),
      fn index -> Log.term_at(s, index) != msg.entries[index].term end
    )
    s =
      if conflicting_index != nil do
        s
        |> Log.delete_entries_from(conflicting_index)
      else
        s
      end
      |> Log.merge_entries(msg.entries)

    if msg.leader_commit_index > s.commit_index do
      new_commit_index = min(msg.leader_commit_index, Log.last_index(s))

      for index <- s.commit_index + 1..new_commit_index do
        send s.databaseP, {:DB_REQUEST, Log.entry_at(s, index).request}
      end

      s
      |> State.commit_index(new_commit_index)
    else
      s
    end
  end

  # _________________________________________________________ handle_heartbeat
  defp handle_heartbeat(s, leaderP, leader_term) do
    s = case s.role do
      :LEADER ->
        s
        |> Timer.cancel_all_append_entries_timers()
        |> Server.print("#{s.server_num} got evicted")

      :CANDIDATE ->
        s
        |> Server.print("#{s.server_num} steps down from election")

      :FOLLOWER ->
        s
    end

    s
    |> State.role(:FOLLOWER)
    |> State.curr_term(leader_term)
    |> State.leaderP(leaderP)
    |> Timer.restart_election_timer()
  end # handle_heartbeat

end # AppendEntries
