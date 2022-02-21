# distributed algorithms, n.dulay, 8 feb 2022
# coursework, raft consensus, v2
# modified by Samuel Adekunle (sja119) and Neel Dugar (nd419), 20 feb 2022

defmodule AppendEntries do

  # s = server process state (c.f. this/self)

  # Assumes you're a leader
  def handle_timeout(s, term, followerP) do
    {entries, prev_log_index} =
      if Log.last_index(s) >= s.next_index[followerP] do
        new_entries =
          for index <- s.next_index[followerP]..Log.last_index(s), into: Map.new, do: {index, Log.entry_at(s, index)}
        {new_entries, s.next_index[followerP] - 1}
      else
        {%{}, 0}
      end

    s
    |> send_append_entries_request(entries, prev_log_index, followerP)
    |> Timer.restart_append_entries_timer(followerP)
  end

  # _________________________________________________________ send_append_entries_request
  defp send_append_entries_request(s, entries, prev_log_index, followerP) do
    send followerP, {
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
  end # send_append_entries_request

  # _________________________________________________________ send_append_entries_request
  defp send_append_entries_reply(s, msg, success) do
    send msg.leaderP, {
      :APPEND_ENTRIES_REPLY,
      %{
        followerP: s.selfP,
        term: s.curr_term,
        success: success
      }
    }
  end # send_append_entries_request


  # _________________________________________________________ handle_request_send_reply
  def handle_request_send_reply(s, msg) do
    leader_term_behind = msg.term < s.curr_term
    entry_at_prev_index = Log.entry_at(s, msg.prev_log_index)
    prev_log_mismatch = (entry_at_prev_index != nil) and (entry_at_prev_index.term != msg.prev_log_term)

    if leader_term_behind or prev_log_mismatch do
      s
      |> handle_heartbeat(msg.leaderP, max(msg.term, s.curr_term))
      |> send_append_entries_reply(msg, false)
    else
      s
      |> handle_append_entries_request(msg)
      |> handle_heartbeat(msg.leaderP, msg.term)
      |> send_append_entries_reply(msg, true)
    end
  end # handle_request_send_reply

  defp handle_append_entries_request(s, msg) do
    conflicting_index = Enum.find(
      Enum.to_list(msg.prev_log_index + 1..Log.last_index(s)),
      fn index -> Log.term_at(s, index) != msg.entries[index].term end
    )

    s.log = Log.delete_entries_from(s, conflicting_index)
    s.log = Log.merge_entries(s, msg.entries)

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