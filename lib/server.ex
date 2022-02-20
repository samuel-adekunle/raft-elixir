# distributed algorithms, n.dulay, 8 feb 2022
# coursework, raft consensus, v2

defmodule Server do

  # s = server process state (c.f. self/this)

  # _________________________________________________________ Server.start()
  def start(config, server_num) do
    config = config
             |> Configuration.node_info("Server", server_num)
             |> Debug.node_starting()

    receive do
      {:BIND, servers, databaseP} ->
        State.initialise(config, server_num, servers, databaseP)
        |> Timer.restart_election_timer()
        |> Server.next()
    end # receive
  end # start

  # _________________________________________________________ next()
  def next(s) do
    s = receive do
      # Broadcast heartbeat as leader
      {:SEND_HEARTBEAT} when s.role == :LEADER ->
        for server when server != s.selfP <- s.servers do
          send server, {:HEARTBEAT_REQUEST, s.selfP}
        end
        Process.send_after(s.selfP, {:SEND_HEARTBEAT}, s.config.heartbeat_interval)
        s

      # Heartbeat request from leader
      {:HEARTBEAT_REQUEST, leaderP} ->
        if s.role != :FOLLOWER do        end

          Monitor.send_msg(s, {:PRINT, s.curr_term, "#{s.server_num} stepping down"})
        s = s
            |> State.role(:FOLLOWER)
            |> State.leaderP(leaderP)
            |> Timer.restart_election_timer()

        send s.leaderP, {:HEARTBEAT_REPLY, s.selfP}
        s

      # Heartbeat reply from followers
      {:HEARTBEAT_REPLY, serverP} -> s

      # Append Entries request from leader
      {:APPEND_ENTRIES_REQUEST, msg} -> s

      {:APPEND_ENTRIES_REPLY, msg} -> s

      # Vote Request from Candidate
      {:VOTE_REQUEST, msg} ->
        # TODO: Check against log index -> (voted_for = NIL MUST DO)
        Monitor.send_msg(
          s,
          {
            :PRINT,
            s.curr_term,
            ": #{s.server_num}-> received votereq from #{inspect msg.candidateP} for term #{msg.candidate_term}}"
          }
        )

        case {msg.candidate_term, s.voted_for} do
          {c_term, _} when c_term > s.curr_term ->
            Monitor.send_msg(s, {:PRINT, s.curr_term, ": #{s.server_num}-> votes for #{inspect msg.candidateP}"})
            send(msg.candidateP, {:VOTE_REPLY, %{voteP: s.selfP, curr_term: c_term}})
            s
            |> State.curr_term(c_term)
            |> State.voted_for(msg.candidateP)
            |> Timer.restart_election_timer()
          {c_term, nil} when c_term == s.curr_term ->
            send(msg.candidateP, {:VOTE_REPLY, %{voteP: s.selfP, curr_term: c_term}})
            s
            |> State.voted_for(msg.candidateP)
            |> Timer.restart_election_timer()
          _ ->
            s
        end

      {:VOTE_REPLY, msg} when s.role == :CANDIDATE ->
        s = if msg.curr_term == s.curr_term do
          Monitor.send_msg(s, {:PRINT, s.curr_term, "#{s.server_num} voted for by #{inspect msg.voteP}"})
          s
          |> State.add_to_voted_by(msg.voteP)
        else
          s
        end
        if State.vote_tally(s) >= s.majority do
          # leadership achieved start heartbeat
          Process.send_after(s.selfP, {:SEND_HEARTBEAT}, s.config.heartbeat_interval)
          Monitor.send_msg(s, {:PRINT, s.curr_term, "won by #{s.server_num}"})
        end
        s
        |> State.role(:LEADER)

      {:VOTE_REPLY, msg} ->
        s

      # Followers election timer expires
      {:ELECTION_TIMEOUT, msg} when s.role == :FOLLOWER ->
        # Start a new election
        s = s
            |> State.inc_term()
            |> State.role(:CANDIDATE)
            |> State.voted_for(s.selfP)
            |> State.new_voted_by()
            |> State.add_to_voted_by(s.selfP)
            |> Timer.restart_election_timer()
        Monitor.send_msg(s, {:PRINT, s.curr_term, "#{s.server_num} standing for election"})
        # Broadcast message to all servers
        for server when server != s.selfP <- s.servers do
          send server, {:VOTE_REQUEST, %{candidateP: s.selfP, candidate_term: s.curr_term}}
        end
        s

      {:ELECTION_TIMEOUT, msg} when s.role == :CANDIDATE ->
        Monitor.send_msg(s, {:PRINT, s.curr_term, "#{s.server_num} dropping down from election"})
        s
        |> State.role(:FOLLOWER)
        |> Timer.restart_election_timer()

      {:ELECTION_TIMEOUT, msg} when s.role == :LEADER -> s

      {:APPEND_ENTRIES_TIMEOUT, msg} -> s

      # Client Request to leader
      {:CLIENT_REQUEST, msg} when s.role == :LEADER ->

        # Add message to Log
        s = s
            |> Log.append_entry(msg)

        # Broadcast message to all followers
        for server when server != s.selfP <- s.servers do
          send server, {:APPEND_ENTRIES_REQUEST, msg}
        end

        # TODO: incomplete function (also handle candidate and follower)

        s

      # Client Request to follower
      {:CLIENT_REQUEST, msg} when s.role == :FOLLOWER ->
        send msg.clientP, {msg.cid, :NOT_LEADER, s.leaderP}
        s

      # Client Request to candidate gets dropped (or possibly queued)
      {:CLIENT_REQUEST, msg} when s.role == :CANDIDATE -> s

      unexpected ->
        Helper.node_halt(inspect unexpected)
        s

    end # receive

    Server.next(s)

  end # next

  # _________________________________________________________ send_

end # Server

