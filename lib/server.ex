# distributed algorithms, n.dulay, 8 feb 2022
# coursework, raft consensus, v2
# modified by Samuel Adekunle (sja119) and Neel Dugar (nd1412), 20 feb 2022

defmodule Server do

  # s = server process state (c.f. self/this)

  # _________________________________________________________ Server.start()
  def start(config, server_num) do
    config = config
             |> Configuration.node_info("Server", server_num)
             |> Debug.node_starting()

    receive do
      {:BIND, servers, databaseP} ->
        s = State.initialise(config, server_num, servers, databaseP)
            |> Timer.restart_election_timer()

        {crash_time, crash_duration} = config.crash_servers[server_num]

        if crash_time != nil do
          Process.send_after(s.selfP, {:CRASH, crash_duration}, crash_time)
        end

        Server.next(s)
    end # receive
  end # start

  # _________________________________________________________ next()
  def next(s) do
    s = receive do
      # Broadcast heartbeat request when leader
      {:SEND_HEARTBEAT} when s.role == :LEADER ->
        for server when server != s.selfP <- s.servers do
          send server, {:HEARTBEAT_REQUEST, s.selfP, s.curr_term}
        end
        s
        |> Timer.restart_heartbeat_timer()

      # Broadcast heartbeat when no longer leader, cancel heartbeat timer
      {:SEND_HEARTBEAT} when s.role != :LEADER ->
        s
        |> Timer.cancel_heartbeat_timer()

      # Heartbeat request from leader when not leader
      # Should step down if candidate
      {:HEARTBEAT_REQUEST, leaderP, leader_term} when s.role != :LEADER ->
        if s.curr_term > leader_term do
          s
        else
          old_s = s
          s = s
              |> State.role(:FOLLOWER)
              |> State.curr_term(leader_term)
              |> State.leaderP(leaderP)
              |> Timer.restart_election_timer()

          send s.leaderP, {:HEARTBEAT_REPLY, s.selfP}

          if old_s.role == :CANDIDATE do
            Monitor.send_msg(old_s, {:PRINT, old_s.curr_term, ": #{old_s.server_num} steps down from election"})
          end
          s
        end

      # Heartbeat request when leader
      # Should decide if it is the true leader or not
      {:HEARTBEAT_REQUEST, leaderP, leader_term} when s.role == :LEADER ->
        if s.curr_term > leader_term do
          s
        else
          s = s
              |> State.role(:FOLLOWER)
              |> State.curr_term(leader_term)
              |> State.leaderP(leaderP)
              |> Timer.cancel_election_timer()
              |> Timer.restart_election_timer()
          send s.leaderP, {:HEARTBEAT_REPLY, s.selfP}
          Monitor.send_msg(s, {:PRINT, s.curr_term, ": #{s.server_num} got evicted"})
          s
        end

      # Heartbeat reply from followers which is ignored
      # Crashes if received as candidate or follower
      {:HEARTBEAT_REPLY, serverP} when s.role == :LEADER ->
        s

      # Append Entries request from leader
      # Crashes if received as a leader
      # Should step down if candidate
      # TODO
      {:APPEND_ENTRIES_REQUEST, msg} when s.role != :LEADER ->
        s

      # Append Entries reply from followers
      # Crashes if received as candidate or follower
      # TODO
      {:APPEND_ENTRIES_REPLY, msg} when s.role == :LEADER ->
        s

      # Append Entries timeout as leader
      # Crashes if received as candidate or follower
      # TODO
      {:APPEND_ENTRIES_TIMEOUT, msg} when s.role == :LEADER ->
        s

      # Vote Request from Candidate when not leader
      # TODO: Check against log index -> (voted_for = NIL MUST DO)
      {:VOTE_REQUEST, msg} when s.role != :LEADER ->
        case {msg.candidate_term, s.voted_for} do
          {c_term, _} when c_term > s.curr_term ->
            s = s
                |> State.curr_term(c_term)
                |> State.voted_for(msg.candidateP)
                |> Timer.restart_election_timer()
            send(msg.candidateP, {:VOTE_REPLY, %{voteP: s.selfP, curr_term: c_term, debugV: s.server_num}})
            Monitor.send_msg(s, {:PRINT, s.curr_term, ": #{s.server_num} votes for #{msg.debugC}"})
            s
          {c_term, nil} when c_term == s.curr_term ->
            s = s
                |> State.voted_for(msg.candidateP)
                |> Timer.restart_election_timer()
            send(msg.candidateP, {:VOTE_REPLY, %{voteP: s.selfP, curr_term: c_term, debugV: s.server_num}})
            Monitor.send_msg(s, {:PRINT, s.curr_term, ": #{s.server_num} votes for #{msg.debugC}"})
            s
          _ ->
            s
        end

      # Vote Request from Candidate as leader
      {:VOTE_REQUEST, msg} when s.role == :LEADER ->
        send msg.candidateP, {:HEARTBEAT_REQUEST, s.selfP, s.curr_term}
        s

      # Vote reply when candidate
      {:VOTE_REPLY, msg} when s.role == :CANDIDATE ->
        s = if msg.curr_term == s.curr_term do
          s
          |> State.add_to_voted_by(msg.voteP)
        else
          s
        end

        if State.vote_tally(s) >= s.majority do
          s = s
              |> State.role(:LEADER)
              |> Timer.restart_heartbeat_timer()
          Monitor.send_msg(s, {:PRINT, s.curr_term, ": #{s.server_num} won election"})
          s
        else
          s
        end

      # Vote reply when no longer candidate, ignore
      {:VOTE_REPLY, msg} when s.role != :CANDIDATE ->
        s

      # Election timeout when follower
      {:ELECTION_TIMEOUT, msg} when s.role == :FOLLOWER ->
        # Start a new election
        s = s
            |> State.inc_term()
            |> State.role(:CANDIDATE)
            |> State.new_voted_by()
            |> Timer.restart_election_timer()

        # Broadcast message to all servers
        for server <- s.servers do
          send server, {:VOTE_REQUEST, %{candidateP: s.selfP, candidate_term: s.curr_term, debugC: s.server_num}}
        end

        Monitor.send_msg(s, {:PRINT, s.curr_term, ": #{s.server_num} stands for election"})
        s

      # Election timeout when candidate
      {:ELECTION_TIMEOUT, msg} when s.role == :CANDIDATE ->
        s = s
            |> State.role(:FOLLOWER)
            |> Timer.restart_election_timer()

        Monitor.send_msg(s, {:PRINT, s.curr_term, ": #{s.server_num} steps down from election"})


      # Election timeout when leader, ignore
      {:ELECTION_TIMEOUT, msg} when s.role == :LEADER ->
        s

      # TODO: everything below here

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
      {:CLIENT_REQUEST, msg} when s.role == :CANDIDATE ->
        s

      # TODO: everything above here

      {:CRASH, duration} ->
        Helper.node_restart_after("#{s.server_num}", duration)
        s

      unexpected ->
        Helper.node_halt(inspect unexpected)
        s

    end # receive

    Server.next(s)

  end # next

  # _________________________________________________________ send_

end # Server

