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
        State.initialise(config, server_num, servers, databaseP)
        |> Timer.start_crash_timer()
        |> Timer.restart_election_timer()
        |> Server.next()
    end # receive
  end # start

  # _________________________________________________________ next()
  def next(s) do
    s = receive do
      # Broadcast heartbeat request when leader
      {:SEND_HEARTBEAT} when s.role == :LEADER ->
        s
        |> broadcast({:HEARTBEAT_REQUEST, s.selfP, s.curr_term})
        |> Timer.restart_heartbeat_timer()

      # Broadcast heartbeat when not leader
      {:SEND_HEARTBEAT} when s.role != :LEADER ->
        s
        |> Timer.cancel_heartbeat_timer()

      # Heartbeat request when not leader
      {:HEARTBEAT_REQUEST, leaderP, leader_term} when s.role != :LEADER ->
        if s.curr_term > leader_term do
          s
        else
          if s.role == :CANDIDATE do
            print(s, "#{s.server_num} steps down from election")
          end

          send s.leaderP, {:HEARTBEAT_REPLY, s.selfP}

          s
          |> State.role(:FOLLOWER)
          |> State.curr_term(leader_term)
          |> State.leaderP(leaderP)
          |> Timer.restart_election_timer()
        end

      # Heartbeat request when leader
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
              |> print("#{s.server_num} got evicted")

          send s.leaderP, {:HEARTBEAT_REPLY, s.selfP}
          s
        end

      # Heartbeat reply when leader
      # Crashes if received as candidate or follower
      {:HEARTBEAT_REPLY, _serverP} when s.role == :LEADER -> s

      # Crash request
      {:CRASH, duration} ->
        s
        |> crash(duration)
        |> print("#{s.server_num} crashing for #{duration}ms")
        |> Timer.cancel_crash_timer()

      # Append Entries request when not leader
      # Crashes if received as a leader
      # TODO
      {:APPEND_ENTRIES_REQUEST, _msg} when s.role != :LEADER -> s

      # Append Entries reply when leader
      # Crashes if received as candidate or follower
      # TODO
      {:APPEND_ENTRIES_REPLY, _msg} when s.role == :LEADER -> s

      # Append Entries timeout when leader
      # Crashes if received as candidate or follower
      # TODO
      {:APPEND_ENTRIES_TIMEOUT, _msg} when s.role == :LEADER -> s

      # Vote Request when not leader
      # TODO: Check against log index -> (voted_for = NIL MUST DO)
      {:VOTE_REQUEST, msg} when s.role != :LEADER ->
        case {msg.candidate_term, s.voted_for} do
          {c_term, _} when c_term > s.curr_term ->
            s = s
                |> State.curr_term(c_term)
                |> State.voted_for(msg.candidateP)
                |> Timer.restart_election_timer()
                |> print("#{s.server_num} votes for #{msg.debugC}")

            send(msg.candidateP, {:VOTE_REPLY, %{voteP: s.selfP, curr_term: c_term, debugV: s.server_num}})
            s

          {c_term, nil} when c_term == s.curr_term ->
            s = s
                |> State.voted_for(msg.candidateP)
                |> Timer.restart_election_timer()
                |> print("#{s.server_num} votes for #{msg.debugC}")

            send(msg.candidateP, {:VOTE_REPLY, %{voteP: s.selfP, curr_term: c_term, debugV: s.server_num}})
            s

          _ -> s
        end

      # Vote Request when leader
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
          s
          |> State.role(:LEADER)
          |> Timer.restart_heartbeat_timer()
          |> print("#{s.server_num} won election")
        else
          s
        end

      # Vote reply when not candidate
      {:VOTE_REPLY, _msg} when s.role != :CANDIDATE -> s

      # Election timeout when follower
      {:ELECTION_TIMEOUT, _msg} when s.role == :FOLLOWER ->
        # Start a new election
        s
        |> State.inc_term()
        |> State.role(:CANDIDATE)
        |> State.new_voted_by()
        |> State.add_to_voted_by(s.selfP)
        |> State.voted_for(s.selfP)
        |> Timer.restart_election_timer()
        |> print("#{s.server_num} stands for election")
        |> broadcast({:VOTE_REQUEST, %{candidateP: s.selfP, candidate_term: s.curr_term, debugC: s.server_num}})

      # Election timeout when candidate
      {:ELECTION_TIMEOUT, _msg} when s.role == :CANDIDATE ->
        s
        |> State.role(:FOLLOWER)
        |> Timer.restart_election_timer()
        |> print("#{s.server_num} steps down from election")

      # Election timeout when leader
      {:ELECTION_TIMEOUT, _msg} when s.role == :LEADER ->
        s
        |> Timer.cancel_election_timer()

      # Client Request to leader
      # TODO
      {:CLIENT_REQUEST, _msg} when s.role == :LEADER -> s

      # Client Request to follower
      # TODO
      {:CLIENT_REQUEST, _msg} when s.role == :FOLLOWER -> s

      # Client Request to candidate
      # TODO
      {:CLIENT_REQUEST, _msg} when s.role == :CANDIDATE -> s

      unexpected ->
        Helper.node_halt(inspect unexpected)
        s

    end # receive

    Server.next(s)

  end # next

  # _________________________________________________________ broadcast
  def broadcast(s, message) do
    for server when server != s.selfP <- s.servers do
      send server, message
    end
    s
  end # broadcast

  # _________________________________________________________ print
  def print(s, message) do
    s
    |> Monitor.send_msg({:PRINT, s.curr_term, "- #{message}"})
  end # print

  # _________________________________________________________ crash
  def crash(s, duration) do
    Process.sleep duration
    s
  end # crash

end # Server

