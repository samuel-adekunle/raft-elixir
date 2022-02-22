# distributed algorithms, n.dulay, 8 feb 2022
# coursework, raft consensus, v2
# modified by Samuel Adekunle (sja119) and Neel Dugar (nd419), 20 feb 2022

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
      # Crash request
      {:CRASH, duration} ->
        s
        |> crash(duration)

      # Append Entries request
      {:APPEND_ENTRIES_REQUEST, msg} ->
        s
        |> AppendEntries.handle_request_send_reply(msg)


      # Append Entries timeout when leader
      {:APPEND_ENTRIES_TIMEOUT, {term, followerP}} when s.role == :LEADER ->
        s
        |> AppendEntries.handle_timeout(term, followerP)

      # Append Entries timeout when not leader
      {:APPEND_ENTRIES_TIMEOUT, _msg} when s.role != :LEADER ->
        s
        |> Timer.cancel_all_append_entries_timers()

      # Append Entries when leader
      {:APPEND_ENTRIES_REPLY, msg} when s.role == :LEADER ->
        s
        |> AppendEntries.handle_append_entries_reply(msg)

      # Append Entries after eviction
      {:APPEND_ENTRIES_REPLY, _msg} when s.role != :LEADER -> s

      # Vote Request when not leader
      {:VOTE_REQUEST, msg} ->
        s
        |> Vote.handle_request_send_reply(msg)

      # Vote reply
      {:VOTE_REPLY, vote} ->
        s
        |> Vote.handle_vote_reply(vote)

      # Election timeout when follower or candidate
      {:ELECTION_TIMEOUT, _msg} when s.role != :LEADER ->
        s
        |> Vote.send_vote_request()

      # Election timeout when leader
      {:ELECTION_TIMEOUT, _msg} when s.role == :LEADER ->
        s
        |> Timer.cancel_election_timer()

      # Client Request
      {:CLIENT_REQUEST, msg} ->
        s
        |> ClientReq.handle_request_send_reply(msg)

      # DB Reply, ignored for now
      {:DB_REPLY, :OK} ->
        s = s
            |> State.last_applied(s.last_applied + 1)

        if s.role == :LEADER do
          client = Log.entry_at(s, s.last_applied).request
          send client.clientP, {:CLIENT_REPLY, {client.cid, :OK, s.selfP}}
        end
        s

      unexpected ->
        Helper.node_halt(inspect unexpected)
        s

    end # receive

    Server.next(s)

  end # next

  # _________________________________________________________ broadcast
  def broadcast(s, message) do
    print(s, "#{s.server_num} broadcasts #{inspect message}}")

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
    s = s
        |> print("#{s.server_num} crashing for #{duration}ms")
        |> Timer.cancel_election_timer()
        |> Timer.cancel_crash_timer()
    Process.sleep duration
    s
  end # crash

end # Server

