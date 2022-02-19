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

      {:APPEND_ENTRIES_REQUEST, msg} -> s

      {:APPEND_ENTRIES_REPLY, msg} -> s

      {:VOTE_REQUEST, msg} -> s

      {:VOTE_REPLY, msg} -> s

      {:ELECTION_TIMEOUT, msg} -> s

      {:APPEND_ENTRIES_TIMEOUT, msg} -> s

      {:CLIENT_REQUEST, msg} -> s

      unexpected -> Helper.node_halt(inspect unexpected)

    end # receive

    Server.next(s)

  end # next

end # Server

