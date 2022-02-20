# distributed algorithms coursework, raft consensus
# Samuel Adekunle (sja119) and Neel Dugar (nd1412), 20 feb 2022

defmodule Heartbeat do

  # s = server process state (c.f. self/this)

  # _________________________________________________________ send_heartbeat_request
  def send_heartbeat_request(s, dst) do
    send dst, {:HEARTBEAT_REQUEST, s.selfP, s.curr_term}
    s
  end # send_heartbeat_request

end
