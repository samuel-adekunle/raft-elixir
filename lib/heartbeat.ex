# distributed algorithms coursework, raft consensus
# Samuel Adekunle (sja119) and Neel Dugar (nd1412), 20 feb 2022

defmodule Heartbeat do

  # s = server process state (c.f. self/this)

  # _________________________________________________________ send_heartbeat_request
  def send_heartbeat_request(s, dst) do
    send dst, heartbeat_request(s)
    s
  end # send_heartbeat_request

  # _________________________________________________________ handle_send_heartbeat_send_request
  def handle_send_heartbeat_send_request(s) do
    s
    |> Server.broadcast(heartbeat_request(s))
    |> Timer.restart_heartbeat_timer()
  end # handle_send_heartbeat_send_request

  # _________________________________________________________ handle_request_send_reply
  def handle_request_send_reply(s, req) do
    {leaderP, leader_term} = req
    if s.curr_term > leader_term do
      s
    else
      s = s
          |> State.role(:FOLLOWER)
          |> State.curr_term(leader_term)
          |> State.leaderP(leaderP)
          |> Timer.restart_election_timer()

      s = case s.role do
        :LEADER ->
          s
          |> Timer.cancel_heartbeat_timer()
          |> Server.print("#{s.server_num} got evicted")

        :CANDIDATE ->
          s
          |> Server.print("#{s.server_num} steps down from election")

        :FOLLOWER -> s
      end

      send s.leaderP, {:HEARTBEAT_REPLY, s.selfP}
      s
    end
  end # handle_request_send_reply

  # _________________________________________________________ handle_heartbeat_reply
  def handle_heartbeat_reply(s) do
    s
  end # handle_heartbeat_reply

  # _________________________________________________________ handle_heartbeat_reply
  defp heartbeat_request(s) do
    {:HEARTBEAT_REQUEST, {s.selfP, s.curr_term}}
  end # handle_heartbeat_reply

end
