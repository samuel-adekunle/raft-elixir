# distributed algorithms, n.dulay, 8 feb 2022
# coursework, raft consensus, v2
# modified by Samuel Adekunle (sja119) and Neel Dugar (nd419), 20 feb 2022


defmodule ClientReq do

  # s = server process state (c.f. self/this)

  # _________________________________________________________ handle_request_send_reply
  def handle_request_send_reply(s, msg) do
    case s.role do
      :FOLLOWER ->
        send msg.clientP, {:CLIENT_REPLY, {msg.cid, :NOT_LEADER, s.leaderP}}
        s
      :CANDIDATE ->
        send msg.clientP, {:CLIENT_REPLY, {msg.cid, :NOT_LEADER, nil}}
        s
      :LEADER ->
        s
        |> Log.append_entry(%{request: msg, term: s.curr_term})
        |> Monitor.send_msg({:CLIENT_REQUEST, s.server_num})
    end
  end # handle_request_send_reply

end # Clientreq


