# distributed algorithms, n.dulay, 8 feb 2022
# coursework, raft consensus, v2
# modified by Samuel Adekunle (sja119) and Neel Dugar (nd419), 20 feb 2022

defmodule AppendEntries do

  # s = server process state (c.f. this/self)

  # _________________________________________________________ send_append_entries_request
  def send_append_entries_request(s, msg) do
    s = s
        |> Log.append_entry(%{request: msg, term: s.curr_term})
    s
    |> Server.broadcast(
         {
           :APPEND_ENTRIES_REQUEST,
           %{
             entries: [],
             term: s.curr_term,
             leaderP: s.selfP,
             leader_commit_index: 0,
             prev_log_index: 0,
             prev_log_term: 0
           }
         }
       )
  end # send_append_entries_request


  # _________________________________________________________ handle_request_send_reply
  def handle_request_send_reply(s, msg) do
    # TODO
  end # handle_request_send_reply

end # AppendEntriess



