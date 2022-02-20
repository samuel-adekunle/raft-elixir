# distributed algorithms, n.dulay, 8 feb 2022
# coursework, raft consensus, v2
# modified by Samuel Adekunle (sja119) and Neel Dugar (nd1412), 20 feb 2022

defmodule Vote do

  # s = server process state (c.f. self/this)

  # _________________________________________________________ send_vote_request
  def send_vote_request(s) do
    s
    |> start_election()
    |> Server.broadcast({:VOTE_REQUEST, %{candidateP: s.selfP, candidate_term: s.curr_term, debugC: s.server_num}})
  end # send_vote_request

  # _________________________________________________________ receive_vote_reply
  def receive_vote_reply(s, vote) do
    s
    |> add_vote(vote)
    |> tally_votes()
  end # receive_vote_reply

  # _________________________________________________________ send_vote_reply
  def send_vote_reply(s, req) do
    s
    |> State.voted_for(req.candidateP)
    |> Timer.restart_election_timer()
    |> Server.print("#{s.server_num} votes for #{req.debugC}")

    send(req.candidateP, {:VOTE_REPLY, %{voteP: s.selfP, curr_term: req.candidate_term, debugV: s.server_num}})
    s
  end # send_vote_reply

  # _________________________________________________________ receive_request_send_reply
  # TODO: Check against log index
  def receive_request_send_reply(s, req) do
    case {req.candidate_term, s.voted_for} do
      {c_term, _} when c_term > s.curr_term ->
        s
        |> State.curr_term(c_term)
        |> send_vote_reply(req)

      {c_term, nil} when c_term == s.curr_term ->
        s
        |> send_vote_reply(req)

      _ -> s
    end
  end # receive_request_send_reply

  # _________________________________________________________ step_down
  def step_down(s) do
    s
    |> State.role(:FOLLOWER)
    |> Timer.restart_election_timer()
    |> Server.print("#{s.server_num} steps down from election")
  end # step_down

  # _________________________________________________________ start_election
  defp start_election(s) do
    s
    |> State.inc_term()
    |> State.role(:CANDIDATE)
    |> State.new_voted_by()
    |> State.add_to_voted_by(s.selfP)
    |> State.voted_for(s.selfP)
    |> Timer.restart_election_timer()
    |> Server.print("#{s.server_num} stands for election")
  end # start_election

  # _________________________________________________________ add_vote
  defp add_vote(s, vote) do
    if vote.curr_term == s.curr_term do
      s
      |> State.add_to_voted_by(vote.voteP)
    else
      s
    end
  end # add_vote

  # _________________________________________________________ tally_votes
  defp tally_votes(s) do
    if State.vote_tally(s) >= s.majority do
      s
      |> State.role(:LEADER)
      |> Timer.restart_heartbeat_timer()
      |> Server.print("#{s.server_num} won election")
    else
      s
    end
  end # tally_votes

end # Vote


