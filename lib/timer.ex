# distributed algorithms, n.dulay, 8 feb 2022
# coursework, raft consensus, v2
# modified by Samuel Adekunle (sja119) and Neel Dugar (nd419), 20 feb 2022

defmodule Timer do

  # s = server process state (c.f. self/this)

  # _________________________________________________________ restart_vote_timer()
  def restart_election_timer(s) do
    s = Timer.cancel_election_timer(s)

    election_timeout = Enum.random(s.config.election_timeout_range)

    election_timer = Process.send_after(
      s.selfP,
      {:ELECTION_TIMEOUT, {s.curr_term, s.curr_election}},
      election_timeout
    )

    s
    |> State.election_timer(election_timer)
    |> Debug.message("+etim", {:ELECTION_TIMEOUT, {s.curr_term, s.curr_election}, election_timeout})
  end # restart_election_timer

  # _________________________________________________________ restart_vote_timer()
  def cancel_election_timer(s) do
    if s.election_timer do
      Process.cancel_timer(s.election_timer)
    end # if
    s
    |> State.election_timer(nil)
  end # cancel_election_timer

  # _________________________________________________________ restart_append_entries_timer()
  def restart_append_entries_timer(s, followerP) do
    s = Timer.cancel_append_entries_timer(s, followerP)

    append_entries_timer = Process.send_after(
      s.selfP,
      {:APPEND_ENTRIES_TIMEOUT, {s.curr_term, followerP}},
      s.config.append_entries_timeout
    )
    s
    |> State.append_entries_timer(followerP, append_entries_timer)
    |> Debug.message("+atim", {{:APPEND_ENTRIES_TIMEOUT, s.curr_term, followerP}, s.config.append_entries_timeout})
  end # restart_append_entries_timer

  # _________________________________________________________ cancel_append_entries_timer()
  def cancel_append_entries_timer(s, followerP) do
    if s.append_entries_timers[followerP] do
      Process.cancel_timer(s.append_entries_timers[followerP])
    end # if
    s
    |> State.append_entries_timer(followerP, nil)
  end # cancel_append_entries_timer

  # _________________________________________________________ cancel_all_append_entries_timers()
  def cancel_all_append_entries_timers(s) do
    for followerP <- s.append_entries_timers do
      Timer.cancel_append_entries_timer(s, followerP)         # mutated result ignored, next statement will reset
    end
    s
    |> State.append_entries_timers()                        # now reset to Map.new
  end # cancel_all_append_entries_timers

  # _________________________________________________________ restart_heartbeat_timer
  def restart_heartbeat_timer(s) do
    s = s
        |> Timer.cancel_election_timer()

    heartbeat_timer = Process.send_after(
      s.selfP,
      {:SEND_HEARTBEAT},
      s.config.heartbeat_interval
    )

    s
    |> State.heartbeat_timer(heartbeat_timer)
  end # restart_heartbeat_timer

  # _________________________________________________________ cancel_heartbeat_timer
  def cancel_heartbeat_timer(s) do
    if s.heartbeat_timer do
      Process.cancel_timer(s.heartbeat_timer)
    end
    s
    |> State.heartbeat_timer(nil)
  end # cancel_heartbeat_timer

  # _________________________________________________________ start_crash_timer
  def start_crash_timer(s) do
    {crash_time, crash_duration} = s.config.crash_servers[s.server_num]

    if crash_time != nil do
      crash_timer = Process.send_after(
        s.selfP,
        {:CRASH, crash_duration},
        crash_time
      )

      s
      |> State.crash_timer(crash_timer)
    else
      s
    end
  end # start_crash_timer

  # _________________________________________________________ cancel_crash_timer
  def cancel_crash_timer(s) do
    if s.crash_timer do
      Process.cancel_timer(s.crash_timer)
    end
    s
    |> State.crash_timer(nil)
  end # cancel_crash_timer

end # Timer
