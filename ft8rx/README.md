# ft8rx

`ft8rx` is the receive/UI application for this repo. It also contains the FT8 QSO engine, queue scheduler, web UI, and rig control hooks used to attempt automated QSOs with stations selected from the monitor pane.

## Config

The app loads `config/ft8rx.json` by default.

The file contains:

- Station identity:
  - `our_call`
  - `our_grid`
- TX settings:
  - `base_freq_hz`
  - `drive_level`
  - `playback_channels`
  - `output_device`
  - `power_w`
  - `tx_freq_min_hz`
  - `tx_freq_max_hz`
- FSM settings:
  - `rr73_enabled`
  - `timeout_seconds`
  - per-state `no_msg` / `no_fwd` thresholds
- Logging:
  - `fsm_log_path`
  - `app_log_path`

Current important defaults:

- `send_grid.no_msg = 3`
- `send_sig.no_msg = 3`
- QSO timeout = 10 minutes

## Web UI

The web UI is split into:

- Top row:
  - status / rig control / waterfall
  - station detail pane
- Second row:
  - QSO pane
  - `Stations To Work` queue pane

Rig controls:

- Band dropdown applies immediately.
- Power dropdown applies immediately.
- `Tune 10s` sends a 1000 Hz tone for 10 seconds, then returns the rig to RX.

## Queue Workflow

There is no direct manual `Start QSO` button anymore.

The intended flow is:

1. Click a station in the monitor pane.
2. In `Station Detail`, click `Add To Queue`.
3. In the QSO pane, enable `Auto QSO From Queue`.
4. When the QSO engine is idle, the scheduler picks the oldest ready station and starts a QSO automatically.

The QSO pane still controls the TX audio frequency:

- `TX Freq` sets the queue’s transmit frequency.
- `Auto-Pick Quiet Spot` scans the bandmap and chooses an empty/quiet 50 Hz bucket for the current candidate parity.
- `Stop` or `Escape` aborts TX immediately and exits the active QSO.

## Queue Scheduling

The scheduler uses `oldest ready first`.

Each queue entry stores:

- `callsign`
- `queued_at`
- `ok_to_schedule_after`

Only one queue entry per callsign is allowed.

Duplicate `Add To Queue` clicks are ignored.

### Ready Rules

A queued station is ready only if all of these are true:

- The station has been heard in at least one of the last two slots of its own parity.
  - For an even-slot station, that means one of the last two even slots.
  - For an odd-slot station, that means one of the last two odd slots.
- The most recent message heard from that station was one of:
  - `CQ`
  - `73`
  - `RR73`
- `now >= ok_to_schedule_after`
- The station has not been worked in the last 24 hours.

### Queue Removal and Requeue Rules

Queued stations are removed immediately when:

- They are already worked in the last 24 hours.
- They have not been heard for 10 minutes.
- They are manually removed from the queue.
- They are dispatched into a QSO attempt.

If a QSO exits because of:

- `send_grid_no_msg_limit`, or
- `send_sig_no_msg_limit`

then the station is treated as “QSO did not happen” and is requeued:

- with a fresh `queued_at`
- with `ok_to_schedule_after = now + 5 minutes`

All other QSO exits drop the station instead of requeueing it.

## Recent Worked Suppression

The queue suppresses calls worked in the last 24 hours.

This cache is rebuilt on startup by scanning the QSO JSONL log. A station counts as “worked” only if we actually launched a terminal `73` transmission:

- `tx_launch` with `state_before = send_73`, or
- `tx_launch` with `state_before = send_73_once`

Receiving `R`, `RRR`, or `RR73` alone does not mark a station as worked.

## QSO Start and Parity

When the scheduler starts a QSO:

- The selected partner is the queue entry chosen by `oldest ready first`.
- The initial state is always `SendGrid`.
- Partner TX parity is inferred from that station’s most recent heard slot family.
- Our TX parity is the opposite family.

This implementation is intentionally answering/direct-calling only. It does not start from CQ ownership logic.

## FSM Overview

The FSM runs inside `ft8rx/src/qso.rs`.

High-level behavior:

- State is advanced by partner RX events.
- TX is built from the current FSM state and synthesized through the decoder TX API.
- Late RX is handled explicitly:
  - early decode stages are preferred
  - committed TX slots stay bound to their committed state/message
  - late state changes are queued and only applied before the next TX slot if no fresher in-time RX supersedes them

### Decode Feeding

The FSM consumes partner RX in this order:

- `early41`
- `early47`
- `full`

For a given RX slot:

- the first early stage that produces a partner event is used
- `full` is only used if neither early stage produced a partner event

This reduces late transitions by allowing the FSM to act on early clean decodes instead of waiting for the full decode.

### Event Classification

For the active partner, the FSM classifies messages as:

- directed to our call
- directed to another station
- CQ
- non-call token in field 1
- freeform / unsupported
- none

Special handling:

- `RR73` is normalized to `RR73` semantics even when it arrives through the WSJT-X-compatible grid-coded form.
- Any non-call token in field 1 from the active partner is treated as “partner moved on” in terminal states.

## FSM States

### `Idle`

- No TX.
- The scheduler may dispatch a ready queue entry from here.

### `SendGrid`

- TX: `<partner> <our_call> <our_grid>`
- Directed plain grid/report to our call counts as forward progress.
- Directed `R`/ack to our call also counts as forward progress.
- Next state on forward progress: `SendSigAck`
- Directed `RR73` to our call: `Send73Once`
- Directed `RRR` or `73` to our call: `Send73`
- No partner message to our call: increment `no_msg`
- Message to our call without forward progress: increment `no_fwd`
- `no_fwd` threshold: `Send73Once`
- `no_msg` threshold: exit with `send_grid_no_msg_limit`
  - queue logic requeues the station with a 5 minute delay

### `SendSig`

- TX: `<partner> <our_call> <report>`
- Directed `R`/ack or `RRR` to our call:
  - `SendRR73` if `rr73_enabled = true`
  - otherwise `SendRRR`
- Directed `RR73` to our call: `Send73Once`
- Directed `73` to our call: `Send73`
- No partner message to our call: increment `no_msg`
- Message to our call without forward progress: increment `no_fwd`
- `no_fwd` threshold: `Send73Once`
- `no_msg` threshold: exit with `send_sig_no_msg_limit`
  - queue logic requeues the station with a 5 minute delay

### `SendSigAck`

- TX: `<partner> <our_call> R<report>`
- Directed `RR73` to our call: `Send73Once`
- Directed `R`, `RRR`, or `73` to our call: `Send73`
- No partner message to our call: increment `no_msg`
- Message to our call without forward progress: increment `no_fwd`
- Either threshold: `Send73Once`

### `SendRR73`

- TX: `<partner> <our_call> RR73`
- Directed `73` to our call: exit
- Other directed message to our call: increment `no_fwd`
- Partner CQ, directed message to another station, non-call token, freeform, or no message: exit
- `no_fwd` threshold: `Send73Once`

### `SendRRR`

- TX: `<partner> <our_call> RRR`
- Directed `73` to our call: `Send73`
- Directed `RR73` to our call: `Send73Once`
- Directed message to another station or non-call token: `Send73Once`
- No partner message to our call: increment `no_msg`
- Message to our call without `73`: increment `no_fwd`
- Either threshold: `Send73Once`

### `Send73`

- TX: `<partner> <our_call> 73`
- Directed `73` to our call: exit
- Partner CQ, directed message to another station, non-call token, or freeform: exit
- No partner message: increment `no_msg`
- Message to our call without `73`: increment `no_fwd`
- Either threshold: exit

### `Send73Once`

- TX exactly one `<partner> <our_call> 73`
- Exit immediately after that actual `73` TX completes

## Counters and Timeout

- `no_msg_count` increments only when the partner does not produce a qualifying message to our call for the current state.
- `no_fwd_count` increments only when the partner does produce a message to our call, but it does not advance the QSO.
- Both counters reset on state entry.
- Every QSO hard-times out after 10 minutes regardless of state.

## Logging

There are two logs now:

- JSONL QSO log:
  - default: `ft8rx/logs/ft8rx-qso.jsonl`
- Human-readable app log:
  - default: `ft8rx/logs/ft8rx.log`

The JSONL QSO log is intended for exact reconstruction of QSO behavior.

Each QSO record includes:

- session id
- partner call
- state before / after
- TX parity
- TX frequency
- counters
- timeout remaining
- RX stage
- RX classification
- rendered RX text
- serialized structured RX payload
- TX text when present

The text log includes both general app events and QSO events, including:

- queue add / duplicate ignore / remove
- recent-worked cache load and updates
- requeue decisions
- scheduler dispatch decisions
- QSO lifecycle events

Use the text log for quick debugging and the JSONL log when exact state reconstruction matters.
