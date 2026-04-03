# ft8rx

`ft8rx` is the receive/UI application for this repo. It now also contains a manual FT8 QSO state machine that can drive transmit audio through the rig path when a user starts a QSO from the web interface.

## Config

The app loads `config/ft8rx.json` by default. The file contains:

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

## Web QSO Control

The web UI exposes a dedicated QSO pane:

- Select a station in the monitor/detail pane.
- Pick a TX frequency in Hz.
- `Auto-Pick Quiet Spot` scans the bandmap for the current TX parity and picks the emptiest 50 Hz cell inside the configured TX range.
- `Start QSO` starts a manual QSO with the selected station.
- `Stop` or `Escape` aborts transmit immediately and exits the QSO.

TX parity is inferred automatically:

- The selected station’s last-heard slot family is used as the partner RX family.
- Our TX family is the opposite family.

This v1 always starts in `SendGrid`. There is no auto-start and no CQ-originated flow yet.

## FSM

The FSM advances only after each full decode on the partner RX cycle. It logs every state transition, RX classification, TX launch, TX completion, abort, timeout, and exit to the configured tracing file.

### `Idle`

- No TX.
- Entered when no QSO is active.
- Exit only when the web UI starts a QSO for a selected station.

### `SendGrid`

- TX: `<partner> <our_call> <our_grid>`
- If partner sends a message to our call with `R`, `R grid`, or `R report`: go to `SendSigAck`
- If partner sends `RRR`, `RR73`, or `73` to our call: go to `Send73`
- If no partner message to our call: increment `no_msg`
- If partner addresses our call without forward progress: increment `no_fwd`
- Exit to `Send73Once` on `no_fwd` threshold
- Exit QSO on `no_msg` threshold

### `SendSig`

- TX: `<partner> <our_call> <report>`
- If partner sends `R`, `RRR`, or `RR73` to our call:
  - go to `SendRR73` when `rr73_enabled=true`
  - otherwise go to `SendRRR`
- If partner sends `73` to our call: go to `Send73`
- If no partner message to our call: increment `no_msg`
- If partner addresses our call without forward progress: increment `no_fwd`
- Exit to `Send73Once` on `no_fwd` threshold
- Exit QSO on `no_msg` threshold

### `SendSigAck`

- TX: `<partner> <our_call> R<report>`
- If partner sends `R`, `RRR`, `RR73`, or `73` to our call: go to `Send73`
- If no partner message to our call: increment `no_msg`
- If partner addresses our call without forward progress: increment `no_fwd`
- Go to `Send73Once` on either threshold

### `SendRR73`

- TX: `<partner> <our_call> RR73`
- If partner sends `73` to our call: exit QSO
- If partner sends any other message to our call: increment `no_fwd`
- If partner sends CQ, talks to another station, sends freeform/unsupported, or disappears: exit QSO
- Go to `Send73Once` on `no_fwd` threshold

### `SendRRR`

- TX: `<partner> <our_call> RRR`
- If partner sends `73` to our call: go to `Send73`
- If partner addresses another station: go to `Send73Once`
- If no partner message to our call: increment `no_msg`
- If partner addresses our call without `73`: increment `no_fwd`
- Go to `Send73Once` on either threshold

### `Send73`

- TX: `<partner> <our_call> 73`
- If partner sends `73` to our call: exit QSO
- If partner sends CQ, talks to another station, or sends freeform/unsupported: exit QSO
- If no partner message: increment `no_msg`
- If partner addresses our call without `73`: increment `no_fwd`
- Exit QSO on either threshold

### `Send73Once`

- TX exactly one `<partner> <our_call> 73`
- Exit immediately after TX completes

## Counters and Timeout

- `no_msg_count` increments only when the partner does not send a qualifying message to our call for the current state.
- `no_fwd_count` increments only when the partner does send a message to our call but it does not advance the QSO.
- Both counters reset on state entry.
- Every QSO hard-times out after 10 minutes by default, regardless of state.

## Logging

The QSO FSM emits structured tracing events to `logs/ft8rx-qso.jsonl` by default. Each record includes:

- session id
- wall-clock timestamp
- partner call
- TX parity
- TX frequency
- state before / after
- counters
- timeout remaining
- last RX classification
- RX text when present
- TX text when present

The intent is that a full QSO can be reconstructed from the log without relying on the UI.
