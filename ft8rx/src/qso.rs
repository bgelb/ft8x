use crate::config::AppConfig;
use chrono::{DateTime, Utc};
use ft8_decoder::{
    DecodeStage, Mode, ReplyWord, StructuredInfoValue, StructuredMessage, TxDirectedPayload,
    TxMessage, WaveformOptions, synthesize_tx_message,
};
use rigctl::K3s;
use rigctl::audio::{AudioDevice, prepare_mono_playback};
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, SystemTime};
use tracing::{error, info, warn};

const PRE_KEY_MS: u64 = 150;
const TRANSCRIPT_LIMIT: usize = 256;
const REPORT_MIN_DB: i32 = -30;
const REPORT_MAX_DB: i32 = 49;

#[derive(Debug, Clone)]
pub struct StationStartInfo {
    pub callsign: String,
    pub last_heard_at: SystemTime,
    pub last_heard_slot_family: SlotFamily,
    pub last_snr_db: i32,
    pub last_text: Option<String>,
    pub last_structured_json: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QsoStartMode {
    Normal,
    Direct,
    Cq,
}

impl QsoStartMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Direct => "direct",
            Self::Cq => "cq",
        }
    }
}

#[derive(Debug, Clone)]
pub enum QsoCommand {
    Start {
        partner_call: String,
        tx_freq_hz: f32,
        initial_state: QsoState,
        start_mode: QsoStartMode,
        tx_slot_family_override: Option<SlotFamily>,
    },
    Stop {
        reason: String,
    },
}

#[derive(Debug, Clone)]
pub struct QsoOutcome {
    pub partner_call: String,
    pub exit_reason: String,
    pub finished_at: SystemTime,
    pub rig_band: Option<String>,
    pub sent_terminal_73: bool,
}

#[derive(Debug, Clone)]
pub struct CompoundHandoffPlan {
    pub next_station: StationStartInfo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotFamily {
    Even,
    Odd,
}

impl SlotFamily {
    pub fn opposite(self) -> Self {
        match self {
            Self::Even => Self::Odd,
            Self::Odd => Self::Even,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Even => "even",
            Self::Odd => "odd",
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QsoState {
    Idle,
    SendCq,
    SendGrid,
    SendSig,
    SendSigAck,
    SendRR73,
    SendRRR,
    Send73,
    Send73Once,
}

impl QsoState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::SendCq => "send_cq",
            Self::SendGrid => "send_grid",
            Self::SendSig => "send_sig",
            Self::SendSigAck => "send_sig_ack",
            Self::SendRR73 => "send_rr73",
            Self::SendRRR => "send_rrr",
            Self::Send73 => "send_73",
            Self::Send73Once => "send_73_once",
        }
    }

    fn transmits(self) -> bool {
        self != Self::Idle
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct WebQsoDefaults {
    pub tx_freq_min_hz: f32,
    pub tx_freq_max_hz: f32,
    pub tx_freq_default_hz: f32,
}

#[derive(Debug, Clone, Serialize)]
pub struct WebQsoTranscriptEntry {
    pub timestamp: String,
    pub direction: String,
    pub state: String,
    pub text: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct WebQsoSnapshot {
    pub active: bool,
    pub partner_call: Option<String>,
    pub state: String,
    pub tx_slot_family: Option<String>,
    pub latest_partner_snr_db: Option<i32>,
    pub selected_tx_freq_hz: Option<f32>,
    pub no_msg_count: u32,
    pub no_fwd_count: u32,
    pub timeout_remaining_seconds: Option<u64>,
    pub tx_active: bool,
    pub last_rx_event: Option<String>,
    pub transcript: Vec<WebQsoTranscriptEntry>,
}

impl Default for WebQsoSnapshot {
    fn default() -> Self {
        Self {
            active: false,
            partner_call: None,
            state: QsoState::Idle.as_str().to_string(),
            tx_slot_family: None,
            latest_partner_snr_db: None,
            selected_tx_freq_hz: None,
            no_msg_count: 0,
            no_fwd_count: 0,
            timeout_remaining_seconds: None,
            tx_active: false,
            last_rx_event: None,
            transcript: Vec::new(),
        }
    }
}

pub struct QsoController {
    config: AppConfig,
    backend: Box<dyn TxBackend>,
    next_session_id: u64,
    session: Option<ActiveSession>,
    last_snapshot: WebQsoSnapshot,
    pending_outcomes: VecDeque<QsoOutcome>,
    current_rig_frequency_hz: Option<u64>,
    current_rig_band: Option<String>,
    current_app_mode: Mode,
}

impl QsoController {
    pub fn new(config: AppConfig, backend: Box<dyn TxBackend>) -> Self {
        let last_snapshot = WebQsoSnapshot {
            selected_tx_freq_hz: Some(config.clamped_default_tx_freq_hz()),
            ..WebQsoSnapshot::default()
        };
        Self {
            config,
            backend,
            next_session_id: 1,
            session: None,
            last_snapshot,
            pending_outcomes: VecDeque::new(),
            current_rig_frequency_hz: None,
            current_rig_band: None,
            current_app_mode: Mode::Ft8,
        }
    }

    pub fn update_rig_context(
        &mut self,
        frequency_hz: Option<u64>,
        band: Option<String>,
        app_mode: Mode,
    ) {
        self.current_rig_frequency_hz = frequency_hz;
        self.current_rig_band = band.clone();
        self.current_app_mode = app_mode;
        if let Some(session) = &mut self.session {
            session.rig_frequency_hz = frequency_hz;
            session.rig_band = band;
            session.app_mode = app_mode;
        }
    }

    pub fn defaults(&self) -> WebQsoDefaults {
        WebQsoDefaults {
            tx_freq_min_hz: self.config.tx.tx_freq_min_hz,
            tx_freq_max_hz: self.config.tx.tx_freq_max_hz,
            tx_freq_default_hz: self.config.clamped_default_tx_freq_hz(),
        }
    }

    pub fn handle_command(
        &mut self,
        command: QsoCommand,
        station_info: Option<StationStartInfo>,
        now: SystemTime,
    ) {
        match command {
            QsoCommand::Start {
                partner_call,
                tx_freq_hz,
                initial_state,
                start_mode,
                tx_slot_family_override,
            } => self.handle_start(
                partner_call,
                tx_freq_hz,
                initial_state,
                start_mode,
                tx_slot_family_override,
                station_info,
                now,
            ),
            QsoCommand::Stop { reason } => self.stop_session(&reason, now),
        }
    }

    pub fn on_full_decode(
        &mut self,
        slot_start: SystemTime,
        decodes: &[ft8_decoder::DecodedMessage],
        now: SystemTime,
    ) {
        self.on_decode_stage(slot_start, DecodeStage::Full, decodes, now);
    }

    pub fn on_decode_stage(
        &mut self,
        slot_start: SystemTime,
        stage: DecodeStage,
        decodes: &[ft8_decoder::DecodedMessage],
        now: SystemTime,
    ) {
        let Some(session) = &mut self.session else {
            return;
        };
        if slot_family_for_mode(session.app_mode, slot_start) == session.tx_slot_family {
            return;
        }
        Self::roll_rx_stage_tracking(session, slot_start);
        session.compound_rr73_ready_slot = None;

        let event = if session.state == QsoState::SendCq {
            PartnerEvent::None
        } else {
            classify_partner_event(
                decodes,
                &session.partner_call,
                &self.config.station.our_call,
                session.state,
            )
        };
        let should_consume = Self::should_consume_stage_event(session, stage, &event);
        if !should_consume {
            return;
        }
        session.rx_slot_consumed_stage = Some(stage);

        let committed_next_tx =
            is_next_tx_slot_committed(session, slot_start, self.backend.is_active());
        if !committed_next_tx {
            Self::clear_pending_transition(session, now);
        }
        if let Some(snr_db) = event.snr_db() {
            session.latest_partner_snr_db = snr_db;
        }
        if event.has_partner_message() {
            session.partner_rx_count += 1;
        }
        session.last_rx_event = Some(event.summary());
        session.last_rx_stage = Some(stage);
        session.last_rx_text = event.message_text();
        session.last_rx_structured_json = event.structured_json();
        Self::push_transcript(session, now, "RX:", session.state, event.transcript_text());
        Self::log_fsm(
            session,
            match stage {
                DecodeStage::Early41 => "rx_slot_early41",
                DecodeStage::Early47 => "rx_slot_early47",
                DecodeStage::Full => "rx_slot_full",
            },
            session.state,
            session.state,
            event.summary(),
            event.message_text(),
            None,
            now,
        );

        let previous_state = session.state;
        let mut exit_reason = None;
        let mut next_state = previous_state;

        match previous_state {
            QsoState::Idle => {}
            QsoState::SendCq => {
                session.no_msg_count += 1;
                if session.no_msg_count >= self.config.fsm.send_grid.no_msg {
                    exit_reason = Some("send_cq_no_msg_limit");
                }
            }
            QsoState::SendGrid => match event {
                PartnerEvent::ToUs {
                    event: ToUsEvent::Ack,
                    ..
                }
                | PartnerEvent::ToUs {
                    event: ToUsEvent::ReportLike,
                    ..
                } => next_state = QsoState::SendSigAck,
                PartnerEvent::ToUs {
                    event: ToUsEvent::Reply(ReplyWord::Rr73),
                    ..
                } => next_state = QsoState::Send73Once,
                PartnerEvent::ToUs {
                    event: ToUsEvent::Reply(_),
                    ..
                } => next_state = QsoState::Send73,
                PartnerEvent::ToUs {
                    event: ToUsEvent::Other,
                    ..
                } => {
                    session.no_fwd_count += 1;
                    if session.no_fwd_count >= self.config.fsm.send_grid.no_fwd {
                        exit_reason = Some("send_grid_no_fwd_limit");
                    }
                }
                _ => {
                    session.no_msg_count += 1;
                    if session.no_msg_count >= self.config.fsm.send_grid.no_msg {
                        exit_reason = Some("send_grid_no_msg_limit");
                    }
                }
            },
            QsoState::SendSig => match event {
                PartnerEvent::ToUs {
                    event: ToUsEvent::Reply(ReplyWord::SeventyThree),
                    ..
                } => next_state = QsoState::Send73,
                PartnerEvent::ToUs {
                    event: ToUsEvent::Reply(ReplyWord::Rr73),
                    ..
                } => next_state = QsoState::Send73Once,
                PartnerEvent::ToUs {
                    event: ToUsEvent::Ack,
                    ..
                }
                | PartnerEvent::ToUs {
                    event: ToUsEvent::Reply(ReplyWord::Rrr),
                    ..
                } => {
                    next_state = if self.config.fsm.rr73_enabled {
                        QsoState::SendRR73
                    } else {
                        QsoState::SendRRR
                    }
                }
                PartnerEvent::ToUs {
                    event: ToUsEvent::Other,
                    ..
                }
                | PartnerEvent::ToUs {
                    event: ToUsEvent::ReportLike,
                    ..
                } => {
                    session.no_fwd_count += 1;
                    if session.no_fwd_count >= self.config.fsm.send_sig.no_fwd {
                        exit_reason = Some("send_sig_no_fwd_limit");
                    }
                }
                _ => {
                    session.no_msg_count += 1;
                    if session.no_msg_count >= self.config.fsm.send_sig.no_msg {
                        exit_reason = Some("send_sig_no_msg_limit");
                    }
                }
            },
            QsoState::SendSigAck => match event {
                PartnerEvent::ToUs {
                    event: ToUsEvent::Reply(ReplyWord::Rr73),
                    ..
                } => next_state = QsoState::Send73Once,
                PartnerEvent::ToUs {
                    event: ToUsEvent::Ack,
                    ..
                }
                | PartnerEvent::ToUs {
                    event: ToUsEvent::Reply(_),
                    ..
                } => next_state = QsoState::Send73,
                PartnerEvent::ToUs {
                    event: ToUsEvent::Other,
                    ..
                }
                | PartnerEvent::ToUs {
                    event: ToUsEvent::ReportLike,
                    ..
                } => {
                    session.no_fwd_count += 1;
                    if session.no_fwd_count >= self.config.fsm.send_sig_ack.no_fwd {
                        next_state = QsoState::Send73Once;
                    }
                }
                _ => {
                    session.no_msg_count += 1;
                    if session.no_msg_count >= self.config.fsm.send_sig_ack.no_msg {
                        next_state = QsoState::Send73Once;
                    }
                }
            },
            QsoState::SendRR73 => match event {
                PartnerEvent::ToUs {
                    event: ToUsEvent::Reply(ReplyWord::SeventyThree),
                    ..
                } => exit_reason = Some("send_rr73_confirmed"),
                PartnerEvent::ToUs { .. } => {
                    session.no_fwd_count += 1;
                    if session.no_fwd_count >= self.config.fsm.send_rr73.no_fwd {
                        next_state = QsoState::Send73Once;
                    }
                }
                PartnerEvent::ToOther { .. }
                | PartnerEvent::Cq { .. }
                | PartnerEvent::NonCallFirstField { .. }
                | PartnerEvent::Freeform { .. }
                | PartnerEvent::None => exit_reason = Some("send_rr73_partner_moved_on"),
            },
            QsoState::SendRRR => match event {
                PartnerEvent::ToUs {
                    event: ToUsEvent::Reply(ReplyWord::SeventyThree),
                    ..
                } => next_state = QsoState::Send73,
                PartnerEvent::ToUs {
                    event: ToUsEvent::Reply(ReplyWord::Rr73),
                    ..
                } => next_state = QsoState::Send73Once,
                PartnerEvent::ToUs { .. } => {
                    session.no_fwd_count += 1;
                    if session.no_fwd_count >= self.config.fsm.send_rrr.no_fwd {
                        next_state = QsoState::Send73Once;
                    }
                }
                PartnerEvent::ToOther { .. } | PartnerEvent::NonCallFirstField { .. } => {
                    next_state = QsoState::Send73Once
                }
                _ => {
                    session.no_msg_count += 1;
                    if session.no_msg_count >= self.config.fsm.send_rrr.no_msg {
                        next_state = QsoState::Send73Once;
                    }
                }
            },
            QsoState::Send73 => match event {
                PartnerEvent::ToUs {
                    event: ToUsEvent::Reply(ReplyWord::SeventyThree),
                    ..
                } => exit_reason = Some("send_73_confirmed"),
                PartnerEvent::ToUs { .. } => {
                    session.no_fwd_count += 1;
                    if session.no_fwd_count >= self.config.fsm.send_73.no_fwd {
                        exit_reason = Some("send_73_no_fwd_limit");
                    }
                }
                PartnerEvent::ToOther { .. }
                | PartnerEvent::Cq { .. }
                | PartnerEvent::NonCallFirstField { .. }
                | PartnerEvent::Freeform { .. } => exit_reason = Some("send_73_partner_moved_on"),
                PartnerEvent::None => {
                    session.no_msg_count += 1;
                    if session.no_msg_count >= self.config.fsm.send_73.no_msg {
                        exit_reason = Some("send_73_no_msg_limit");
                    }
                }
            },
            QsoState::Send73Once => {}
        }

        if let Some(reason) = exit_reason {
            if let Some(session) = &mut self.session {
                if committed_next_tx {
                    session.pending_action = Some(PendingAction::Exit(reason.to_string()));
                    let is_no_partner_case = !event.has_partner_message();
                    if !is_no_partner_case {
                        Self::log_late_tx_switch_wanted(
                            session,
                            now,
                            previous_state,
                            previous_state,
                            None,
                            Some(reason),
                        );
                    }
                    Self::push_transcript(
                        session,
                        now,
                        "SYS:",
                        session.state,
                        if is_no_partner_case {
                            format!("exit queued after committed tx ({reason})")
                        } else {
                            format!(
                                "late RX after tx launch: exit queued after current tx ({reason})"
                            )
                        },
                    );
                    Self::log_fsm(
                        session,
                        if is_no_partner_case {
                            "committed_tx_exit_queued"
                        } else {
                            "late_rx_exit_queued"
                        },
                        previous_state,
                        previous_state,
                        session
                            .last_rx_event
                            .clone()
                            .unwrap_or_else(|| "none".to_string()),
                        event.message_text(),
                        None,
                        now,
                    );
                    session.next_tx_slot = schedule_next_tx_slot(session, slot_start);
                    return;
                }
            }
            self.finish_session(reason, now);
            return;
        }

        if let Some(session) = &mut self.session {
            if next_state != previous_state {
                if matches!(next_state, QsoState::SendRR73 | QsoState::Send73Once) {
                    session.compound_rr73_ready_slot = Some(slot_start);
                }
                if committed_next_tx {
                    session.pending_action = Some(PendingAction::Transition(next_state));
                    Self::log_late_tx_switch_wanted(
                        session,
                        now,
                        previous_state,
                        next_state,
                        Some(render_tx_message(&self.config, session)),
                        None,
                    );
                    Self::push_transcript(
                        session,
                        now,
                        "SYS:",
                        previous_state,
                        format!(
                            "late RX after tx launch: state {} -> {} queued after current tx",
                            previous_state.as_str(),
                            next_state.as_str()
                        ),
                    );
                    Self::log_fsm(
                        session,
                        "late_rx_transition_queued",
                        previous_state,
                        next_state,
                        session
                            .last_rx_event
                            .clone()
                            .unwrap_or_else(|| "none".to_string()),
                        event.message_text(),
                        None,
                        now,
                    );
                } else {
                    session.state = next_state;
                    session.no_fwd_count = 0;
                    session.no_msg_count = 0;
                    Self::push_transcript(
                        session,
                        now,
                        "SYS:",
                        next_state,
                        format!(
                            "state {} -> {}",
                            previous_state.as_str(),
                            next_state.as_str()
                        ),
                    );
                    Self::log_fsm(
                        session,
                        "transition",
                        previous_state,
                        next_state,
                        session
                            .last_rx_event
                            .clone()
                            .unwrap_or_else(|| "none".to_string()),
                        None,
                        None,
                        now,
                    );
                }
            } else {
                if committed_next_tx {
                    let desired_tx = render_tx_message(&self.config, session);
                    let current_tx = session
                        .in_flight_tx
                        .as_ref()
                        .map(|tx| tx.message_text.as_str());
                    if current_tx != Some(desired_tx.as_str()) {
                        Self::log_late_tx_switch_wanted(
                            session,
                            now,
                            previous_state,
                            previous_state,
                            Some(desired_tx),
                            None,
                        );
                    }
                }
                Self::log_fsm(
                    session,
                    "stay",
                    previous_state,
                    next_state,
                    session
                        .last_rx_event
                        .clone()
                        .unwrap_or_else(|| "none".to_string()),
                    None,
                    None,
                    now,
                );
            }
            session.next_tx_slot = schedule_next_tx_slot(session, slot_start);
        }
    }

    pub fn tick(&mut self, now: SystemTime) {
        while let Some(event) = self.backend.poll_event() {
            let mut exit_after = None;
            let mut compound_handoff_after = None;
            if let Some(session) = &mut self.session {
                if session.session_id == event.session_id() {
                    match &event {
                        TxEvent::Started {
                            state,
                            message_text,
                            ..
                        } => {
                            Self::push_transcript(
                                session,
                                now,
                                "TX:",
                                *state,
                                message_text.clone(),
                            );
                        }
                        TxEvent::Completed {
                            state,
                            message_text,
                            ..
                        } => {
                            let completed_compound =
                                matches!(*state, QsoState::SendRR73 | QsoState::Send73Once)
                                    && session
                                        .pending_compound_handoff
                                        .as_ref()
                                        .is_some_and(|handoff| handoff.tx_text == *message_text);
                            session.in_flight_tx = None;
                            Self::push_transcript(
                                session,
                                now,
                                "SYS:",
                                *state,
                                "tx complete".to_string(),
                            );
                            if completed_compound {
                                compound_handoff_after = session.pending_compound_handoff.clone();
                                exit_after = Some("compound_handoff_sent".to_string());
                            } else if *state == QsoState::Send73Once {
                                exit_after = Some("send_73_once_complete".to_string());
                            }
                        }
                        TxEvent::Aborted { state, reason, .. } => {
                            session.in_flight_tx = None;
                            Self::push_transcript(
                                session,
                                now,
                                "SYS:",
                                *state,
                                format!("tx aborted: {reason}"),
                            );
                            exit_after = Some("tx_aborted".to_string());
                        }
                        TxEvent::Error { state, message, .. } => {
                            session.in_flight_tx = None;
                            Self::push_transcript(
                                session,
                                now,
                                "SYS:",
                                *state,
                                format!("tx error: {message}"),
                            );
                            exit_after = Some("tx_error".to_string());
                        }
                    }
                    Self::log_fsm(
                        session,
                        event.kind(),
                        session.state,
                        session.state,
                        session
                            .last_rx_event
                            .clone()
                            .unwrap_or_else(|| "none".to_string()),
                        None,
                        Some(event.message()),
                        now,
                    );
                }
            }
            if let Some(handoff) = compound_handoff_after {
                self.finish_session("compound_handoff_sent", now);
                self.start_compound_follow_on(handoff, now);
                continue;
            }
            if let Some(reason) = exit_after {
                self.finish_session(&reason, now);
            }
        }

        if let Some(session) = &self.session {
            if now >= session.deadline_at {
                self.backend.abort();
                self.finish_session("timeout", now);
                return;
            }
        }

        let Some(session) = &mut self.session else {
            return;
        };
        if !session.state.transmits() || self.backend.is_active() {
            return;
        }
        let Some(target_slot) = session.next_tx_slot else {
            return;
        };
        let key_time = target_slot
            .checked_sub(Duration::from_millis(PRE_KEY_MS))
            .unwrap_or(target_slot);
        if now < key_time {
            return;
        }
        match session.pending_action.take() {
            Some(PendingAction::Exit(reason)) => {
                Self::push_transcript(
                    session,
                    now,
                    "SYS:",
                    session.state,
                    format!("applying queued exit before tx: {reason}"),
                );
                Self::log_fsm(
                    session,
                    "queued_exit_applied",
                    session.state,
                    session.state,
                    session
                        .last_rx_event
                        .clone()
                        .unwrap_or_else(|| "none".to_string()),
                    None,
                    None,
                    now,
                );
                self.finish_session(&reason, now);
                return;
            }
            Some(PendingAction::Transition(next_state)) => {
                let prior_state = session.state;
                session.state = next_state;
                session.no_fwd_count = 0;
                session.no_msg_count = 0;
                Self::push_transcript(
                    session,
                    now,
                    "SYS:",
                    next_state,
                    format!(
                        "applying queued state {} -> {} before tx",
                        prior_state.as_str(),
                        next_state.as_str()
                    ),
                );
                Self::log_fsm(
                    session,
                    "queued_transition_applied",
                    prior_state,
                    next_state,
                    session
                        .last_rx_event
                        .clone()
                        .unwrap_or_else(|| "none".to_string()),
                    None,
                    None,
                    now,
                );
            }
            None => {}
        }

        let request = build_tx_request(&self.config, session, target_slot);
        let message_text = request.message_text.clone();
        match self.backend.start(request) {
            Ok(()) => {
                session.last_tx_slot = Some(target_slot);
                if matches!(
                    session.state,
                    QsoState::SendRR73 | QsoState::Send73 | QsoState::Send73Once
                ) {
                    session.sent_terminal_73 = true;
                }
                session.in_flight_tx = Some(InFlightTx {
                    target_slot,
                    state: session.state,
                    message_text: message_text.clone(),
                });
                session.next_tx_slot = None;
                Self::log_fsm(
                    session,
                    "tx_launch",
                    session.state,
                    session.state,
                    session
                        .last_rx_event
                        .clone()
                        .unwrap_or_else(|| "none".to_string()),
                    None,
                    Some(message_text),
                    now,
                );
            }
            Err(error) => {
                Self::push_transcript(
                    session,
                    now,
                    "SYS:",
                    session.state,
                    format!("tx launch failed: {error}"),
                );
                self.backend.abort();
                self.finish_session("tx_launch_failed", now);
            }
        }
    }

    pub fn snapshot(&self, now: SystemTime) -> WebQsoSnapshot {
        if let Some(session) = &self.session {
            WebQsoSnapshot {
                active: true,
                partner_call: Some(session.partner_call.clone()),
                state: session.state.as_str().to_string(),
                tx_slot_family: Some(session.tx_slot_family.as_str().to_string()),
                latest_partner_snr_db: Some(session.latest_partner_snr_db),
                selected_tx_freq_hz: Some(session.tx_freq_hz),
                no_msg_count: session.no_msg_count,
                no_fwd_count: session.no_fwd_count,
                timeout_remaining_seconds: Some(
                    session
                        .deadline_at
                        .duration_since(now)
                        .unwrap_or(Duration::ZERO)
                        .as_secs(),
                ),
                tx_active: self.backend.is_active(),
                last_rx_event: session.last_rx_event.clone(),
                transcript: session.transcript.iter().cloned().collect(),
            }
        } else {
            let mut snapshot = self.last_snapshot.clone();
            snapshot.tx_active = self.backend.is_active();
            snapshot.timeout_remaining_seconds = None;
            snapshot
        }
    }

    pub fn shutdown(&mut self, now: SystemTime) {
        self.backend.abort();
        self.finish_session("shutdown", now);
    }

    pub fn drain_outcomes(&mut self) -> Vec<QsoOutcome> {
        self.pending_outcomes.drain(..).collect()
    }

    pub fn active_partner_call(&self) -> Option<String> {
        self.session
            .as_ref()
            .filter(|session| session.start_mode != QsoStartMode::Cq)
            .map(|session| session.partner_call.clone())
    }

    pub fn reserved_compound_next_call(&self) -> Option<String> {
        self.session
            .as_ref()
            .and_then(|session| session.pending_compound_handoff.as_ref())
            .map(|handoff| handoff.next_station.callsign.clone())
    }

    pub fn refresh_reserved_compound_next_station(
        &mut self,
        station_info: StationStartInfo,
        now: SystemTime,
    ) -> bool {
        let Some(session) = &mut self.session else {
            return false;
        };
        let Some(handoff) = &mut session.pending_compound_handoff else {
            return false;
        };
        if !handoff
            .next_station
            .callsign
            .eq_ignore_ascii_case(&station_info.callsign)
        {
            return false;
        }
        handoff.next_station = station_info.clone();
        let report_db = clamp_report_db(handoff.next_station.last_snr_db);
        handoff.tx_text = format!(
            "{} RR73; {} <{}> {:+03}",
            handoff.finished_call,
            handoff.next_station.callsign,
            self.config.station.our_call,
            report_db
        );
        let next_text = handoff.next_station.last_text.clone().unwrap_or_default();
        let tx_text = handoff.tx_text.clone();
        let last_rx_event = session
            .last_rx_event
            .clone()
            .unwrap_or_else(|| "none".to_string());
        info!(
            finished_call = %handoff.finished_call,
            next_call = %handoff.next_station.callsign,
            report_db,
            "qso_compound_handoff_refreshed"
        );
        Self::log_fsm(
            session,
            "compound_handoff_refreshed",
            session.state,
            session.state,
            last_rx_event,
            Some(next_text),
            Some(tx_text),
            now,
        );
        true
    }

    pub fn maybe_arm_compound_handoff(
        &mut self,
        slot_start: SystemTime,
        plan: CompoundHandoffPlan,
        allow_send_73_once: bool,
        now: SystemTime,
    ) -> bool {
        let Some(session) = &mut self.session else {
            return false;
        };
        if session.compound_rr73_ready_slot != Some(slot_start) {
            return false;
        }
        let compound_pending = session.state == QsoState::SendRR73
            || matches!(
                session.pending_action,
                Some(PendingAction::Transition(QsoState::SendRR73))
            )
            || (allow_send_73_once
                && (session.state == QsoState::Send73Once
                    || matches!(
                        session.pending_action,
                        Some(PendingAction::Transition(QsoState::Send73Once))
                    )));
        if !compound_pending || session.pending_compound_handoff.is_some() {
            return false;
        }
        let report_db = clamp_report_db(plan.next_station.last_snr_db);
        let tx_text = format!(
            "{} RR73; {} <{}> {:+03}",
            session.partner_call,
            plan.next_station.callsign,
            self.config.station.our_call,
            report_db
        );
        session.pending_compound_handoff = Some(PendingCompoundHandoff {
            finished_call: session.partner_call.clone(),
            next_station: plan.next_station.clone(),
            tx_text: tx_text.clone(),
            tx_freq_hz: session.tx_freq_hz,
            tx_slot_family: session.tx_slot_family,
        });
        session.compound_rr73_ready_slot = None;
        Self::push_transcript(
            session,
            now,
            "SYS:",
            session.state,
            format!(
                "compound handoff armed: {} -> {}",
                session.partner_call, plan.next_station.callsign
            ),
        );
        Self::log_fsm(
            session,
            "compound_handoff_armed",
            session.state,
            session.state,
            session
                .last_rx_event
                .clone()
                .unwrap_or_else(|| "none".to_string()),
            None,
            Some(tx_text),
            now,
        );
        true
    }

    pub fn preempt_for_priority_direct(&mut self, now: SystemTime) -> bool {
        let Some(session) = &self.session else {
            return false;
        };
        let reason = match (session.start_mode, session.state, session.partner_rx_count) {
            (QsoStartMode::Normal, QsoState::SendGrid, 0) => Some("send_grid_no_msg_limit"),
            (_, QsoState::SendSig, 0)
                if session.last_tx_slot.is_some() && session.no_msg_count > 0 =>
            {
                Some("send_sig_no_msg_limit")
            }
            (QsoStartMode::Cq, QsoState::SendCq, _) => Some("send_cq_direct_preempt"),
            _ => None,
        };
        let Some(reason) = reason else {
            return false;
        };
        self.finish_session(reason, now);
        true
    }

    fn handle_start(
        &mut self,
        partner_call: String,
        tx_freq_hz: f32,
        initial_state: QsoState,
        start_mode: QsoStartMode,
        tx_slot_family_override: Option<SlotFamily>,
        station_info: Option<StationStartInfo>,
        now: SystemTime,
    ) {
        if self.session.is_some() || self.backend.is_active() {
            warn!(
                partner_call,
                "qso start rejected because a session or tx is already active"
            );
            return;
        }
        if !self.config.validate_tx_freq_hz(tx_freq_hz) {
            warn!(
                partner_call,
                tx_freq_hz, "qso start rejected because tx frequency is invalid"
            );
            return;
        }
        let station_info = if start_mode == QsoStartMode::Cq {
            None
        } else {
            let Some(station_info) = station_info else {
                warn!(
                    partner_call,
                    "qso start rejected because station info is unavailable"
                );
                return;
            };
            Some(station_info)
        };
        let tx_slot_family = if let Some(tx_slot_family_override) = tx_slot_family_override {
            tx_slot_family_override
        } else if let Some(station_info) = &station_info {
            station_info.last_heard_slot_family.opposite()
        } else {
            slot_family_for_mode(
                self.current_app_mode,
                crate::next_slot_boundary_for_mode(self.current_app_mode, now),
            )
        };
        let next_tx_slot = first_matching_slot_after(now, tx_slot_family, self.current_app_mode);
        let mut session = ActiveSession {
            session_id: self.next_session_id,
            partner_call: if start_mode == QsoStartMode::Cq {
                partner_call
            } else {
                station_info
                    .as_ref()
                    .map(|info| info.callsign.clone())
                    .unwrap_or(partner_call)
            },
            state: initial_state,
            start_mode,
            tx_slot_family,
            tx_freq_hz,
            latest_partner_snr_db: station_info
                .as_ref()
                .map(|info| info.last_snr_db)
                .unwrap_or(0),
            started_at: now,
            deadline_at: now + Duration::from_secs(self.config.fsm.timeout_seconds),
            next_tx_slot: Some(next_tx_slot),
            last_tx_slot: None,
            in_flight_tx: None,
            pending_action: None,
            compound_rr73_ready_slot: None,
            pending_compound_handoff: None,
            no_msg_count: 0,
            no_fwd_count: 0,
            partner_rx_count: 0,
            rig_frequency_hz: self.current_rig_frequency_hz,
            rig_band: self.current_rig_band.clone(),
            app_mode: self.current_app_mode,
            last_rx_event: station_info
                .as_ref()
                .and_then(|info| info.last_text.as_ref())
                .as_ref()
                .map(|_| "start_context".to_string()),
            last_rx_stage: None,
            last_rx_text: station_info
                .as_ref()
                .and_then(|info| info.last_text.clone()),
            last_rx_structured_json: station_info
                .as_ref()
                .and_then(|info| info.last_structured_json.clone()),
            current_rx_slot: None,
            rx_slot_consumed_stage: None,
            transcript: VecDeque::new(),
            sent_terminal_73: false,
        };
        self.next_session_id += 1;
        if let Some(text) = station_info
            .as_ref()
            .and_then(|info| info.last_text.clone())
        {
            let state = session.state;
            Self::push_transcript(
                &mut session,
                now,
                "RX:",
                state,
                format!("start context: {text}"),
            );
        }
        let start_line = if let Some(station_info) = &station_info {
            format!(
                "start {} with {} tx={} freq={:.0}Hz state={} latest_snr={:+} last_heard={}",
                start_mode.as_str(),
                session.partner_call,
                session.tx_slot_family.as_str(),
                session.tx_freq_hz,
                session.state.as_str(),
                session.latest_partner_snr_db,
                format_timestamp(station_info.last_heard_at),
            )
        } else {
            format!(
                "start cq tx={} freq={:.0}Hz state={}",
                session.tx_slot_family.as_str(),
                session.tx_freq_hz,
                session.state.as_str(),
            )
        };
        let state = session.state;
        Self::push_transcript(&mut session, now, "SYS:", state, start_line);
        Self::log_fsm(
            &session,
            "start",
            QsoState::Idle,
            session.state,
            "start".to_string(),
            station_info.and_then(|info| info.last_text),
            None,
            now,
        );
        self.session = Some(session);
    }

    fn start_compound_follow_on(&mut self, handoff: PendingCompoundHandoff, now: SystemTime) {
        let follow_on_call = handoff.next_station.callsign.clone();
        self.handle_start(
            follow_on_call,
            handoff.tx_freq_hz,
            QsoState::SendSig,
            QsoStartMode::Direct,
            Some(handoff.tx_slot_family),
            Some(handoff.next_station.clone()),
            now,
        );
        if let Some(session) = &mut self.session {
            session.partner_rx_count = 1;
            Self::push_transcript(
                session,
                now,
                "SYS:",
                session.state,
                format!(
                    "compound handoff from {}: opening report already sent",
                    handoff.finished_call
                ),
            );
            info!(
                event = "compound_start",
                wall_ts = %format_timestamp(now),
                session_id = session.session_id,
                partner_call = %session.partner_call,
                start_mode = %session.start_mode.as_str(),
                rig_frequency_hz = session.rig_frequency_hz.unwrap_or_default(),
                rig_band = session.rig_band.clone().unwrap_or_default(),
                app_mode = %session.app_mode.as_str(),
                tx_slot_family = %session.tx_slot_family.as_str(),
                tx_freq_hz = session.tx_freq_hz,
                state_before = %QsoState::Idle.as_str(),
                state_after = %session.state.as_str(),
                no_msg_count = session.no_msg_count,
                no_fwd_count = session.no_fwd_count,
                timeout_remaining_seconds = session
                    .deadline_at
                    .duration_since(now)
                    .unwrap_or(Duration::ZERO)
                    .as_secs(),
                latest_partner_snr_db = session.latest_partner_snr_db,
                last_rx_stage = session
                    .last_rx_stage
                    .map(DecodeStage::as_str)
                    .unwrap_or(""),
                last_rx_event = %session
                    .last_rx_event
                    .clone()
                    .unwrap_or_else(|| "start_context".to_string()),
                last_rx_text = session.last_rx_text.clone().unwrap_or_default(),
                last_rx_structured_json = session.last_rx_structured_json.clone().unwrap_or_default(),
                rx_text = "",
                tx_text = %handoff.tx_text,
                compound_finished_call = %handoff.finished_call,
                compound_next_call = %session.partner_call,
                started_at = %format_timestamp(session.started_at),
                deadline_at = %format_timestamp(session.deadline_at),
                "qso_fsm"
            );
        }
    }

    fn stop_session(&mut self, reason: &str, now: SystemTime) {
        if self.session.is_none() && !self.backend.is_active() {
            return;
        }
        self.backend.abort();
        self.finish_session(reason, now);
    }

    fn finish_session(&mut self, reason: &str, now: SystemTime) {
        let Some(mut session) = self.session.take() else {
            return;
        };
        let state = session.state;
        Self::push_transcript(
            &mut session,
            now,
            "SYS:",
            state,
            format!("qso exit: {reason}"),
        );
        Self::log_fsm(
            &session,
            "exit",
            session.state,
            QsoState::Idle,
            reason.to_string(),
            None,
            None,
            now,
        );
        self.last_snapshot = WebQsoSnapshot {
            active: false,
            partner_call: Some(session.partner_call.clone()),
            state: QsoState::Idle.as_str().to_string(),
            tx_slot_family: Some(session.tx_slot_family.as_str().to_string()),
            latest_partner_snr_db: Some(session.latest_partner_snr_db),
            selected_tx_freq_hz: Some(session.tx_freq_hz),
            no_msg_count: session.no_msg_count,
            no_fwd_count: session.no_fwd_count,
            timeout_remaining_seconds: None,
            tx_active: self.backend.is_active(),
            last_rx_event: session.last_rx_event.clone(),
            transcript: session.transcript.into_iter().collect(),
        };
        self.pending_outcomes.push_back(QsoOutcome {
            partner_call: session.partner_call,
            exit_reason: reason.to_string(),
            finished_at: now,
            rig_band: session.rig_band,
            sent_terminal_73: session.sent_terminal_73,
        });
    }

    fn push_transcript(
        session: &mut ActiveSession,
        now: SystemTime,
        direction: &str,
        state: QsoState,
        text: String,
    ) {
        session.transcript.push_back(WebQsoTranscriptEntry {
            timestamp: format_timestamp(now),
            direction: direction.to_string(),
            state: state.as_str().to_string(),
            text,
        });
        while session.transcript.len() > TRANSCRIPT_LIMIT {
            session.transcript.pop_front();
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn log_fsm(
        session: &ActiveSession,
        event: &str,
        state_before: QsoState,
        state_after: QsoState,
        rx_summary: String,
        rx_text: Option<String>,
        tx_text: Option<String>,
        now: SystemTime,
    ) {
        info!(
            event,
            wall_ts = %format_timestamp(now),
            session_id = session.session_id,
            partner_call = %session.partner_call,
            start_mode = %session.start_mode.as_str(),
            rig_frequency_hz = session.rig_frequency_hz.unwrap_or_default(),
            rig_band = session.rig_band.clone().unwrap_or_default(),
            app_mode = %session.app_mode.as_str(),
            tx_slot_family = %session.tx_slot_family.as_str(),
            tx_freq_hz = session.tx_freq_hz,
            state_before = %state_before.as_str(),
            state_after = %state_after.as_str(),
            no_msg_count = session.no_msg_count,
            no_fwd_count = session.no_fwd_count,
            timeout_remaining_seconds = session
                .deadline_at
                .duration_since(now)
                .unwrap_or(Duration::ZERO)
                .as_secs(),
            latest_partner_snr_db = session.latest_partner_snr_db,
            last_rx_stage = session
                .last_rx_stage
                .map(DecodeStage::as_str)
                .unwrap_or(""),
            last_rx_event = %rx_summary,
            last_rx_text = session.last_rx_text.clone().unwrap_or_default(),
            last_rx_structured_json = session.last_rx_structured_json.clone().unwrap_or_default(),
            rx_text = rx_text.unwrap_or_default(),
            tx_text = tx_text.unwrap_or_default(),
            compound_finished_call = session
                .pending_compound_handoff
                .as_ref()
                .map(|handoff| handoff.finished_call.clone())
                .unwrap_or_default(),
            compound_next_call = session
                .pending_compound_handoff
                .as_ref()
                .map(|handoff| handoff.next_station.callsign.clone())
                .unwrap_or_default(),
            started_at = %format_timestamp(session.started_at),
            deadline_at = %format_timestamp(session.deadline_at),
            "qso_fsm"
        );
    }

    fn log_late_tx_switch_wanted(
        session: &mut ActiveSession,
        now: SystemTime,
        previous_state: QsoState,
        desired_state: QsoState,
        desired_tx: Option<String>,
        exit_reason: Option<&str>,
    ) {
        let Some(in_flight) = session.in_flight_tx.clone() else {
            return;
        };
        let detail = if let Some(reason) = exit_reason {
            format!(
                "late RX after tx launch: wanted to stop {} and exit ({reason})",
                in_flight.message_text
            )
        } else if let Some(ref desired_tx) = desired_tx {
            if *desired_tx == in_flight.message_text {
                return;
            }
            format!(
                "late RX after tx launch: wanted to switch {} -> {}",
                in_flight.message_text, desired_tx
            )
        } else {
            return;
        };
        Self::push_transcript(session, now, "SYS:", desired_state, detail.clone());
        info!(
            event = "late_tx_switch_wanted",
            wall_ts = %format_timestamp(now),
            session_id = session.session_id,
            partner_call = %session.partner_call,
            committed_slot = %format_timestamp(in_flight.target_slot),
            committed_state = %in_flight.state.as_str(),
            committed_tx_text = %in_flight.message_text,
            desired_state = %desired_state.as_str(),
            desired_tx_text = desired_tx.unwrap_or_default(),
            exit_reason = exit_reason.unwrap_or_default(),
            state_before = %previous_state.as_str(),
            state_after = %desired_state.as_str(),
            no_msg_count = session.no_msg_count,
            no_fwd_count = session.no_fwd_count,
            last_rx_stage = session
                .last_rx_stage
                .map(DecodeStage::as_str)
                .unwrap_or(""),
            last_rx_event = %session
                .last_rx_event
                .clone()
                .unwrap_or_else(|| "none".to_string()),
            last_rx_text = session.last_rx_text.clone().unwrap_or_default(),
            last_rx_structured_json = session.last_rx_structured_json.clone().unwrap_or_default(),
            started_at = %format_timestamp(session.started_at),
            deadline_at = %format_timestamp(session.deadline_at),
            "qso_fsm"
        );
    }

    fn clear_pending_transition(session: &mut ActiveSession, now: SystemTime) {
        let pending_action = session.pending_action.take();
        let Some(pending_action) = pending_action else {
            return;
        };
        let joined = match pending_action {
            PendingAction::Transition(state) => format!("state={}", state.as_str()),
            PendingAction::Exit(reason) => format!("exit={reason}"),
        };
        Self::push_transcript(
            session,
            now,
            "SYS:",
            session.state,
            format!("fresh RX superseded queued transition: {joined}"),
        );
        Self::log_fsm(
            session,
            "queued_transition_superseded",
            session.state,
            session.state,
            session
                .last_rx_event
                .clone()
                .unwrap_or_else(|| "none".to_string()),
            None,
            None,
            now,
        );
    }

    fn roll_rx_stage_tracking(session: &mut ActiveSession, slot_start: SystemTime) {
        if session.current_rx_slot != Some(slot_start) {
            session.current_rx_slot = Some(slot_start);
            session.rx_slot_consumed_stage = None;
        }
    }

    fn should_consume_stage_event(
        session: &ActiveSession,
        stage: DecodeStage,
        event: &PartnerEvent,
    ) -> bool {
        if session.rx_slot_consumed_stage.is_some() {
            return false;
        }
        if event.has_partner_message() {
            return true;
        }
        matches!(stage, DecodeStage::Full)
    }
}

#[derive(Debug, Clone)]
struct ActiveSession {
    session_id: u64,
    partner_call: String,
    state: QsoState,
    start_mode: QsoStartMode,
    tx_slot_family: SlotFamily,
    tx_freq_hz: f32,
    latest_partner_snr_db: i32,
    rig_frequency_hz: Option<u64>,
    rig_band: Option<String>,
    app_mode: Mode,
    started_at: SystemTime,
    deadline_at: SystemTime,
    next_tx_slot: Option<SystemTime>,
    last_tx_slot: Option<SystemTime>,
    in_flight_tx: Option<InFlightTx>,
    pending_action: Option<PendingAction>,
    compound_rr73_ready_slot: Option<SystemTime>,
    pending_compound_handoff: Option<PendingCompoundHandoff>,
    no_msg_count: u32,
    no_fwd_count: u32,
    partner_rx_count: u32,
    last_rx_event: Option<String>,
    last_rx_stage: Option<DecodeStage>,
    last_rx_text: Option<String>,
    last_rx_structured_json: Option<String>,
    current_rx_slot: Option<SystemTime>,
    rx_slot_consumed_stage: Option<DecodeStage>,
    transcript: VecDeque<WebQsoTranscriptEntry>,
    sent_terminal_73: bool,
}

#[derive(Debug, Clone)]
struct InFlightTx {
    target_slot: SystemTime,
    state: QsoState,
    message_text: String,
}

#[derive(Debug, Clone)]
struct PendingCompoundHandoff {
    finished_call: String,
    next_station: StationStartInfo,
    tx_text: String,
    tx_freq_hz: f32,
    tx_slot_family: SlotFamily,
}

#[derive(Debug, Clone)]
enum PendingAction {
    Transition(QsoState),
    Exit(String),
}

#[derive(Debug, Clone)]
pub(crate) struct TxRequest {
    session_id: u64,
    target_slot: SystemTime,
    state: QsoState,
    message: TxMessage,
    message_text: String,
    tx_freq_hz: f32,
    drive_level: f32,
    playback_channels: usize,
    app_mode: Mode,
}

#[derive(Debug, Clone)]
pub(crate) enum TxEvent {
    Started {
        session_id: u64,
        state: QsoState,
        message_text: String,
    },
    Completed {
        session_id: u64,
        state: QsoState,
        message_text: String,
    },
    Aborted {
        session_id: u64,
        state: QsoState,
        message_text: String,
        reason: String,
    },
    Error {
        session_id: u64,
        state: QsoState,
        message_text: String,
        message: String,
    },
}

impl TxEvent {
    fn session_id(&self) -> u64 {
        match self {
            Self::Started { session_id, .. }
            | Self::Completed { session_id, .. }
            | Self::Aborted { session_id, .. }
            | Self::Error { session_id, .. } => *session_id,
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Started { .. } => "tx_started",
            Self::Completed { .. } => "tx_completed",
            Self::Aborted { .. } => "tx_aborted",
            Self::Error { .. } => "tx_error",
        }
    }

    fn message(&self) -> String {
        match self {
            Self::Started { message_text, .. }
            | Self::Completed { message_text, .. }
            | Self::Aborted { message_text, .. }
            | Self::Error { message_text, .. } => message_text.clone(),
        }
    }
}

pub(crate) trait TxBackend: Send {
    fn start(&mut self, request: TxRequest) -> Result<(), String>;
    fn abort(&mut self);
    fn poll_event(&mut self) -> Option<TxEvent>;
    fn is_active(&self) -> bool;
}

pub struct RigTxBackend {
    rig: Arc<Mutex<Option<K3s>>>,
    output_device: AudioDevice,
    tx_busy: Arc<AtomicBool>,
    active: bool,
    cancel: Option<Arc<AtomicBool>>,
    event_rx: Option<mpsc::Receiver<TxEvent>>,
}

impl RigTxBackend {
    pub fn new(
        rig: Arc<Mutex<Option<K3s>>>,
        output_device: AudioDevice,
        tx_busy: Arc<AtomicBool>,
    ) -> Self {
        Self {
            rig,
            output_device,
            tx_busy,
            active: false,
            cancel: None,
            event_rx: None,
        }
    }
}

impl TxBackend for RigTxBackend {
    fn start(&mut self, request: TxRequest) -> Result<(), String> {
        if self.active {
            return Err("tx backend already active".to_string());
        }
        if self
            .tx_busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err("transmit path busy".to_string());
        }
        let synthesized = synthesize_tx_message(
            &request.message,
            &WaveformOptions {
                mode: request.app_mode,
                base_freq_hz: request.tx_freq_hz,
                amplitude: request.drive_level,
                ..WaveformOptions::for_mode(request.app_mode)
            },
        )
        .map_err(|error| {
            self.tx_busy.store(false, Ordering::Release);
            error.to_string()
        })?;
        let (event_tx, event_rx) = mpsc::channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let rig = Arc::clone(&self.rig);
        let cancel_thread = Arc::clone(&cancel);
        let output_device = self.output_device.clone();
        let tx_busy = Arc::clone(&self.tx_busy);
        thread::spawn(move || {
            run_tx_thread(
                rig,
                output_device,
                request,
                synthesized.audio.sample_rate_hz,
                synthesized.audio.samples,
                cancel_thread,
                tx_busy,
                event_tx,
            );
        });
        self.active = true;
        self.cancel = Some(cancel);
        self.event_rx = Some(event_rx);
        Ok(())
    }

    fn abort(&mut self) {
        if let Some(cancel) = &self.cancel {
            cancel.store(true, Ordering::Relaxed);
        }
        force_rx(&self.rig);
        self.active = false;
        self.cancel = None;
        self.event_rx = None;
    }

    fn poll_event(&mut self) -> Option<TxEvent> {
        let event = self.event_rx.as_ref().and_then(|rx| rx.try_recv().ok());
        if matches!(
            event,
            Some(TxEvent::Completed { .. } | TxEvent::Aborted { .. } | TxEvent::Error { .. })
        ) {
            self.active = false;
            self.cancel = None;
            self.event_rx = None;
        }
        event
    }

    fn is_active(&self) -> bool {
        self.active
    }
}

pub struct UnavailableTxBackend {
    reason: String,
}

impl UnavailableTxBackend {
    pub fn new(reason: String) -> Self {
        Self { reason }
    }
}

impl TxBackend for UnavailableTxBackend {
    fn start(&mut self, _request: TxRequest) -> Result<(), String> {
        Err(self.reason.clone())
    }

    fn abort(&mut self) {}

    fn poll_event(&mut self) -> Option<TxEvent> {
        None
    }

    fn is_active(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone)]
enum PartnerEvent {
    None,
    ToUs {
        event: ToUsEvent,
        text: String,
        structured_json: String,
        snr_db: i32,
    },
    ToOther {
        text: String,
        structured_json: String,
        snr_db: i32,
    },
    Cq {
        text: String,
        structured_json: String,
        snr_db: i32,
    },
    NonCallFirstField {
        text: String,
        structured_json: String,
        snr_db: i32,
    },
    Freeform {
        text: String,
        structured_json: String,
    },
}

impl PartnerEvent {
    fn has_partner_message(&self) -> bool {
        !matches!(self, Self::None)
    }

    fn summary(&self) -> String {
        match self {
            Self::None => "none".to_string(),
            Self::ToUs { event, .. } => match event {
                ToUsEvent::Ack => "to_us_ack".to_string(),
                ToUsEvent::ReportLike => "to_us_report_like".to_string(),
                ToUsEvent::Reply(reply) => {
                    format!("to_us_reply_{}", reply_text(*reply).to_ascii_lowercase())
                }
                ToUsEvent::Other => "to_us_other".to_string(),
            },
            Self::ToOther { .. } => "to_other".to_string(),
            Self::Cq { .. } => "cq".to_string(),
            Self::NonCallFirstField { .. } => "noncall_first_field".to_string(),
            Self::Freeform { .. } => "freeform".to_string(),
        }
    }

    fn transcript_text(&self) -> String {
        match self {
            Self::None => "no partner message".to_string(),
            Self::ToUs { text, .. }
            | Self::ToOther { text, .. }
            | Self::Cq { text, .. }
            | Self::NonCallFirstField { text, .. }
            | Self::Freeform { text, .. } => format!("RX: {text}"),
        }
    }

    fn message_text(&self) -> Option<String> {
        match self {
            Self::None => None,
            Self::ToUs { text, .. }
            | Self::ToOther { text, .. }
            | Self::Cq { text, .. }
            | Self::NonCallFirstField { text, .. }
            | Self::Freeform { text, .. } => Some(text.clone()),
        }
    }

    fn structured_json(&self) -> Option<String> {
        match self {
            Self::None => None,
            Self::ToUs {
                structured_json, ..
            }
            | Self::ToOther {
                structured_json, ..
            }
            | Self::Cq {
                structured_json, ..
            }
            | Self::NonCallFirstField {
                structured_json, ..
            }
            | Self::Freeform {
                structured_json, ..
            } => Some(structured_json.clone()),
        }
    }

    fn snr_db(&self) -> Option<i32> {
        match self {
            Self::ToUs { snr_db, .. }
            | Self::ToOther { snr_db, .. }
            | Self::Cq { snr_db, .. }
            | Self::NonCallFirstField { snr_db, .. } => Some(*snr_db),
            Self::None | Self::Freeform { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ToUsEvent {
    Ack,
    ReportLike,
    Reply(ReplyWord),
    Other,
}

fn build_tx_request(
    config: &AppConfig,
    session: &ActiveSession,
    target_slot: SystemTime,
) -> TxRequest {
    let payload = match session.state {
        QsoState::Idle => TxDirectedPayload::Blank,
        QsoState::SendCq => TxDirectedPayload::Blank,
        QsoState::SendGrid => TxDirectedPayload::Grid(config.station.our_grid.clone()),
        QsoState::SendSig => {
            TxDirectedPayload::Signal(clamp_report_db(session.latest_partner_snr_db))
        }
        QsoState::SendSigAck => {
            TxDirectedPayload::SignalWithAck(clamp_report_db(session.latest_partner_snr_db))
        }
        QsoState::SendRR73 => TxDirectedPayload::Reply(ReplyWord::Rr73),
        QsoState::SendRRR => TxDirectedPayload::Reply(ReplyWord::Rrr),
        QsoState::Send73 | QsoState::Send73Once => {
            TxDirectedPayload::Reply(ReplyWord::SeventyThree)
        }
    };
    let message = if let Some(handoff) = &session.pending_compound_handoff {
        if matches!(session.state, QsoState::SendRR73 | QsoState::Send73Once) {
            TxMessage::DxpeditionCompound {
                finished_call: handoff.finished_call.clone(),
                next_call: handoff.next_station.callsign.clone(),
                my_call: config.station.our_call.clone(),
                report_db: clamp_report_db(handoff.next_station.last_snr_db),
            }
        } else if session.state == QsoState::SendCq {
            TxMessage::Cq {
                my_call: config.station.our_call.clone(),
                my_grid: Some(config.station.our_grid.clone()),
            }
        } else {
            TxMessage::Directed {
                my_call: config.station.our_call.clone(),
                peer_call: session.partner_call.clone(),
                payload,
            }
        }
    } else if session.state == QsoState::SendCq {
        TxMessage::Cq {
            my_call: config.station.our_call.clone(),
            my_grid: Some(config.station.our_grid.clone()),
        }
    } else {
        TxMessage::Directed {
            my_call: config.station.our_call.clone(),
            peer_call: session.partner_call.clone(),
            payload,
        }
    };
    let message_text = render_tx_message(config, session);
    TxRequest {
        session_id: session.session_id,
        target_slot,
        state: session.state,
        message,
        message_text,
        tx_freq_hz: session.tx_freq_hz,
        drive_level: config.tx.drive_level,
        playback_channels: config.tx.playback_channels,
        app_mode: session.app_mode,
    }
}

fn render_tx_message(config: &AppConfig, session: &ActiveSession) -> String {
    if let Some(handoff) = &session.pending_compound_handoff {
        if matches!(session.state, QsoState::SendRR73 | QsoState::Send73Once) {
            return handoff.tx_text.clone();
        }
    }
    match session.state {
        QsoState::Idle => String::new(),
        QsoState::SendCq => format!("CQ {} {}", config.station.our_call, config.station.our_grid),
        QsoState::SendGrid => format!(
            "{} {} {}",
            session.partner_call, config.station.our_call, config.station.our_grid
        ),
        QsoState::SendSig => format!(
            "{} {} {:+03}",
            session.partner_call,
            config.station.our_call,
            clamp_report_db(session.latest_partner_snr_db)
        ),
        QsoState::SendSigAck => format!(
            "{} {} R{:+03}",
            session.partner_call,
            config.station.our_call,
            clamp_report_db(session.latest_partner_snr_db)
        ),
        QsoState::SendRR73 => format!("{} {} RR73", session.partner_call, config.station.our_call),
        QsoState::SendRRR => format!("{} {} RRR", session.partner_call, config.station.our_call),
        QsoState::Send73 | QsoState::Send73Once => {
            format!("{} {} 73", session.partner_call, config.station.our_call)
        }
    }
}

fn clamp_report_db(value: i32) -> i16 {
    value.clamp(REPORT_MIN_DB, REPORT_MAX_DB) as i16
}

fn classify_partner_event(
    decodes: &[ft8_decoder::DecodedMessage],
    partner_call: &str,
    our_call: &str,
    state: QsoState,
) -> PartnerEvent {
    let mut best_to_us: Option<(u8, PartnerEvent)> = None;
    let mut to_other: Option<PartnerEvent> = None;
    let mut cq: Option<PartnerEvent> = None;
    let mut noncall_first_field: Option<PartnerEvent> = None;
    let mut freeform: Option<PartnerEvent> = None;

    for decode in decodes {
        let sender = semantic_sender_call(&decode.message);
        if sender.as_deref() != Some(partner_call) {
            continue;
        }
        match classify_single_message(&decode.message, our_call, state) {
            SingleClass::ToUs(event) => {
                let score = to_us_priority(event, state);
                let candidate = PartnerEvent::ToUs {
                    event,
                    text: decode.text.clone(),
                    structured_json: serialize_structured_message(&decode.message),
                    snr_db: decode.snr_db,
                };
                if best_to_us
                    .as_ref()
                    .map(|(best, _)| score > *best)
                    .unwrap_or(true)
                {
                    best_to_us = Some((score, candidate));
                }
            }
            SingleClass::ToOther => {
                to_other.get_or_insert(PartnerEvent::ToOther {
                    text: decode.text.clone(),
                    structured_json: serialize_structured_message(&decode.message),
                    snr_db: decode.snr_db,
                });
            }
            SingleClass::Cq => {
                cq.get_or_insert(PartnerEvent::Cq {
                    text: decode.text.clone(),
                    structured_json: serialize_structured_message(&decode.message),
                    snr_db: decode.snr_db,
                });
            }
            SingleClass::NonCallFirstField => {
                noncall_first_field.get_or_insert(PartnerEvent::NonCallFirstField {
                    text: decode.text.clone(),
                    structured_json: serialize_structured_message(&decode.message),
                    snr_db: decode.snr_db,
                });
            }
            SingleClass::Freeform => {
                freeform.get_or_insert(PartnerEvent::Freeform {
                    text: decode.text.clone(),
                    structured_json: serialize_structured_message(&decode.message),
                });
            }
            SingleClass::Irrelevant => {}
        }
    }

    if let Some((_, event)) = best_to_us {
        event
    } else if let Some(event) = to_other {
        event
    } else if let Some(event) = cq {
        event
    } else if let Some(event) = noncall_first_field {
        event
    } else if let Some(event) = freeform {
        event
    } else {
        PartnerEvent::None
    }
}

enum SingleClass {
    ToUs(ToUsEvent),
    ToOther,
    Cq,
    NonCallFirstField,
    Freeform,
    Irrelevant,
}

fn classify_single_message(
    message: &StructuredMessage,
    our_call: &str,
    state: QsoState,
) -> SingleClass {
    match message {
        StructuredMessage::Standard {
            first,
            acknowledge,
            info,
            ..
        } => {
            if let ft8_decoder::StructuredCallValue::Token { token } = &first.value {
                return if token == "CQ" {
                    SingleClass::Cq
                } else {
                    SingleClass::NonCallFirstField
                };
            }
            let target = structured_call_station_name(first);
            if target.as_deref() == Some(our_call) {
                match &info.value {
                    StructuredInfoValue::Grid { locator }
                        if locator.eq_ignore_ascii_case("RR73") =>
                    {
                        SingleClass::ToUs(ToUsEvent::Reply(ReplyWord::Rr73))
                    }
                    StructuredInfoValue::Reply { word } => {
                        SingleClass::ToUs(ToUsEvent::Reply(*word))
                    }
                    StructuredInfoValue::Grid { .. } | StructuredInfoValue::SignalReport { .. } => {
                        if *acknowledge {
                            SingleClass::ToUs(ToUsEvent::Ack)
                        } else {
                            SingleClass::ToUs(ToUsEvent::ReportLike)
                        }
                    }
                    StructuredInfoValue::Blank => {
                        if *acknowledge {
                            SingleClass::ToUs(ToUsEvent::Ack)
                        } else {
                            SingleClass::ToUs(ToUsEvent::Other)
                        }
                    }
                }
            } else if target.as_deref() == Some("CQ") {
                SingleClass::Cq
            } else if target.is_some() {
                if matches!(
                    state,
                    QsoState::Send73 | QsoState::SendRR73 | QsoState::SendRRR
                ) {
                    SingleClass::ToOther
                } else {
                    SingleClass::Irrelevant
                }
            } else {
                SingleClass::Irrelevant
            }
        }
        StructuredMessage::Nonstandard { reply, cq, .. } => {
            if *cq {
                return SingleClass::Cq;
            }
            let target = semantic_first_call_display_call(message);
            if target.as_deref() == Some(our_call) {
                if matches!(reply, ReplyWord::Blank) {
                    SingleClass::ToUs(ToUsEvent::Other)
                } else {
                    SingleClass::ToUs(ToUsEvent::Reply(*reply))
                }
            } else if target.is_some() {
                if matches!(
                    state,
                    QsoState::Send73 | QsoState::SendRR73 | QsoState::SendRRR
                ) {
                    SingleClass::ToOther
                } else {
                    SingleClass::Irrelevant
                }
            } else {
                SingleClass::Irrelevant
            }
        }
        StructuredMessage::Dxpedition {
            completed_call,
            next_call,
            ..
        } => {
            let completed_target = structured_call_station_name(completed_call);
            let next_target = structured_call_station_name(next_call);
            if completed_target.as_deref() == Some(our_call) {
                SingleClass::ToUs(ToUsEvent::Reply(ReplyWord::Rr73))
            } else if next_target.as_deref() == Some(our_call) {
                SingleClass::ToUs(ToUsEvent::ReportLike)
            } else if completed_target.is_some() || next_target.is_some() {
                if matches!(
                    state,
                    QsoState::Send73 | QsoState::SendRR73 | QsoState::SendRRR
                ) {
                    SingleClass::ToOther
                } else {
                    SingleClass::Irrelevant
                }
            } else {
                SingleClass::Irrelevant
            }
        }
        StructuredMessage::FieldDay { .. }
        | StructuredMessage::RttyContest { .. }
        | StructuredMessage::EuVhf { .. } => SingleClass::Freeform,
        StructuredMessage::FreeText { .. } | StructuredMessage::Unsupported { .. } => {
            SingleClass::Freeform
        }
    }
}

fn to_us_priority(event: ToUsEvent, state: QsoState) -> u8 {
    match state {
        QsoState::SendCq => 1,
        QsoState::SendGrid => match event {
            ToUsEvent::Reply(_) => 4,
            ToUsEvent::Ack => 3,
            ToUsEvent::ReportLike => 3,
            ToUsEvent::Other => 1,
        },
        QsoState::SendSig => match event {
            ToUsEvent::Reply(ReplyWord::SeventyThree) => 5,
            ToUsEvent::Ack => 4,
            ToUsEvent::Reply(_) => 4,
            ToUsEvent::ReportLike => 1,
            ToUsEvent::Other => 1,
        },
        QsoState::SendSigAck => match event {
            ToUsEvent::Reply(_) => 4,
            ToUsEvent::Ack => 4,
            ToUsEvent::ReportLike => 1,
            ToUsEvent::Other => 1,
        },
        QsoState::SendRR73 => match event {
            ToUsEvent::Reply(ReplyWord::SeventyThree) => 4,
            ToUsEvent::Reply(_) | ToUsEvent::Ack | ToUsEvent::ReportLike | ToUsEvent::Other => 1,
        },
        QsoState::SendRRR => match event {
            ToUsEvent::Reply(ReplyWord::SeventyThree) => 4,
            ToUsEvent::Reply(_) | ToUsEvent::Ack | ToUsEvent::ReportLike | ToUsEvent::Other => 1,
        },
        QsoState::Send73 => match event {
            ToUsEvent::Reply(ReplyWord::SeventyThree) => 4,
            ToUsEvent::Reply(_) | ToUsEvent::Ack | ToUsEvent::ReportLike | ToUsEvent::Other => 1,
        },
        QsoState::Send73Once | QsoState::Idle => 1,
    }
}

fn run_tx_thread(
    rig: Arc<Mutex<Option<K3s>>>,
    output_device: AudioDevice,
    request: TxRequest,
    sample_rate_hz: u32,
    samples: Vec<f32>,
    cancel: Arc<AtomicBool>,
    tx_busy: Arc<AtomicBool>,
    event_tx: mpsc::Sender<TxEvent>,
) {
    let _busy_guard = TxBusyGuard::new(tx_busy);
    let key_target = request
        .target_slot
        .checked_sub(Duration::from_millis(PRE_KEY_MS))
        .unwrap_or(request.target_slot);
    if wait_until(key_target, &cancel) {
        let _ = event_tx.send(TxEvent::Aborted {
            session_id: request.session_id,
            state: request.state,
            message_text: request.message_text,
            reason: "cancelled_before_key".to_string(),
        });
        return;
    }

    if let Err(error) = with_rig(&rig, |rig| rig.enter_tx()) {
        let _ = event_tx.send(TxEvent::Error {
            session_id: request.session_id,
            state: request.state,
            message_text: request.message_text,
            message: format!("enter_tx failed: {error}"),
        });
        return;
    }

    let prepared_playback = match prepare_mono_playback(
        &output_device,
        sample_rate_hz,
        request.playback_channels,
        &samples,
    ) {
        Ok(playback) => playback,
        Err(error) => {
            force_rx(&rig);
            let _ = event_tx.send(TxEvent::Error {
                session_id: request.session_id,
                state: request.state,
                message_text: request.message_text,
                message: format!("prepare playback failed: {error}"),
            });
            return;
        }
    };

    if wait_until(request.target_slot, &cancel) {
        force_rx(&rig);
        let _ = event_tx.send(TxEvent::Aborted {
            session_id: request.session_id,
            state: request.state,
            message_text: request.message_text,
            reason: "cancelled_before_audio".to_string(),
        });
        return;
    }

    let _ = event_tx.send(TxEvent::Started {
        session_id: request.session_id,
        state: request.state,
        message_text: request.message_text.clone(),
    });

    match prepared_playback.play_until(Some(cancel.as_ref())) {
        Ok(()) if cancel.load(Ordering::Relaxed) => {
            force_rx(&rig);
            let _ = event_tx.send(TxEvent::Aborted {
                session_id: request.session_id,
                state: request.state,
                message_text: request.message_text,
                reason: format!("cancelled_in_{}", request.state.as_str()),
            });
        }
        Ok(()) => {
            force_rx(&rig);
            let _ = event_tx.send(TxEvent::Completed {
                session_id: request.session_id,
                state: request.state,
                message_text: request.message_text,
            });
        }
        Err(error) => {
            force_rx(&rig);
            let _ = event_tx.send(TxEvent::Error {
                session_id: request.session_id,
                state: request.state,
                message_text: request.message_text,
                message: error.to_string(),
            });
        }
    }
}

struct TxBusyGuard {
    tx_busy: Arc<AtomicBool>,
}

impl TxBusyGuard {
    fn new(tx_busy: Arc<AtomicBool>) -> Self {
        Self { tx_busy }
    }
}

impl Drop for TxBusyGuard {
    fn drop(&mut self) {
        self.tx_busy.store(false, Ordering::Release);
    }
}

fn wait_until(target: SystemTime, cancel: &AtomicBool) -> bool {
    loop {
        if cancel.load(Ordering::Relaxed) {
            return true;
        }
        let now = SystemTime::now();
        match target.duration_since(now) {
            Ok(remaining) if remaining > Duration::from_millis(5) => {
                thread::sleep(remaining.min(Duration::from_millis(50)));
            }
            _ => return cancel.load(Ordering::Relaxed),
        }
    }
}

fn force_rx(rig: &Arc<Mutex<Option<K3s>>>) {
    if let Err(error) = with_rig(rig, |rig| rig.enter_rx()) {
        error!(message = %error, "force_rx_failed");
    }
}

fn with_rig<T>(
    rig: &Arc<Mutex<Option<K3s>>>,
    f: impl FnOnce(&mut K3s) -> Result<T, rigctl::Error>,
) -> Result<T, String> {
    let mut guard = rig.lock().expect("rig mutex poisoned");
    let rig = guard
        .as_mut()
        .ok_or_else(|| "rig unavailable".to_string())?;
    f(rig).map_err(|error| error.to_string())
}

fn first_matching_slot_after(now: SystemTime, family: SlotFamily, app_mode: Mode) -> SystemTime {
    let mut slot = crate::next_slot_boundary_for_mode(app_mode, now);
    let key_time = slot
        .checked_sub(Duration::from_millis(PRE_KEY_MS))
        .unwrap_or(slot);
    if now >= key_time {
        slot += crate::slot_duration_for_mode(app_mode);
    }
    while slot_family_for_mode(app_mode, slot) != family {
        slot += crate::slot_duration_for_mode(app_mode);
    }
    slot
}

fn next_matching_slot_after(
    slot_start: SystemTime,
    family: SlotFamily,
    app_mode: Mode,
) -> Option<SystemTime> {
    let mut slot = slot_start + crate::slot_duration_for_mode(app_mode);
    while slot_family_for_mode(app_mode, slot) != family {
        slot += crate::slot_duration_for_mode(app_mode);
    }
    Some(slot)
}

fn schedule_next_tx_slot(session: &ActiveSession, rx_slot_start: SystemTime) -> Option<SystemTime> {
    let mut candidate =
        next_matching_slot_after(rx_slot_start, session.tx_slot_family, session.app_mode)?;
    if let Some(last_tx_slot) = session.last_tx_slot {
        while candidate <= last_tx_slot {
            candidate =
                next_matching_slot_after(candidate, session.tx_slot_family, session.app_mode)?;
        }
    }
    Some(candidate)
}

fn is_next_tx_slot_committed(
    session: &ActiveSession,
    rx_slot_start: SystemTime,
    tx_backend_active: bool,
) -> bool {
    if !tx_backend_active {
        return false;
    }
    let Some(candidate_slot) =
        next_matching_slot_after(rx_slot_start, session.tx_slot_family, session.app_mode)
    else {
        return false;
    };
    session.last_tx_slot == Some(candidate_slot)
}

pub fn slot_family(time: SystemTime) -> SlotFamily {
    slot_family_for_mode(Mode::Ft8, time)
}

pub fn slot_family_for_mode(app_mode: Mode, time: SystemTime) -> SlotFamily {
    if crate::is_even_slot_family_for_mode(app_mode, time) {
        SlotFamily::Even
    } else {
        SlotFamily::Odd
    }
}

fn format_timestamp(time: SystemTime) -> String {
    let utc: DateTime<Utc> = time.into();
    utc.format("%H:%M:%S").to_string()
}

fn semantic_sender_call(message: &StructuredMessage) -> Option<String> {
    match message {
        StructuredMessage::Standard { second, .. } => structured_call_station_name(second),
        StructuredMessage::Nonstandard {
            hashed_call,
            plain_call,
            hashed_is_second,
            cq,
            ..
        } => {
            if *cq {
                Some(plain_call.callsign.clone())
            } else if *hashed_is_second {
                hashed_call.resolved_callsign.clone()
            } else {
                Some(plain_call.callsign.clone())
            }
        }
        StructuredMessage::Dxpedition { hashed_call10, .. } => {
            hashed_call10.resolved_callsign.clone()
        }
        StructuredMessage::FieldDay { second, .. }
        | StructuredMessage::RttyContest { second, .. } => structured_call_station_name(second),
        StructuredMessage::EuVhf { hashed_call22, .. } => hashed_call22.resolved_callsign.clone(),
        StructuredMessage::FreeText { .. } | StructuredMessage::Unsupported { .. } => None,
    }
}

fn semantic_first_call_display_call(message: &StructuredMessage) -> Option<String> {
    match message {
        StructuredMessage::Standard { first, .. } => structured_call_station_name(first),
        StructuredMessage::Nonstandard {
            hashed_call,
            plain_call,
            hashed_is_second,
            cq,
            ..
        } => {
            if *cq {
                None
            } else if *hashed_is_second {
                Some(plain_call.callsign.clone())
            } else {
                hashed_call.resolved_callsign.clone()
            }
        }
        StructuredMessage::FieldDay { first, .. }
        | StructuredMessage::RttyContest { first, .. } => structured_call_station_name(first),
        StructuredMessage::Dxpedition { completed_call, .. } => {
            structured_call_station_name(completed_call)
        }
        StructuredMessage::EuVhf { hashed_call12, .. } => hashed_call12.resolved_callsign.clone(),
        StructuredMessage::FreeText { .. } | StructuredMessage::Unsupported { .. } => None,
    }
}

fn structured_call_station_name(field: &ft8_decoder::StructuredCallField) -> Option<String> {
    match &field.value {
        ft8_decoder::StructuredCallValue::StandardCall { callsign } => Some(callsign.clone()),
        ft8_decoder::StructuredCallValue::Hash22 {
            resolved_callsign: Some(callsign),
            ..
        } => Some(callsign.clone()),
        ft8_decoder::StructuredCallValue::Token { .. }
        | ft8_decoder::StructuredCallValue::Hash22 { .. } => None,
    }
}

fn reply_text(word: ReplyWord) -> &'static str {
    match word {
        ReplyWord::Blank => "",
        ReplyWord::Rrr => "RRR",
        ReplyWord::Rr73 => "RR73",
        ReplyWord::SeventyThree => "73",
    }
}

fn serialize_structured_message(message: &StructuredMessage) -> String {
    serde_json::to_string(message).unwrap_or_else(|error| {
        format!(
            "{{\"serialize_error\":{},\"fallback_text\":{}}}",
            serde_json::to_string(&error.to_string()).unwrap_or_else(|_| "\"unknown\"".to_string()),
            serde_json::to_string(&message.to_text()).unwrap_or_else(|_| "\"\"".to_string())
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        FsmConfig, LoggingConfig, NoFwdThreshold, QueueConfig, RetryThresholds, StationConfig,
        TxConfig,
    };
    use ft8_decoder::{
        DecodeOptions, DecodeProfile, DecodedMessage, DecoderSession, HashedCallField10,
        StructuredCallField, StructuredCallValue, StructuredInfoField, TxDirectedPayload,
        TxMessage, WaveformOptions, synthesize_tx_message,
    };

    #[derive(Default)]
    struct MockTxBackend {
        active: bool,
        events: VecDeque<TxEvent>,
        launches: Vec<String>,
    }

    impl TxBackend for MockTxBackend {
        fn start(&mut self, request: TxRequest) -> Result<(), String> {
            self.active = true;
            self.launches.push(request.message_text.clone());
            self.events.push_back(TxEvent::Started {
                session_id: request.session_id,
                state: request.state,
                message_text: request.message_text.clone(),
            });
            self.events.push_back(TxEvent::Completed {
                session_id: request.session_id,
                state: request.state,
                message_text: request.message_text,
            });
            Ok(())
        }

        fn abort(&mut self) {
            self.active = false;
        }

        fn poll_event(&mut self) -> Option<TxEvent> {
            let event = self.events.pop_front();
            if matches!(
                event,
                Some(TxEvent::Completed { .. } | TxEvent::Aborted { .. } | TxEvent::Error { .. })
            ) {
                self.active = false;
            }
            event
        }

        fn is_active(&self) -> bool {
            self.active
        }
    }

    fn sample_config() -> AppConfig {
        AppConfig {
            station: StationConfig {
                our_call: "N1VF".to_string(),
                our_grid: "CM97".to_string(),
            },
            tx: TxConfig {
                base_freq_hz: 1000.0,
                drive_level: 0.12,
                playback_channels: 2,
                output_device: None,
                power_w: None,
                tx_freq_min_hz: 200.0,
                tx_freq_max_hz: 3500.0,
            },
            queue: QueueConfig {
                auto_add_all_decoded_calls_default: false,
                auto_add_decoded_min_count_5m_default: 2,
                auto_add_direct_calls_default: true,
                ignore_direct_calls_from_recently_worked_default: true,
                cq_enabled_default: false,
                cq_percent_default: 80,
                pause_cq_when_few_unique_calls_default: false,
                cq_pause_min_unique_calls_5m_default: 3,
                use_compound_rr73_handoff_default: true,
                use_compound_73_once_handoff_default: false,
                use_compound_for_direct_signal_callers_default: false,
                no_message_retry_delay_seconds_default: 35,
                no_forward_retry_delay_seconds_default: 300,
            },
            fsm: FsmConfig {
                rr73_enabled: true,
                timeout_seconds: 600,
                send_grid: RetryThresholds {
                    no_fwd: 3,
                    no_msg: 3,
                },
                send_sig: RetryThresholds {
                    no_fwd: 3,
                    no_msg: 3,
                },
                send_sig_ack: RetryThresholds {
                    no_fwd: 5,
                    no_msg: 2,
                },
                send_rr73: NoFwdThreshold { no_fwd: 3 },
                send_rrr: RetryThresholds {
                    no_fwd: 5,
                    no_msg: 2,
                },
                send_73: RetryThresholds {
                    no_fwd: 3,
                    no_msg: 2,
                },
            },
            logging: LoggingConfig {
                fsm_log_path: "logs/test.jsonl".to_string(),
                app_log_path: "logs/test.log".to_string(),
            },
        }
    }

    fn directed_decode(from: &str, to: &str, event: ToUsEvent) -> DecodedMessage {
        let acknowledge = matches!(event, ToUsEvent::Ack);
        let info = match event {
            ToUsEvent::Ack | ToUsEvent::Other => ft8_decoder::StructuredInfoValue::Blank,
            ToUsEvent::ReportLike => ft8_decoder::StructuredInfoValue::SignalReport { db: -8 },
            ToUsEvent::Reply(word) => ft8_decoder::StructuredInfoValue::Reply { word },
        };
        DecodedMessage {
            utc: "00:00:00".to_string(),
            snr_db: -7,
            dt_seconds: 0.1,
            freq_hz: 1000.0,
            text: format!("{from} {to}"),
            candidate_score: 0.0,
            ldpc_iterations: 0,
            message: StructuredMessage::Standard {
                i3: 1,
                first: StructuredCallField {
                    raw: 0,
                    value: StructuredCallValue::StandardCall {
                        callsign: to.to_string(),
                    },
                    modifier: None,
                },
                second: StructuredCallField {
                    raw: 0,
                    value: StructuredCallValue::StandardCall {
                        callsign: from.to_string(),
                    },
                    modifier: None,
                },
                acknowledge,
                info: StructuredInfoField {
                    raw: 0,
                    value: info,
                },
            },
        }
    }

    fn cq_decode(from: &str) -> DecodedMessage {
        DecodedMessage {
            utc: "00:00:00".to_string(),
            snr_db: -7,
            dt_seconds: 0.1,
            freq_hz: 1000.0,
            text: format!("CQ {from}"),
            candidate_score: 0.0,
            ldpc_iterations: 0,
            message: StructuredMessage::Standard {
                i3: 1,
                first: StructuredCallField {
                    raw: 0,
                    value: StructuredCallValue::Token {
                        token: "CQ".to_string(),
                    },
                    modifier: None,
                },
                second: StructuredCallField {
                    raw: 0,
                    value: StructuredCallValue::StandardCall {
                        callsign: from.to_string(),
                    },
                    modifier: None,
                },
                acknowledge: false,
                info: StructuredInfoField {
                    raw: 0,
                    value: ft8_decoder::StructuredInfoValue::Blank,
                },
            },
        }
    }

    fn token_first_decode(token: &str, from: &str) -> DecodedMessage {
        DecodedMessage {
            utc: "00:00:00".to_string(),
            snr_db: -7,
            dt_seconds: 0.1,
            freq_hz: 1000.0,
            text: format!("{token} {from}"),
            candidate_score: 0.0,
            ldpc_iterations: 0,
            message: StructuredMessage::Standard {
                i3: 1,
                first: StructuredCallField {
                    raw: 0,
                    value: StructuredCallValue::Token {
                        token: token.to_string(),
                    },
                    modifier: None,
                },
                second: StructuredCallField {
                    raw: 0,
                    value: StructuredCallValue::StandardCall {
                        callsign: from.to_string(),
                    },
                    modifier: None,
                },
                acknowledge: false,
                info: StructuredInfoField {
                    raw: 0,
                    value: StructuredInfoValue::Blank,
                },
            },
        }
    }

    fn dxpedition_decode(
        sender: &str,
        completed: &str,
        next: &str,
        report_db: i16,
    ) -> DecodedMessage {
        let message = StructuredMessage::Dxpedition {
            i3: 0,
            n3: 1,
            completed_call: StructuredCallField {
                raw: 0,
                value: StructuredCallValue::StandardCall {
                    callsign: completed.to_string(),
                },
                modifier: None,
            },
            next_call: StructuredCallField {
                raw: 0,
                value: StructuredCallValue::StandardCall {
                    callsign: next.to_string(),
                },
                modifier: None,
            },
            hashed_call10: HashedCallField10 {
                raw: 0,
                resolved_callsign: Some(sender.to_string()),
            },
            report_db,
        };
        DecodedMessage {
            utc: "00:00:00".to_string(),
            snr_db: -7,
            dt_seconds: 0.1,
            freq_hz: 1000.0,
            text: message.to_text(),
            candidate_score: 0.0,
            ldpc_iterations: 0,
            message,
        }
    }

    fn synthesized_stage_reports(message: TxMessage) -> Vec<ft8_decoder::StageDecodeReport> {
        let synthesized = synthesize_tx_message(
            &message,
            &WaveformOptions {
                mode: Mode::Ft8,
                base_freq_hz: 1_000.0,
                ..WaveformOptions::for_mode(Mode::Ft8)
            },
        )
        .expect("synthesize tx message");
        let mut session = DecoderSession::new();
        session
            .decode_available(
                &synthesized.audio,
                &DecodeOptions {
                    profile: DecodeProfile::Medium,
                    max_candidates: 16,
                    max_successes: 4,
                    ..DecodeOptions::default()
                },
            )
            .expect("decode synthesized audio")
    }

    fn station_start_info(callsign: &str, now: SystemTime, family: SlotFamily) -> StationStartInfo {
        StationStartInfo {
            callsign: callsign.to_string(),
            last_heard_at: now,
            last_heard_slot_family: family,
            last_snr_db: -9,
            last_text: None,
            last_structured_json: None,
        }
    }

    fn start_command(partner_call: &str, tx_freq_hz: f32) -> QsoCommand {
        QsoCommand::Start {
            partner_call: partner_call.to_string(),
            tx_freq_hz,
            initial_state: QsoState::SendGrid,
            start_mode: QsoStartMode::Normal,
            tx_slot_family_override: None,
        }
    }

    #[test]
    fn cq_start_can_force_tx_slot_family() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(31);
        controller.handle_command(
            QsoCommand::Start {
                partner_call: "CQ".to_string(),
                tx_freq_hz: 1000.0,
                initial_state: QsoState::SendCq,
                start_mode: QsoStartMode::Cq,
                tx_slot_family_override: Some(SlotFamily::Odd),
            },
            None,
            now,
        );
        let snapshot = controller.snapshot(now);
        assert_eq!(snapshot.tx_slot_family, Some("odd".to_string()));
    }

    #[test]
    fn start_infers_opposite_slot_family() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        controller.handle_command(
            start_command("K1ABC", 1000.0),
            Some(StationStartInfo {
                callsign: "K1ABC".to_string(),
                last_heard_at: now,
                last_heard_slot_family: SlotFamily::Even,
                last_snr_db: -9,
                last_text: None,
                last_structured_json: None,
            }),
            now,
        );
        assert_eq!(
            controller.snapshot(now).tx_slot_family.as_deref(),
            Some("odd")
        );
    }

    #[test]
    fn start_context_seeds_last_received_decode() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let mut station_info = station_start_info("K1ABC", now, SlotFamily::Odd);
        station_info.last_text = Some("CQ K1ABC CM97".to_string());
        station_info.last_structured_json = Some("{\"kind\":\"test\"}".to_string());
        controller.handle_command(start_command("K1ABC", 1000.0), Some(station_info), now);
        let snapshot = controller.snapshot(now);
        assert_eq!(snapshot.last_rx_event.as_deref(), Some("start_context"));
        assert!(
            snapshot
                .transcript
                .iter()
                .any(|entry| entry.text == "start context: CQ K1ABC CM97")
        );
    }

    #[test]
    fn send_grid_ack_transitions_to_sig_ack() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        controller.handle_command(
            start_command("K1ABC", 1000.0),
            Some(StationStartInfo {
                callsign: "K1ABC".to_string(),
                last_heard_at: now,
                last_heard_slot_family: SlotFamily::Odd,
                last_snr_db: -9,
                last_text: None,
                last_structured_json: None,
            }),
            now,
        );
        controller.on_full_decode(
            now + Duration::from_secs(15),
            &[directed_decode("K1ABC", "N1VF", ToUsEvent::Ack)],
            now + Duration::from_secs(15),
        );
        assert_eq!(controller.snapshot(now).state, "send_sig_ack");
    }

    #[test]
    fn send_grid_plain_signal_report_transitions_to_sig_ack() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        controller.handle_command(
            start_command("K1ABC", 1000.0),
            Some(StationStartInfo {
                callsign: "K1ABC".to_string(),
                last_heard_at: now,
                last_heard_slot_family: SlotFamily::Odd,
                last_snr_db: -9,
                last_text: None,
                last_structured_json: None,
            }),
            now,
        );
        controller.on_full_decode(
            now + Duration::from_secs(15),
            &[DecodedMessage {
                utc: "00:00:00".to_string(),
                snr_db: -8,
                dt_seconds: 0.1,
                freq_hz: 1000.0,
                text: "N1VF K1ABC -08".to_string(),
                candidate_score: 0.0,
                ldpc_iterations: 0,
                message: StructuredMessage::Standard {
                    i3: 1,
                    first: StructuredCallField {
                        raw: 0,
                        value: StructuredCallValue::StandardCall {
                            callsign: "N1VF".to_string(),
                        },
                        modifier: None,
                    },
                    second: StructuredCallField {
                        raw: 0,
                        value: StructuredCallValue::StandardCall {
                            callsign: "K1ABC".to_string(),
                        },
                        modifier: None,
                    },
                    acknowledge: false,
                    info: StructuredInfoField {
                        raw: 0,
                        value: ft8_decoder::StructuredInfoValue::SignalReport { db: -8 },
                    },
                },
            }],
            now + Duration::from_secs(15),
        );
        assert_eq!(controller.snapshot(now).state, "send_sig_ack");
    }

    #[test]
    fn synthesized_rr73_decode_advances_send_sig_ack_to_send_73_once() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let rx_slot_start = now + Duration::from_secs(15);
        controller.handle_command(
            start_command("W5XO", 1_000.0),
            Some(StationStartInfo {
                callsign: "W5XO".to_string(),
                last_heard_at: now,
                last_heard_slot_family: SlotFamily::Odd,
                last_snr_db: -9,
                last_text: None,
                last_structured_json: None,
            }),
            now,
        );
        controller.session.as_mut().expect("session").state = QsoState::SendSigAck;

        let reports = synthesized_stage_reports(TxMessage::Directed {
            my_call: "W5XO".to_string(),
            peer_call: "N1VF".to_string(),
            payload: TxDirectedPayload::Reply(ReplyWord::Rr73),
        });
        assert!(
            !reports.is_empty(),
            "expected at least one decode stage for synthesized RR73"
        );

        let mut saw_rr73 = false;
        for report in &reports {
            let texts: Vec<_> = report
                .report
                .decodes
                .iter()
                .map(|decode| decode.text.as_str())
                .collect();
            if texts.contains(&"N1VF W5XO RR73") {
                saw_rr73 = true;
                assert!(
                    matches!(
                        classify_partner_event(
                            &report.report.decodes,
                            "W5XO",
                            "N1VF",
                            QsoState::SendSigAck,
                        ),
                        PartnerEvent::ToUs {
                            event: ToUsEvent::Reply(ReplyWord::Rr73),
                            ..
                        }
                    ),
                    "decoded RR73 should classify as reply, stage={}, texts={texts:?}",
                    report.stage.as_str(),
                );
            }
            controller.on_decode_stage(
                rx_slot_start,
                report.stage,
                &report.report.decodes,
                rx_slot_start + Duration::from_secs(11),
            );
        }

        assert!(saw_rr73, "expected synthesized RR73 in decoder outputs");
        let snapshot = controller.snapshot(now);
        assert_eq!(snapshot.state, "send_73_once");
        assert_eq!(snapshot.last_rx_event.as_deref(), Some("to_us_reply_rr73"));
    }

    #[test]
    fn send_sig_rr73_transitions_to_send_73_once() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        controller.handle_command(
            start_command("K1ABC", 1000.0),
            Some(StationStartInfo {
                callsign: "K1ABC".to_string(),
                last_heard_at: now,
                last_heard_slot_family: SlotFamily::Odd,
                last_snr_db: -9,
                last_text: None,
                last_structured_json: None,
            }),
            now,
        );
        if let Some(session) = controller.session.as_mut() {
            session.state = QsoState::SendSig;
        }
        controller.on_full_decode(
            now + Duration::from_secs(15),
            &[directed_decode(
                "K1ABC",
                "N1VF",
                ToUsEvent::Reply(ReplyWord::Rr73),
            )],
            now + Duration::from_secs(15),
        );
        assert_eq!(controller.snapshot(now).state, "send_73_once");
    }

    #[test]
    fn send_sig_ack_dxpedition_rr73_to_us_transitions_to_send_73_once() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        controller.handle_command(
            start_command("PY7ZZ", 1000.0),
            Some(StationStartInfo {
                callsign: "PY7ZZ".to_string(),
                last_heard_at: now,
                last_heard_slot_family: SlotFamily::Odd,
                last_snr_db: -9,
                last_text: None,
                last_structured_json: None,
            }),
            now,
        );
        if let Some(session) = controller.session.as_mut() {
            session.state = QsoState::SendSigAck;
        }
        controller.on_full_decode(
            now + Duration::from_secs(15),
            &[dxpedition_decode("PY7ZZ", "N1VF", "SP4MCH", -18)],
            now + Duration::from_secs(15),
        );
        let snapshot = controller.snapshot(now);
        assert_eq!(snapshot.state, "send_73_once");
        assert_eq!(snapshot.last_rx_event.as_deref(), Some("to_us_reply_rr73"));
    }

    #[test]
    fn compound_rr73_handoff_reuses_rr73_slot_and_starts_follow_on_qso() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let rx_slot_start = now + Duration::from_secs(15);
        controller.handle_command(
            start_command("K1ABC", 1225.0),
            Some(station_start_info("K1ABC", now, SlotFamily::Odd)),
            now,
        );
        controller.session.as_mut().expect("session").state = QsoState::SendSig;
        controller.on_full_decode(
            rx_slot_start,
            &[directed_decode("K1ABC", "N1VF", ToUsEvent::Ack)],
            rx_slot_start + Duration::from_secs(15),
        );
        assert_eq!(controller.snapshot(now).state, "send_rr73");
        assert!(controller.maybe_arm_compound_handoff(
            rx_slot_start,
            CompoundHandoffPlan {
                next_station: StationStartInfo {
                    callsign: "K2ABC".to_string(),
                    last_heard_at: rx_slot_start,
                    last_heard_slot_family: SlotFamily::Odd,
                    last_snr_db: -7,
                    last_text: Some("N1VF K2ABC FN20".to_string()),
                    last_structured_json: Some("{\"kind\":\"grid\"}".to_string()),
                },
            },
            true,
            rx_slot_start + Duration::from_secs(15),
        ));
        controller.tick(SystemTime::UNIX_EPOCH + Duration::from_secs(60));
        controller.tick(SystemTime::UNIX_EPOCH + Duration::from_secs(60));
        let snapshot = controller.snapshot(SystemTime::UNIX_EPOCH + Duration::from_secs(60));
        assert!(snapshot.active);
        assert_eq!(snapshot.partner_call.as_deref(), Some("K2ABC"));
        assert_eq!(snapshot.state, "send_sig");
        assert_eq!(snapshot.tx_slot_family.as_deref(), Some("even"));
        assert_eq!(snapshot.selected_tx_freq_hz, Some(1225.0));
        assert!(
            snapshot
                .transcript
                .iter()
                .any(|entry| entry.text.contains("opening report already sent"))
        );
        let outcomes = controller.drain_outcomes();
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].partner_call, "K1ABC");
        assert!(outcomes[0].sent_terminal_73);
    }

    #[test]
    fn compound_73_once_handoff_reuses_73_slot_and_starts_follow_on_qso() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let rx_slot_start = now + Duration::from_secs(15);
        controller.handle_command(
            start_command("K1ABC", 1225.0),
            Some(station_start_info("K1ABC", now, SlotFamily::Odd)),
            now,
        );
        controller.session.as_mut().expect("session").state = QsoState::SendSigAck;
        controller.on_full_decode(
            rx_slot_start,
            &[directed_decode(
                "K1ABC",
                "N1VF",
                ToUsEvent::Reply(ReplyWord::Rr73),
            )],
            rx_slot_start + Duration::from_secs(15),
        );
        assert_eq!(controller.snapshot(now).state, "send_73_once");
        assert!(controller.maybe_arm_compound_handoff(
            rx_slot_start,
            CompoundHandoffPlan {
                next_station: StationStartInfo {
                    callsign: "K2ABC".to_string(),
                    last_heard_at: rx_slot_start,
                    last_heard_slot_family: SlotFamily::Odd,
                    last_snr_db: -7,
                    last_text: Some("N1VF K2ABC FN20".to_string()),
                    last_structured_json: Some("{\"kind\":\"grid\"}".to_string()),
                },
            },
            true,
            rx_slot_start + Duration::from_secs(15),
        ));
        controller.tick(SystemTime::UNIX_EPOCH + Duration::from_secs(60));
        controller.tick(SystemTime::UNIX_EPOCH + Duration::from_secs(60));
        let snapshot = controller.snapshot(SystemTime::UNIX_EPOCH + Duration::from_secs(60));
        assert!(snapshot.active);
        assert_eq!(snapshot.partner_call.as_deref(), Some("K2ABC"));
        assert_eq!(snapshot.state, "send_sig");
        assert!(
            snapshot
                .transcript
                .iter()
                .any(|entry| entry.text.contains("opening report already sent"))
        );
        let outcomes = controller.drain_outcomes();
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].partner_call, "K1ABC");
        assert!(outcomes[0].sent_terminal_73);
    }

    #[test]
    fn reserved_compound_handoff_can_refresh_next_station_report() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let rx_slot_start = now + Duration::from_secs(15);
        controller.handle_command(
            start_command("K1ABC", 1225.0),
            Some(station_start_info("K1ABC", now, SlotFamily::Odd)),
            now,
        );
        controller.session.as_mut().expect("session").state = QsoState::SendSig;
        controller.on_full_decode(
            rx_slot_start,
            &[directed_decode("K1ABC", "N1VF", ToUsEvent::Ack)],
            rx_slot_start + Duration::from_secs(15),
        );
        assert!(controller.maybe_arm_compound_handoff(
            rx_slot_start,
            CompoundHandoffPlan {
                next_station: StationStartInfo {
                    callsign: "K2ABC".to_string(),
                    last_heard_at: rx_slot_start,
                    last_heard_slot_family: SlotFamily::Odd,
                    last_snr_db: -7,
                    last_text: Some("N1VF K2ABC FN20".to_string()),
                    last_structured_json: Some("{\"kind\":\"grid\"}".to_string()),
                },
            },
            true,
            rx_slot_start + Duration::from_secs(15),
        ));
        assert!(controller.refresh_reserved_compound_next_station(
            StationStartInfo {
                callsign: "K2ABC".to_string(),
                last_heard_at: rx_slot_start,
                last_heard_slot_family: SlotFamily::Odd,
                last_snr_db: -3,
                last_text: Some("N1VF K2ABC FN20".to_string()),
                last_structured_json: Some("{\"kind\":\"grid\"}".to_string()),
            },
            rx_slot_start + Duration::from_secs(16),
        ));
        controller.tick(SystemTime::UNIX_EPOCH + Duration::from_secs(60));
        if let Some(session) = controller.session.as_ref() {
            if let Some(tx) = session.in_flight_tx.as_ref() {
                assert_eq!(tx.message_text, "K1ABC RR73; K2ABC <N1VF> -03");
            }
        }
    }

    #[test]
    fn early_partner_decode_is_consumed_before_full() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let rx_slot_start = now + Duration::from_secs(15);
        controller.handle_command(
            start_command("K1ABC", 1000.0),
            Some(StationStartInfo {
                callsign: "K1ABC".to_string(),
                last_heard_at: now,
                last_heard_slot_family: SlotFamily::Odd,
                last_snr_db: -9,
                last_text: None,
                last_structured_json: None,
            }),
            now,
        );
        controller.on_decode_stage(
            rx_slot_start,
            DecodeStage::Early41,
            &[directed_decode("K1ABC", "N1VF", ToUsEvent::ReportLike)],
            rx_slot_start + Duration::from_secs(11),
        );
        assert_eq!(controller.snapshot(now).state, "send_sig_ack");
        controller.on_decode_stage(
            rx_slot_start,
            DecodeStage::Full,
            &[directed_decode(
                "K1ABC",
                "N1VF",
                ToUsEvent::Reply(ReplyWord::Rrr),
            )],
            rx_slot_start + Duration::from_secs(15),
        );
        let snapshot = controller.snapshot(now);
        assert_eq!(snapshot.state, "send_sig_ack");
        assert_eq!(snapshot.no_msg_count, 0);
        assert_eq!(snapshot.no_fwd_count, 0);
    }

    #[test]
    fn full_stage_waits_until_both_early_stages_miss_partner() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let rx_slot_start = now + Duration::from_secs(15);
        controller.handle_command(
            start_command("K1ABC", 1000.0),
            Some(StationStartInfo {
                callsign: "K1ABC".to_string(),
                last_heard_at: now,
                last_heard_slot_family: SlotFamily::Odd,
                last_snr_db: -9,
                last_text: None,
                last_structured_json: None,
            }),
            now,
        );
        controller.on_decode_stage(
            rx_slot_start,
            DecodeStage::Early41,
            &[cq_decode("ZZ9")],
            rx_slot_start + Duration::from_secs(11),
        );
        let snapshot = controller.snapshot(now);
        assert_eq!(snapshot.state, "send_grid");
        assert_eq!(snapshot.no_msg_count, 0);
        controller.on_decode_stage(
            rx_slot_start,
            DecodeStage::Early47,
            &[cq_decode("ZZ9")],
            rx_slot_start + Duration::from_secs(12),
        );
        let snapshot = controller.snapshot(now);
        assert_eq!(snapshot.state, "send_grid");
        assert_eq!(snapshot.no_msg_count, 0);
        controller.on_decode_stage(
            rx_slot_start,
            DecodeStage::Full,
            &[],
            rx_slot_start + Duration::from_secs(15),
        );
        let snapshot = controller.snapshot(now);
        assert_eq!(snapshot.state, "send_grid");
        assert_eq!(snapshot.no_msg_count, 1);
    }

    #[test]
    fn send_sig_report_like_counts_as_no_fwd() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let rx_slot_start = now + Duration::from_secs(15);
        controller.handle_command(
            start_command("K1ABC", 1000.0),
            Some(StationStartInfo {
                callsign: "K1ABC".to_string(),
                last_heard_at: now,
                last_heard_slot_family: SlotFamily::Odd,
                last_snr_db: -9,
                last_text: None,
                last_structured_json: None,
            }),
            now,
        );
        controller.session.as_mut().expect("session").state = QsoState::SendSig;
        controller.on_decode_stage(
            rx_slot_start,
            DecodeStage::Early41,
            &[directed_decode("K1ABC", "N1VF", ToUsEvent::ReportLike)],
            rx_slot_start + Duration::from_secs(11),
        );
        let snapshot = controller.snapshot(now);
        assert_eq!(snapshot.state, "send_sig");
        assert_eq!(snapshot.no_msg_count, 0);
        assert_eq!(snapshot.no_fwd_count, 1);
    }

    #[test]
    fn send_sig_ack_report_like_counts_as_no_fwd() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let rx_slot_start = now + Duration::from_secs(15);
        controller.handle_command(
            start_command("K1ABC", 1000.0),
            Some(StationStartInfo {
                callsign: "K1ABC".to_string(),
                last_heard_at: now,
                last_heard_slot_family: SlotFamily::Odd,
                last_snr_db: -9,
                last_text: None,
                last_structured_json: None,
            }),
            now,
        );
        controller.session.as_mut().expect("session").state = QsoState::SendSigAck;
        controller.on_decode_stage(
            rx_slot_start,
            DecodeStage::Early41,
            &[directed_decode("K1ABC", "N1VF", ToUsEvent::ReportLike)],
            rx_slot_start + Duration::from_secs(11),
        );
        let snapshot = controller.snapshot(now);
        assert_eq!(snapshot.state, "send_sig_ack");
        assert_eq!(snapshot.no_msg_count, 0);
        assert_eq!(snapshot.no_fwd_count, 1);
    }

    #[test]
    fn send_rr73_exits_on_cq() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        controller.handle_command(
            start_command("K1ABC", 1000.0),
            Some(StationStartInfo {
                callsign: "K1ABC".to_string(),
                last_heard_at: now,
                last_heard_slot_family: SlotFamily::Odd,
                last_snr_db: -9,
                last_text: None,
                last_structured_json: None,
            }),
            now,
        );
        if let Some(session) = controller.session.as_mut() {
            session.state = QsoState::SendRR73;
        }
        controller.on_full_decode(
            now + Duration::from_secs(15),
            &[cq_decode("K1ABC")],
            now + Duration::from_secs(15),
        );
        assert!(!controller.snapshot(now).active);
    }

    #[test]
    fn send_73_exits_on_cq() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        controller.handle_command(
            start_command("K1ABC", 1000.0),
            Some(StationStartInfo {
                callsign: "K1ABC".to_string(),
                last_heard_at: now,
                last_heard_slot_family: SlotFamily::Odd,
                last_snr_db: -9,
                last_text: None,
                last_structured_json: None,
            }),
            now,
        );
        if let Some(session) = controller.session.as_mut() {
            session.state = QsoState::Send73;
        }
        controller.on_full_decode(
            now + Duration::from_secs(15),
            &[cq_decode("K1ABC")],
            now + Duration::from_secs(15),
        );
        assert!(!controller.snapshot(now).active);
    }

    #[test]
    fn send_73_exits_on_partner_noncall_first_field() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        controller.handle_command(
            start_command("K1ABC", 1000.0),
            Some(StationStartInfo {
                callsign: "K1ABC".to_string(),
                last_heard_at: now,
                last_heard_slot_family: SlotFamily::Odd,
                last_snr_db: -9,
                last_text: None,
                last_structured_json: None,
            }),
            now,
        );
        if let Some(session) = controller.session.as_mut() {
            session.state = QsoState::Send73;
        }
        controller.on_full_decode(
            now + Duration::from_secs(15),
            &[token_first_decode("QRZ", "K1ABC")],
            now + Duration::from_secs(15),
        );
        assert!(!controller.snapshot(now).active);
    }

    #[test]
    fn send_73_once_exits_after_tx_completes() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        controller.handle_command(
            start_command("K1ABC", 1000.0),
            Some(StationStartInfo {
                callsign: "K1ABC".to_string(),
                last_heard_at: now,
                last_heard_slot_family: SlotFamily::Even,
                last_snr_db: -9,
                last_text: None,
                last_structured_json: None,
            }),
            now,
        );
        if let Some(session) = controller.session.as_mut() {
            session.state = QsoState::Send73Once;
            session.next_tx_slot = Some(first_matching_slot_after(now, SlotFamily::Odd, Mode::Ft8));
        }
        controller.tick(now + Duration::from_secs(15));
        controller.tick(now + Duration::from_secs(15));
        assert!(!controller.snapshot(now).active);
    }

    #[test]
    fn next_tx_slot_skips_current_slot_when_already_transmitting() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let session = ActiveSession {
            session_id: 1,
            partner_call: "K1ABC".to_string(),
            state: QsoState::SendGrid,
            start_mode: QsoStartMode::Normal,
            tx_slot_family: SlotFamily::Even,
            tx_freq_hz: 1000.0,
            latest_partner_snr_db: -9,
            rig_frequency_hz: None,
            rig_band: None,
            app_mode: Mode::Ft8,
            started_at: now,
            deadline_at: now + Duration::from_secs(600),
            next_tx_slot: None,
            last_tx_slot: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(30)),
            in_flight_tx: None,
            pending_action: None,
            compound_rr73_ready_slot: None,
            pending_compound_handoff: None,
            no_msg_count: 0,
            no_fwd_count: 0,
            partner_rx_count: 0,
            last_rx_event: None,
            last_rx_stage: None,
            last_rx_text: None,
            last_rx_structured_json: None,
            current_rx_slot: None,
            rx_slot_consumed_stage: None,
            transcript: VecDeque::new(),
            sent_terminal_73: false,
        };
        let rescheduled =
            schedule_next_tx_slot(&session, SystemTime::UNIX_EPOCH + Duration::from_secs(15))
                .expect("rescheduled");
        assert_eq!(
            rescheduled
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            60
        );
    }

    #[test]
    fn late_transition_to_send_73_once_waits_for_actual_73_tx() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        controller.handle_command(
            start_command("K1ABC", 1000.0),
            Some(StationStartInfo {
                callsign: "K1ABC".to_string(),
                last_heard_at: now,
                last_heard_slot_family: SlotFamily::Odd,
                last_snr_db: -9,
                last_text: None,
                last_structured_json: None,
            }),
            now,
        );
        if let Some(session) = controller.session.as_mut() {
            session.state = QsoState::SendRRR;
            session.next_tx_slot = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(30));
        }
        controller.tick(SystemTime::UNIX_EPOCH + Duration::from_secs(30));
        controller.on_full_decode(
            SystemTime::UNIX_EPOCH + Duration::from_secs(15),
            &[directed_decode("K1ABC", "ZZ9", ToUsEvent::Other)],
            SystemTime::UNIX_EPOCH + Duration::from_secs(30),
        );
        assert_eq!(
            controller
                .snapshot(SystemTime::UNIX_EPOCH + Duration::from_secs(30))
                .state,
            "send_rrr"
        );
        controller.tick(SystemTime::UNIX_EPOCH + Duration::from_secs(45));
        let snapshot = controller.snapshot(SystemTime::UNIX_EPOCH + Duration::from_secs(45));
        assert!(snapshot.active);
        assert_eq!(snapshot.state, "send_rrr");
        controller.tick(SystemTime::UNIX_EPOCH + Duration::from_secs(60));
        let snapshot = controller.snapshot(SystemTime::UNIX_EPOCH + Duration::from_secs(60));
        assert!(snapshot.active);
        assert_eq!(snapshot.state, "send_73_once");
        controller.tick(SystemTime::UNIX_EPOCH + Duration::from_secs(60));
        assert!(
            !controller
                .snapshot(SystemTime::UNIX_EPOCH + Duration::from_secs(60))
                .active
        );
    }

    #[test]
    fn fresh_rx_supersedes_queued_state_before_next_tx() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        controller.handle_command(
            start_command("K1ABC", 1000.0),
            Some(StationStartInfo {
                callsign: "K1ABC".to_string(),
                last_heard_at: now,
                last_heard_slot_family: SlotFamily::Odd,
                last_snr_db: -9,
                last_text: None,
                last_structured_json: None,
            }),
            now,
        );
        if let Some(session) = controller.session.as_mut() {
            session.state = QsoState::SendRRR;
            session.next_tx_slot = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(30));
        }
        controller.tick(SystemTime::UNIX_EPOCH + Duration::from_secs(30));
        controller.on_full_decode(
            SystemTime::UNIX_EPOCH + Duration::from_secs(15),
            &[directed_decode("K1ABC", "ZZ9", ToUsEvent::Other)],
            SystemTime::UNIX_EPOCH + Duration::from_secs(30),
        );
        controller.tick(SystemTime::UNIX_EPOCH + Duration::from_secs(45));
        controller.on_full_decode(
            SystemTime::UNIX_EPOCH + Duration::from_secs(45),
            &[directed_decode(
                "K1ABC",
                "N1VF",
                ToUsEvent::Reply(ReplyWord::SeventyThree),
            )],
            SystemTime::UNIX_EPOCH + Duration::from_secs(59),
        );
        controller.tick(SystemTime::UNIX_EPOCH + Duration::from_secs(60));
        let snapshot = controller.snapshot(SystemTime::UNIX_EPOCH + Duration::from_secs(60));
        assert!(snapshot.active);
        assert_eq!(snapshot.state, "send_73");
        assert!(
            snapshot
                .transcript
                .iter()
                .any(|entry| entry.text.contains("fresh RX superseded queued transition"))
        );
    }

    #[test]
    fn send_sig_can_preempt_after_tx_and_empty_rx_when_partner_not_engaged() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        controller.handle_command(
            QsoCommand::Start {
                partner_call: "K1ABC".to_string(),
                tx_freq_hz: 1225.0,
                initial_state: QsoState::SendSig,
                start_mode: QsoStartMode::Direct,
                tx_slot_family_override: Some(SlotFamily::Even),
            },
            Some(station_start_info("K1ABC", now, SlotFamily::Odd)),
            now,
        );
        let session = controller.session.as_mut().expect("session");
        session.last_tx_slot = Some(now);
        session.no_msg_count = 1;
        session.partner_rx_count = 0;

        assert!(controller.preempt_for_priority_direct(now + Duration::from_secs(1)));
        let outcomes = controller.drain_outcomes();
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].exit_reason, "send_sig_no_msg_limit");
    }

    #[test]
    fn send_sig_does_not_preempt_before_first_empty_rx_cycle() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        controller.handle_command(
            QsoCommand::Start {
                partner_call: "K1ABC".to_string(),
                tx_freq_hz: 1225.0,
                initial_state: QsoState::SendSig,
                start_mode: QsoStartMode::Direct,
                tx_slot_family_override: Some(SlotFamily::Even),
            },
            Some(station_start_info("K1ABC", now, SlotFamily::Odd)),
            now,
        );
        let session = controller.session.as_mut().expect("session");
        session.last_tx_slot = Some(now);
        session.no_msg_count = 0;
        session.partner_rx_count = 0;

        assert!(!controller.preempt_for_priority_direct(now + Duration::from_secs(1)));
    }

    #[test]
    fn late_decode_exit_waits_for_committed_tx_completion() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        controller.handle_command(
            start_command("K1ABC", 1000.0),
            Some(StationStartInfo {
                callsign: "K1ABC".to_string(),
                last_heard_at: now,
                last_heard_slot_family: SlotFamily::Odd,
                last_snr_db: -9,
                last_text: None,
                last_structured_json: None,
            }),
            now,
        );
        if let Some(session) = controller.session.as_mut() {
            session.state = QsoState::SendRR73;
        }
        controller.tick(SystemTime::UNIX_EPOCH + Duration::from_secs(60));
        assert!(controller.snapshot(now).active);
        controller.on_full_decode(
            SystemTime::UNIX_EPOCH + Duration::from_secs(45),
            &[cq_decode("K1ABC")],
            SystemTime::UNIX_EPOCH + Duration::from_secs(60),
        );
        assert!(controller.snapshot(now).active);
        controller.tick(SystemTime::UNIX_EPOCH + Duration::from_secs(60));
        let snapshot = controller.snapshot(now);
        assert!(snapshot.active);
        controller.tick(SystemTime::UNIX_EPOCH + Duration::from_secs(90));
        let snapshot = controller.snapshot(now);
        assert!(!snapshot.active);
        assert!(
            snapshot
                .transcript
                .iter()
                .any(|entry| entry.text.contains("late RX after tx launch: exit queued"))
        );
    }
}
