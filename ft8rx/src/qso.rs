use crate::config::AppConfig;
use chrono::{DateTime, Utc};
use ft8_decoder::{
    ReplyWord, StructuredInfoValue, StructuredMessage, TxDirectedPayload, TxMessage,
    WaveformOptions, synthesize_tx_message,
};
use rigctl::K3s;
use rigctl::audio::AudioDevice;
use serde::Serialize;
use std::collections::VecDeque;
use std::io::Write as _;
use std::process::{Command, Stdio};
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
}

#[derive(Debug, Clone)]
pub enum QsoCommand {
    Start {
        partner_call: String,
        tx_freq_hz: f32,
    },
    Stop {
        reason: String,
    },
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
            } => self.handle_start(partner_call, tx_freq_hz, station_info, now),
            QsoCommand::Stop { reason } => self.stop_session(&reason, now),
        }
    }

    pub fn on_full_decode(
        &mut self,
        slot_start: SystemTime,
        decodes: &[ft8_decoder::DecodedMessage],
        now: SystemTime,
    ) {
        let Some(session) = &mut self.session else {
            return;
        };
        if slot_family(slot_start) == session.tx_slot_family {
            return;
        }

        let event = classify_partner_event(
            decodes,
            &session.partner_call,
            &self.config.station.our_call,
            session.state,
        );
        if let Some(snr_db) = event.snr_db() {
            session.latest_partner_snr_db = snr_db;
        }
        session.last_rx_event = Some(event.summary());
        Self::push_transcript(session, now, "rx", event.transcript_text());
        Self::log_fsm(
            session,
            "rx_slot",
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
                    event: ToUsEvent::Reply(_),
                    ..
                } => next_state = QsoState::Send73,
                PartnerEvent::ToUs {
                    event: ToUsEvent::Other,
                    ..
                } => {
                    session.no_fwd_count += 1;
                    if session.no_fwd_count >= self.config.fsm.send_grid.no_fwd {
                        next_state = QsoState::Send73Once;
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
                    event: ToUsEvent::Ack,
                    ..
                }
                | PartnerEvent::ToUs {
                    event: ToUsEvent::Reply(ReplyWord::Rrr | ReplyWord::Rr73),
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
                } => {
                    session.no_fwd_count += 1;
                    if session.no_fwd_count >= self.config.fsm.send_sig.no_fwd {
                        next_state = QsoState::Send73Once;
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
                | PartnerEvent::Freeform { .. }
                | PartnerEvent::None => exit_reason = Some("send_rr73_partner_moved_on"),
            },
            QsoState::SendRRR => match event {
                PartnerEvent::ToUs {
                    event: ToUsEvent::Reply(ReplyWord::SeventyThree),
                    ..
                } => next_state = QsoState::Send73,
                PartnerEvent::ToUs { .. } => {
                    session.no_fwd_count += 1;
                    if session.no_fwd_count >= self.config.fsm.send_rrr.no_fwd {
                        next_state = QsoState::Send73Once;
                    }
                }
                PartnerEvent::ToOther { .. } => next_state = QsoState::Send73Once,
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
            self.finish_session(reason, now);
            return;
        }

        if let Some(session) = &mut self.session {
            if next_state != previous_state {
                session.state = next_state;
                session.no_fwd_count = 0;
                session.no_msg_count = 0;
                Self::push_transcript(
                    session,
                    now,
                    "sys",
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
            } else {
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
            session.next_tx_slot = next_matching_slot_after(slot_start, session.tx_slot_family);
        }
    }

    pub fn tick(&mut self, now: SystemTime) {
        while let Some(event) = self.backend.poll_event() {
            let mut exit_after = None;
            if let Some(session) = &mut self.session {
                if session.session_id == event.session_id() {
                    match &event {
                        TxEvent::Started { message_text, .. } => {
                            Self::push_transcript(session, now, "tx", message_text.clone());
                        }
                        TxEvent::Completed { message_text, .. } => {
                            Self::push_transcript(
                                session,
                                now,
                                "sys",
                                format!("tx complete: {message_text}"),
                            );
                            if session.state == QsoState::Send73Once {
                                exit_after = Some("send_73_once_complete");
                            }
                        }
                        TxEvent::Aborted { reason, .. } => {
                            Self::push_transcript(
                                session,
                                now,
                                "sys",
                                format!("tx aborted: {reason}"),
                            );
                            exit_after = Some("tx_aborted");
                        }
                        TxEvent::Error { message, .. } => {
                            Self::push_transcript(
                                session,
                                now,
                                "sys",
                                format!("tx error: {message}"),
                            );
                            exit_after = Some("tx_error");
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
            if let Some(reason) = exit_after {
                self.finish_session(reason, now);
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

        let request = build_tx_request(&self.config, session, target_slot);
        let message_text = request.message_text.clone();
        match self.backend.start(request) {
            Ok(()) => {
                session.last_tx_slot = Some(target_slot);
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
                Self::push_transcript(session, now, "sys", format!("tx launch failed: {error}"));
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

    fn handle_start(
        &mut self,
        partner_call: String,
        tx_freq_hz: f32,
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
        let Some(station_info) = station_info else {
            warn!(
                partner_call,
                "qso start rejected because station info is unavailable"
            );
            return;
        };
        let tx_slot_family = station_info.last_heard_slot_family.opposite();
        let next_tx_slot = first_matching_slot_after(now, tx_slot_family);
        let mut session = ActiveSession {
            session_id: self.next_session_id,
            partner_call: station_info.callsign,
            state: QsoState::SendGrid,
            tx_slot_family,
            tx_freq_hz,
            latest_partner_snr_db: station_info.last_snr_db,
            started_at: now,
            deadline_at: now + Duration::from_secs(self.config.fsm.timeout_seconds),
            next_tx_slot: Some(next_tx_slot),
            last_tx_slot: None,
            no_msg_count: 0,
            no_fwd_count: 0,
            last_rx_event: None,
            transcript: VecDeque::new(),
        };
        self.next_session_id += 1;
        let start_line = format!(
            "start qso with {} tx={} freq={:.0}Hz latest_snr={:+} last_heard={}",
            session.partner_call,
            session.tx_slot_family.as_str(),
            session.tx_freq_hz,
            session.latest_partner_snr_db,
            format_timestamp(station_info.last_heard_at),
        );
        Self::push_transcript(&mut session, now, "sys", start_line);
        Self::log_fsm(
            &session,
            "start",
            QsoState::Idle,
            session.state,
            "start".to_string(),
            None,
            None,
            now,
        );
        self.session = Some(session);
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
        Self::push_transcript(&mut session, now, "sys", format!("qso exit: {reason}"));
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
    }

    fn push_transcript(
        session: &mut ActiveSession,
        now: SystemTime,
        direction: &str,
        text: String,
    ) {
        session.transcript.push_back(WebQsoTranscriptEntry {
            timestamp: format_timestamp(now),
            direction: direction.to_string(),
            state: session.state.as_str().to_string(),
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
            last_rx_event = %rx_summary,
            rx_text = rx_text.unwrap_or_default(),
            tx_text = tx_text.unwrap_or_default(),
            started_at = %format_timestamp(session.started_at),
            deadline_at = %format_timestamp(session.deadline_at),
            "qso_fsm"
        );
    }
}

#[derive(Debug, Clone)]
struct ActiveSession {
    session_id: u64,
    partner_call: String,
    state: QsoState,
    tx_slot_family: SlotFamily,
    tx_freq_hz: f32,
    latest_partner_snr_db: i32,
    started_at: SystemTime,
    deadline_at: SystemTime,
    next_tx_slot: Option<SystemTime>,
    last_tx_slot: Option<SystemTime>,
    no_msg_count: u32,
    no_fwd_count: u32,
    last_rx_event: Option<String>,
    transcript: VecDeque<WebQsoTranscriptEntry>,
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
}

#[derive(Debug, Clone)]
pub(crate) enum TxEvent {
    Started {
        session_id: u64,
        message_text: String,
    },
    Completed {
        session_id: u64,
        message_text: String,
    },
    Aborted {
        session_id: u64,
        message_text: String,
        reason: String,
    },
    Error {
        session_id: u64,
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
    playback_channels: usize,
    active: bool,
    cancel: Option<Arc<AtomicBool>>,
    event_rx: Option<mpsc::Receiver<TxEvent>>,
}

impl RigTxBackend {
    pub fn new(
        rig: Arc<Mutex<Option<K3s>>>,
        output_device: AudioDevice,
        playback_channels: usize,
    ) -> Self {
        Self {
            rig,
            output_device,
            playback_channels,
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
        let synthesized = synthesize_tx_message(
            &request.message,
            &WaveformOptions {
                base_freq_hz: request.tx_freq_hz,
                start_seconds: 0.5,
                total_seconds: 15.0,
                amplitude: request.drive_level,
            },
        )
        .map_err(|error| error.to_string())?;
        let bytes = encode_pcm_bytes(&synthesized.audio.samples, self.playback_channels);
        let (event_tx, event_rx) = mpsc::channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let rig = Arc::clone(&self.rig);
        let cancel_thread = Arc::clone(&cancel);
        let output_device = self.output_device.clone();
        thread::spawn(move || {
            run_tx_thread(rig, output_device, request, bytes, cancel_thread, event_tx);
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
        snr_db: i32,
    },
    ToOther {
        text: String,
        snr_db: i32,
    },
    Cq {
        text: String,
        snr_db: i32,
    },
    Freeform {
        text: String,
    },
}

impl PartnerEvent {
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
            Self::Freeform { .. } => "freeform".to_string(),
        }
    }

    fn transcript_text(&self) -> String {
        match self {
            Self::None => "no partner message".to_string(),
            Self::ToUs { text, .. }
            | Self::ToOther { text, .. }
            | Self::Cq { text, .. }
            | Self::Freeform { text } => format!("RX {text}"),
        }
    }

    fn message_text(&self) -> Option<String> {
        match self {
            Self::None => None,
            Self::ToUs { text, .. }
            | Self::ToOther { text, .. }
            | Self::Cq { text, .. }
            | Self::Freeform { text } => Some(text.clone()),
        }
    }

    fn snr_db(&self) -> Option<i32> {
        match self {
            Self::ToUs { snr_db, .. } | Self::ToOther { snr_db, .. } | Self::Cq { snr_db, .. } => {
                Some(*snr_db)
            }
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
    let message = TxMessage::Directed {
        my_call: config.station.our_call.clone(),
        peer_call: session.partner_call.clone(),
        payload,
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
    }
}

fn render_tx_message(config: &AppConfig, session: &ActiveSession) -> String {
    match session.state {
        QsoState::Idle => String::new(),
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
                    snr_db: decode.snr_db,
                });
            }
            SingleClass::Cq => {
                cq.get_or_insert(PartnerEvent::Cq {
                    text: decode.text.clone(),
                    snr_db: decode.snr_db,
                });
            }
            SingleClass::Freeform => {
                freeform.get_or_insert(PartnerEvent::Freeform {
                    text: decode.text.clone(),
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
            let target = structured_call_station_name(first);
            if target.as_deref() == Some(our_call) {
                match &info.value {
                    StructuredInfoValue::Reply { word } => {
                        SingleClass::ToUs(ToUsEvent::Reply(*word))
                    }
                    StructuredInfoValue::Grid { .. }
                    | StructuredInfoValue::SignalReport { .. } => {
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
        StructuredMessage::FreeText { .. } | StructuredMessage::Unsupported { .. } => {
            SingleClass::Freeform
        }
    }
}

fn to_us_priority(event: ToUsEvent, state: QsoState) -> u8 {
    match state {
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
    bytes: Vec<u8>,
    cancel: Arc<AtomicBool>,
    event_tx: mpsc::Sender<TxEvent>,
) {
    if wait_until(
        request
            .target_slot
            .checked_sub(Duration::from_millis(PRE_KEY_MS))
            .unwrap_or(request.target_slot),
        &cancel,
    ) {
        let _ = event_tx.send(TxEvent::Aborted {
            session_id: request.session_id,
            message_text: request.message_text,
            reason: "cancelled_before_key".to_string(),
        });
        return;
    }

    if let Err(error) = with_rig(&rig, |rig| rig.enter_tx()) {
        let _ = event_tx.send(TxEvent::Error {
            session_id: request.session_id,
            message_text: request.message_text,
            message: format!("enter_tx failed: {error}"),
        });
        return;
    }

    if wait_until(request.target_slot, &cancel) {
        force_rx(&rig);
        let _ = event_tx.send(TxEvent::Aborted {
            session_id: request.session_id,
            message_text: request.message_text,
            reason: "cancelled_before_audio".to_string(),
        });
        return;
    }

    let _ = event_tx.send(TxEvent::Started {
        session_id: request.session_id,
        message_text: request.message_text.clone(),
    });

    match spawn_aplay(&output_device, request.playback_channels, &bytes) {
        Ok(mut child) => loop {
            if cancel.load(Ordering::Relaxed) {
                let _ = child.kill();
                let _ = child.wait();
                force_rx(&rig);
                let _ = event_tx.send(TxEvent::Aborted {
                    session_id: request.session_id,
                    message_text: request.message_text,
                    reason: format!("cancelled_in_{}", request.state.as_str()),
                });
                return;
            }
            match child.try_wait() {
                Ok(Some(status)) => {
                    force_rx(&rig);
                    if status.success() {
                        let _ = event_tx.send(TxEvent::Completed {
                            session_id: request.session_id,
                            message_text: request.message_text,
                        });
                    } else {
                        let _ = event_tx.send(TxEvent::Error {
                            session_id: request.session_id,
                            message_text: request.message_text,
                            message: format!("aplay exited with status {status}"),
                        });
                    }
                    return;
                }
                Ok(None) => thread::sleep(Duration::from_millis(50)),
                Err(error) => {
                    force_rx(&rig);
                    let _ = event_tx.send(TxEvent::Error {
                        session_id: request.session_id,
                        message_text: request.message_text,
                        message: format!("aplay wait failed: {error}"),
                    });
                    return;
                }
            }
        },
        Err(error) => {
            force_rx(&rig);
            let _ = event_tx.send(TxEvent::Error {
                session_id: request.session_id,
                message_text: request.message_text,
                message: error,
            });
        }
    }
}

fn spawn_aplay(
    device: &AudioDevice,
    channels: usize,
    bytes: &[u8],
) -> Result<std::process::Child, String> {
    let mut child = Command::new("aplay")
        .arg("-D")
        .arg(&device.spec)
        .arg("-q")
        .arg("-t")
        .arg("raw")
        .arg("-f")
        .arg("S16_LE")
        .arg("-r")
        .arg("12000")
        .arg("-c")
        .arg(channels.max(1).to_string())
        .stdin(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| format!("spawn aplay failed: {error}"))?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| "aplay stdin unavailable".to_string())?;
    stdin
        .write_all(bytes)
        .map_err(|error| format!("write audio failed: {error}"))?;
    drop(stdin);
    Ok(child)
}

fn encode_pcm_bytes(samples: &[f32], channels: usize) -> Vec<u8> {
    let channel_count = channels.max(1);
    let mut bytes = Vec::with_capacity(samples.len() * channel_count * std::mem::size_of::<i16>());
    for &sample in samples {
        let clamped = sample.clamp(-1.0, 1.0);
        let pcm = (clamped * i16::MAX as f32).round() as i16;
        for _ in 0..channel_count {
            bytes.extend_from_slice(&pcm.to_le_bytes());
        }
    }
    bytes
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

fn first_matching_slot_after(now: SystemTime, family: SlotFamily) -> SystemTime {
    let mut slot = crate::next_slot_boundary(now);
    let key_time = slot
        .checked_sub(Duration::from_millis(PRE_KEY_MS))
        .unwrap_or(slot);
    if now >= key_time {
        slot += Duration::from_secs(crate::SLOT_SECONDS);
    }
    while slot_family(slot) != family {
        slot += Duration::from_secs(crate::SLOT_SECONDS);
    }
    slot
}

fn next_matching_slot_after(slot_start: SystemTime, family: SlotFamily) -> Option<SystemTime> {
    let mut slot = slot_start + Duration::from_secs(crate::SLOT_SECONDS);
    while slot_family(slot) != family {
        slot += Duration::from_secs(crate::SLOT_SECONDS);
    }
    Some(slot)
}

pub fn slot_family(time: SystemTime) -> SlotFamily {
    if crate::is_even_slot_family(time) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        FsmConfig, LoggingConfig, NoFwdThreshold, RetryThresholds, StationConfig, TxConfig,
    };
    use ft8_decoder::{
        DecodedMessage, StructuredCallField, StructuredCallValue, StructuredInfoField,
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
                message_text: request.message_text.clone(),
            });
            self.events.push_back(TxEvent::Completed {
                session_id: request.session_id,
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
            fsm: FsmConfig {
                rr73_enabled: true,
                timeout_seconds: 600,
                send_grid: RetryThresholds {
                    no_fwd: 3,
                    no_msg: 10,
                },
                send_sig: RetryThresholds {
                    no_fwd: 3,
                    no_msg: 10,
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

    #[test]
    fn start_infers_opposite_slot_family() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        controller.handle_command(
            QsoCommand::Start {
                partner_call: "K1ABC".to_string(),
                tx_freq_hz: 1000.0,
            },
            Some(StationStartInfo {
                callsign: "K1ABC".to_string(),
                last_heard_at: now,
                last_heard_slot_family: SlotFamily::Even,
                last_snr_db: -9,
            }),
            now,
        );
        assert_eq!(
            controller.snapshot(now).tx_slot_family.as_deref(),
            Some("odd")
        );
    }

    #[test]
    fn send_grid_ack_transitions_to_sig_ack() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        controller.handle_command(
            QsoCommand::Start {
                partner_call: "K1ABC".to_string(),
                tx_freq_hz: 1000.0,
            },
            Some(StationStartInfo {
                callsign: "K1ABC".to_string(),
                last_heard_at: now,
                last_heard_slot_family: SlotFamily::Odd,
                last_snr_db: -9,
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
            QsoCommand::Start {
                partner_call: "K1ABC".to_string(),
                tx_freq_hz: 1000.0,
            },
            Some(StationStartInfo {
                callsign: "K1ABC".to_string(),
                last_heard_at: now,
                last_heard_slot_family: SlotFamily::Odd,
                last_snr_db: -9,
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
    fn send_rr73_exits_on_cq() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        controller.handle_command(
            QsoCommand::Start {
                partner_call: "K1ABC".to_string(),
                tx_freq_hz: 1000.0,
            },
            Some(StationStartInfo {
                callsign: "K1ABC".to_string(),
                last_heard_at: now,
                last_heard_slot_family: SlotFamily::Odd,
                last_snr_db: -9,
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
    fn send_73_once_exits_after_tx_completes() {
        let mut controller =
            QsoController::new(sample_config(), Box::new(MockTxBackend::default()));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        controller.handle_command(
            QsoCommand::Start {
                partner_call: "K1ABC".to_string(),
                tx_freq_hz: 1000.0,
            },
            Some(StationStartInfo {
                callsign: "K1ABC".to_string(),
                last_heard_at: now,
                last_heard_slot_family: SlotFamily::Even,
                last_snr_db: -9,
            }),
            now,
        );
        if let Some(session) = controller.session.as_mut() {
            session.state = QsoState::Send73Once;
            session.next_tx_slot = Some(first_matching_slot_after(now, SlotFamily::Odd));
        }
        controller.tick(now + Duration::from_secs(15));
        controller.tick(now + Duration::from_secs(15));
        assert!(!controller.snapshot(now).active);
    }
}
