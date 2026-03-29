mod crc;
mod decoder;
mod encode;
mod ldpc;
mod message;
mod protocol;
mod wave;

pub use decoder::{
    CandidateDebugReport, CandidatePassDebug, DecodeCandidate, DecodeDiagnostics, DecodeOptions,
    DecodeProfile, DecodeReport, DecodeStage, DecoderSession, DecoderState, DecodedMessage,
    StageDecodeReport, debug_candidate_pcm, debug_candidate_truth_wav_file,
    debug_candidate_wav_file, decode_pcm, decode_pcm_with_state, decode_wav_file,
    decode_wav_file_with_state,
};
pub use encode::{
    EncodeError, EncodedFrame, WaveformOptions, channel_symbols_from_codeword_bits,
    encode_nonstandard_message, encode_standard_message, parse_standard_info, synthesize_channel_reference,
    synthesize_rectangular_waveform, write_rectangular_standard_wav,
};
pub use message::{DecodedPayload, GridReport, MessageKind, ReplyWord};
pub use wave::{AudioBuffer, DecoderError, write_wav};
