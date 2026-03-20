mod crc;
mod decoder;
mod encode;
mod ldpc;
mod message;
mod protocol;
mod wave;

pub use decoder::{
    CandidateDebugReport, CandidatePassDebug, DecodeCandidate, DecodeDiagnostics, DecodeOptions,
    DecodeProfile, DecodeReport, DecodedMessage, debug_candidate_pcm, debug_candidate_wav_file,
    decode_pcm, decode_wav_file,
};
pub use encode::{
    EncodeError, EncodedFrame, WaveformOptions, channel_symbols_from_codeword_bits,
    encode_standard_message, parse_standard_info, synthesize_channel_reference,
    synthesize_rectangular_waveform, write_rectangular_standard_wav,
};
pub use message::{DecodedPayload, GridReport, MessageKind, ReplyWord};
pub use wave::{AudioBuffer, DecoderError, write_wav};
