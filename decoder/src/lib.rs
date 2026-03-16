mod crc;
mod decoder;
mod encode;
mod ldpc;
mod message;
mod protocol;
mod wave;

pub use decoder::{
    DecodeCandidate, DecodeDiagnostics, DecodeOptions, DecodeReport, DecodedMessage, decode_pcm,
    decode_wav_file,
};
pub use encode::{
    EncodeError, EncodedFrame, WaveformOptions, encode_standard_message, parse_standard_info,
    synthesize_rectangular_waveform, write_rectangular_standard_wav,
};
pub use message::{DecodedPayload, GridReport, MessageKind, ReplyWord};
pub use wave::{AudioBuffer, DecoderError, write_wav};
