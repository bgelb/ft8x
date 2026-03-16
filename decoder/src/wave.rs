use std::path::Path;

use thiserror::Error;

#[derive(Debug, Clone)]
pub struct AudioBuffer {
    pub sample_rate_hz: u32,
    pub samples: Vec<f32>,
}

#[derive(Debug, Error)]
pub enum DecoderError {
    #[error("unsupported wav format: {0}")]
    UnsupportedFormat(String),
    #[error("wav io error: {0}")]
    Wav(#[from] hound::Error),
}

pub fn load_wav(path: impl AsRef<Path>) -> Result<AudioBuffer, DecoderError> {
    let mut reader = hound::WavReader::open(path)?;
    let spec = reader.spec();
    let channels = spec.channels as usize;
    if channels == 0 {
        return Err(DecoderError::UnsupportedFormat("zero channels".to_string()));
    }

    let interleaved = match (spec.sample_format, spec.bits_per_sample) {
        (hound::SampleFormat::Int, 16) => reader
            .samples::<i16>()
            .map(|sample| sample.map(|value| value as f32 / i16::MAX as f32))
            .collect::<Result<Vec<_>, _>>()?,
        (hound::SampleFormat::Float, 32) => {
            reader.samples::<f32>().collect::<Result<Vec<_>, _>>()?
        }
        other => {
            return Err(DecoderError::UnsupportedFormat(format!(
                "{:?} {}-bit",
                other.0, other.1
            )));
        }
    };

    let samples = if channels == 1 {
        interleaved
    } else {
        interleaved
            .chunks_exact(channels)
            .map(|chunk| chunk.iter().copied().sum::<f32>() / channels as f32)
            .collect()
    };

    Ok(AudioBuffer {
        sample_rate_hz: spec.sample_rate,
        samples,
    })
}
