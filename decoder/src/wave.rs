use std::path::Path;

use thiserror::Error;

use crate::modes::ft8::FT8_SAMPLE_RATE;

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

    let (sample_rate_hz, samples) = if spec.sample_rate == FT8_SAMPLE_RATE {
        (spec.sample_rate, samples)
    } else {
        (
            FT8_SAMPLE_RATE,
            resample_linear(&samples, spec.sample_rate, FT8_SAMPLE_RATE),
        )
    };

    Ok(AudioBuffer {
        sample_rate_hz,
        samples,
    })
}

pub fn write_wav(path: impl AsRef<Path>, audio: &AudioBuffer) -> Result<(), DecoderError> {
    let spec = hound::WavSpec {
        channels: 1,
        sample_rate: audio.sample_rate_hz,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };
    let mut writer = hound::WavWriter::create(path, spec)?;
    for sample in &audio.samples {
        let clipped = sample.clamp(-1.0, 1.0);
        let quantized = (clipped * i16::MAX as f32).round() as i16;
        writer.write_sample(quantized)?;
    }
    writer.finalize()?;
    Ok(())
}

fn resample_linear(samples: &[f32], src_rate_hz: u32, dst_rate_hz: u32) -> Vec<f32> {
    if samples.is_empty() || src_rate_hz == dst_rate_hz {
        return samples.to_vec();
    }

    let output_len = ((samples.len() as u64 * dst_rate_hz as u64) + (src_rate_hz as u64 / 2))
        / src_rate_hz as u64;
    let mut output = Vec::with_capacity(output_len as usize);
    let scale = src_rate_hz as f64 / dst_rate_hz as f64;
    for index in 0..output_len as usize {
        let position = index as f64 * scale;
        let left = position.floor() as usize;
        let right = (left + 1).min(samples.len().saturating_sub(1));
        let frac = (position - left as f64) as f32;
        let interpolated = samples[left] * (1.0 - frac) + samples[right] * frac;
        output.push(interpolated);
    }
    output
}
