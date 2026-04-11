const COARSE_SNR_SIGNAL_FLOOR: f32 = 1.0;
const COARSE_SNR_DB_OFFSET: f32 = 14.8;
const COARSE_SNR_FLOOR_DB: f32 = -21.0;

pub(super) fn ft4_reported_snr_db(coarse_score: f32) -> i32 {
    let xsnr = if coarse_score > COARSE_SNR_SIGNAL_FLOOR {
        10.0 * (coarse_score - COARSE_SNR_SIGNAL_FLOOR).log10() - COARSE_SNR_DB_OFFSET
    } else {
        COARSE_SNR_FLOOR_DB
    };
    xsnr.max(COARSE_SNR_FLOOR_DB).round() as i32
}
