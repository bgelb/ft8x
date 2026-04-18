use clap::{Parser, Subcommand, ValueEnum};
use rigctl::{Antenna, Band, Mode, Rig, RigConnectionConfig, RigKind, resolve_rig_kind};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(author, version, about = "Minimal rig control CLI")]
struct Cli {
    #[arg(long)]
    rig: Option<RigKind>,
    #[arg(long)]
    port: Option<PathBuf>,
    #[arg(long, default_value_t = 500)]
    timeout_ms: u64,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Get {
        #[arg(value_enum, default_value_t = GetField::All)]
        field: GetField,
    },
    Set {
        #[command(subcommand)]
        field: SetField,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum GetField {
    All,
    Frequency,
    Mode,
    Band,
    Signal,
    Antenna,
}

#[derive(Subcommand, Debug)]
enum SetField {
    Frequency { hz: u64 },
    Mode { mode: ModeArg },
    Band { band: String },
    Antenna { antenna: AntennaArg },
}

#[derive(Clone, Debug)]
struct ModeArg(Mode);

impl std::str::FromStr for ModeArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

#[derive(Clone, Debug)]
struct AntennaArg(Antenna);

impl std::str::FromStr for AntennaArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let kind = resolve_rig_kind(cli.rig)?;
    let mut rig = Rig::connect(RigConnectionConfig {
        kind,
        port_path: cli.port,
        timeout: Duration::from_millis(cli.timeout_ms),
    })?;

    match cli.command {
        Command::Get { field } => match field {
            GetField::All => {
                let state = rig.read_snapshot()?;
                println!("kind={}", state.kind);
                println!("frequency_hz={}", state.frequency_hz);
                println!("mode={}", state.mode);
                println!("band={}", state.band);
                println!("transmitting={}", state.transmitting);
                if let Some(bar_graph) = state.telemetry.bar_graph {
                    println!("signal_bar_graph={bar_graph}");
                }
                if let Some(rx_s) = state.telemetry.rx_s_meter {
                    println!("rx_s_meter={rx_s:.1}");
                }
                if let Some(fwd) = state.telemetry.tx_forward_power_w {
                    println!("tx_forward_power_w={fwd:.1}");
                }
                if let Some(swr) = state.telemetry.tx_swr {
                    println!("tx_swr={swr:.1}");
                }
                if let Rig::K3s(k3) = &mut rig {
                    println!("antenna={}", k3.get_antenna()?);
                }
            }
            GetField::Frequency => println!("{}", rig.get_frequency_hz()?),
            GetField::Mode => println!("{}", rig.get_mode()?),
            GetField::Band => println!("{}", rig.get_band()?),
            GetField::Signal => {
                let signal = rig.read_snapshot()?.telemetry;
                if let Some(rx_s) = signal.rx_s_meter {
                    println!("rx_s_meter={rx_s:.1}");
                }
                if let Some(bar_graph) = signal.bar_graph {
                    println!("bar_graph={bar_graph}");
                }
            }
            GetField::Antenna => match &mut rig {
                Rig::K3s(k3) => println!("{}", k3.get_antenna()?),
                Rig::Mchf(_) => return Err("antenna query is not supported on mcHF".into()),
            },
        },
        Command::Set { field } => match field {
            SetField::Frequency { hz } => {
                rig.set_frequency_hz(hz)?;
                println!("{}", rig.get_frequency_hz()?);
            }
            SetField::Mode { mode } => {
                rig.set_mode(mode.0)?;
                println!("{}", rig.get_mode()?);
            }
            SetField::Band { band } => {
                let band: Band = band.parse()?;
                let frequency = match band {
                    Band::M160 => 1_840_000,
                    Band::M80 => 3_573_000,
                    Band::M60 => 5_357_000,
                    Band::M40 => 7_074_000,
                    Band::M30 => 10_136_000,
                    Band::M20 => 14_074_000,
                    Band::M17 => 18_100_000,
                    Band::M15 => 21_074_000,
                    Band::M12 => 24_915_000,
                    Band::M10 => 28_074_000,
                    Band::M6 => 50_313_000,
                    Band::Xvtr(_) => return Err("xvtr band set is not supported here".into()),
                };
                rig.set_frequency_hz(frequency)?;
                println!("{}", rig.get_band()?);
            }
            SetField::Antenna { antenna } => match &mut rig {
                Rig::K3s(k3) => {
                    k3.set_antenna(antenna.0)?;
                    println!("{}", k3.get_antenna()?);
                }
                Rig::Mchf(_) => return Err("antenna selection is not supported on mcHF".into()),
            },
        },
    }

    Ok(())
}
