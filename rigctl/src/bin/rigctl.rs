use clap::{Parser, Subcommand, ValueEnum};
use rigctl::{Antenna, Band, K3s, K3sConfig, Mode};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(author, version, about = "Minimal K3S rig control CLI")]
struct Cli {
    #[arg(long)]
    port: Option<PathBuf>,
    #[arg(long, default_value_t = rigctl::K3S_BAUD_RATE)]
    baud: u32,
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
    let mut config = K3sConfig::default();
    config.baud_rate = cli.baud;
    config.timeout = Duration::from_millis(cli.timeout_ms);
    if let Some(port) = cli.port {
        config.port_path = port;
    }

    let mut rig = K3s::connect(config)?;

    match cli.command {
        Command::Get { field } => match field {
            GetField::All => {
                let state = rig.read_state()?;
                println!("frequency_hz={}", state.frequency_hz);
                println!("mode={}", state.mode);
                println!("band={}", state.band);
                println!("antenna={}", state.antenna);
                println!("signal_coarse={}", state.signal.coarse);
                if let Some(high_res) = state.signal.high_res {
                    println!("signal_high_res={high_res}");
                }
                if let Some(bar_graph) = state.signal.bar_graph {
                    println!("signal_bar_graph={bar_graph}");
                }
                println!("receiving={}", state.signal.receiving);
            }
            GetField::Frequency => println!("{}", rig.get_frequency_hz()?),
            GetField::Mode => println!("{}", rig.get_mode()?),
            GetField::Band => println!("{}", rig.get_band()?),
            GetField::Signal => {
                let signal = rig.get_signal_level()?;
                println!("coarse={}", signal.coarse);
                if let Some(high_res) = signal.high_res {
                    println!("high_res={high_res}");
                }
                if let Some(bar_graph) = signal.bar_graph {
                    println!("bar_graph={bar_graph}");
                }
                println!("receiving={}", signal.receiving);
            }
            GetField::Antenna => println!("{}", rig.get_antenna()?),
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
                rig.set_band(band)?;
                println!("{}", rig.get_band()?);
            }
            SetField::Antenna { antenna } => {
                rig.set_antenna(antenna.0)?;
                println!("{}", rig.get_antenna()?);
            }
        },
    }

    Ok(())
}
