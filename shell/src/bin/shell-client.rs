extern crate dash;
extern crate shell;
use dash::graph::program;
use dash::runtime::new_client as client;
use dash::util::Result;
use failure::bail;
use shell::interpreter::interpreter;
use shell::scheduler::heuristic::HeuristicScheduler;
use std::env::current_dir;
use std::io::{stdin, stdout, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::process::exit;
use structopt::StructOpt;
use tracing::{error, Level};
use tracing_subscriber::{filter::LevelFilter, FmtSubscriber};

#[derive(Debug)]
enum TraceLevel {
    Debug,
    Info,
    Warn,
    Error,
    Off,
}

impl std::str::FromStr for TraceLevel {
    type Err = failure::Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "debug" => TraceLevel::Debug,
            "info" => TraceLevel::Info,
            "warn" => TraceLevel::Warn,
            "error" => TraceLevel::Error,
            "off" => TraceLevel::Off,
            x => bail!("unknown TRACE level {:?}", x),
        })
    }
}
#[derive(Debug, StructOpt)]
#[structopt(
    name = "shell_binary",
    help = "client shell that demonstrates split storage shell capabilities."
)]
struct Opt {
    #[structopt(
        short = "run",
        long = "runtime_port",
        default_value = "1235",
        help = "Shared filesystem port"
    )]
    runtime_port: String,
    #[structopt(
        short = "f",
        long = "mount_file",
        help = "Folder containing shared mount information."
    )]
    mount_file: String,
    #[structopt(
        short = "a",
        long = "annotations_file",
        help = "File with annotation list."
    )]
    annotation_file: String,
    #[structopt(
        short = "pwd",
        long = "pwd",
        help = "Working directory to run this script"
    )]
    pwd: String,
    #[structopt(
        short = "tmp",
        long = "tmpfile",
        help = "Place to keep temporary stuff"
    )]
    tmp_file: String,
    #[structopt(
        short = "split",
        long = "splitting_factor",
        help = "How much the frontend should split things.",
        default_value = "1"
    )]
    splitting_factor: u32,
    #[structopt(
        short = "trace",
        long = "tracing_level",
        help = "Configure tracing settings.",
        default_value = "off"
    )]
    trace_level: TraceLevel,
}
fn main() {
    let opt = Opt::from_args();
    let mount_info = opt.mount_file;
    let annotation_file = opt.annotation_file;
    let runtime_port = opt.runtime_port;
    let given_pwd = opt.pwd;
    let tmp_file = opt.tmp_file;
    let splitting_factor: u32 = opt.splitting_factor;
    let trace_level = opt.trace_level;
    let subscriber = match trace_level {
        TraceLevel::Debug => FmtSubscriber::builder()
            .with_max_level(Level::DEBUG)
            .finish(),
        TraceLevel::Info => FmtSubscriber::builder()
            .with_max_level(Level::INFO)
            .finish(),
        TraceLevel::Warn => FmtSubscriber::builder()
            .with_max_level(Level::WARN)
            .finish(),
        TraceLevel::Error => FmtSubscriber::builder()
            .with_max_level(Level::ERROR)
            .finish(),
        TraceLevel::Off => FmtSubscriber::builder()
            .with_max_level(LevelFilter::OFF)
            .finish(),
    };
    tracing::subscriber::set_global_default(subscriber).expect("setting defualt subscriber failed");
    let mut pwd = match current_dir() {
        Ok(p) => p,
        Err(e) => {
            error!("Failed to find current dir: {:?}", e);
            exit(exitcode::USAGE);
        }
    };

    if given_pwd != "." {
        pwd = Path::new(&given_pwd).to_path_buf();
    }
    let mut client = match client::ShellClient::new(&runtime_port, pwd.clone(), &tmp_file) {
        Ok(s) => s,
        Err(e) => {
            error!(
                "Failed to construct shell client with given mount file: {:?}",
                e
            );
            process::exit(exitcode::USAGE);
        }
    };

    let mut interpreter = match interpreter::Interpreter::new(
        &mount_info,
        &annotation_file,
        Box::new(HeuristicScheduler {}),
    ) {
        Ok(i) => i,
        Err(e) => {
            error!(
                "Failed to construct interpreter with given mount file and annotation file: {:?}",
                e
            );
            process::exit(exitcode::USAGE);
        }
    };
    interpreter.set_pwd(pwd.clone());
    interpreter.set_splitting_factor(splitting_factor);
    print!("\x1B[2J\x1B[1;1H");
    loop {
        print!("\x1b[92mposh>>>\x1b[0m ");
        let _ = stdout().flush();
        let cmd = match readline() {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to read line: {:?}", e);
                continue;
            }
        };
        let dag = match interpreter.parse_command_line(&cmd) {
            Ok(d) => match d {
                Some(graph) => graph,
                None => {
                    continue;
                }
            },
            Err(e) => {
                error!("Failed to parse: {:?}", e);
                continue;
            }
        };
        match run_program(dag, &mut client, pwd.clone()) {
            Ok(_) => {}
            Err(e) => {
                error!("Failed to execute: {:?}", e);
                continue;
            }
        }
    }
}

fn readline() -> Result<String> {
    let mut input = String::new();
    match stdin().read_line(&mut input) {
        Ok(_) => {}
        Err(e) => bail!("Failed to read line: {:?}", e),
    }
    Ok(input)
}

fn run_program(
    prog: program::Program,
    client: &mut client::ShellClient,
    pwd: PathBuf,
) -> Result<()> {
    // first, set the client's view of the current directory
    client.set_pwd(pwd.clone());
    match client.run_command(prog) {
        Ok(_) => Ok(()),
        Err(e) => bail!("Error running program: {:?}", e),
    }
}
