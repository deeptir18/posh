extern crate dash;
extern crate exitcode;
extern crate shell;
use dash::graph::program;
use dash::runtime::new_client as client;
use dash::util::Result;
use failure::bail;
use shell::interpreter::interpreter;
use shell::scheduler::dp::DPScheduler;
use std::env::current_dir;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
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
#[structopt(name = "shell_exec", help = "Shell to execute dash binaries")]
struct Opt {
    #[structopt(short, long)]
    prep: bool,
    #[structopt(
        short = "run",
        long = "runtime_port",
        default_value = "1234",
        help = "Shared filesystem port"
    )]
    runtime_port: String,
    #[structopt(help = "Dash binary to run")]
    binary: String,
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
    let binary = opt.binary;
    let mount_info = opt.mount_file;
    let annotation_file = opt.annotation_file;
    let runtime_port = opt.runtime_port;
    let given_pwd = opt.pwd;
    let tmp_file = opt.tmp_file;
    let splitting_factor: u32 = opt.splitting_factor;
    let trace_level = opt.trace_level;
    let prep = opt.prep;
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
            error!("Failed to construct a shell with the given params: {:?}", e);
            exit(exitcode::USAGE);
        }
    };

    let mut interpreter = match interpreter::Interpreter::new(
        &mount_info,
        &annotation_file,
        Box::new(DPScheduler {}),
    ) {
        Ok(i) => i,
        Err(e) => {
            error!(
                "Failed to construct interpreter with given mount file: {:?} and annotation file: {:?} -> {:?}",
                mount_info,
                annotation_file,
                e
            );
            exit(exitcode::USAGE);
        }
    };
    interpreter.set_pwd(pwd.clone());
    interpreter.set_splitting_factor(splitting_factor);
    // use more advanced file size query-er
    interpreter.set_offload_filecache(client.clone());

    // loop over the binary, and execute all of the commands
    let file = match File::open(binary) {
        Ok(f) => f,
        Err(e) => {
            error!("Failed to open binary file: {:?}", e);
            exit(exitcode::OSFILE);
        }
    };

    let reader = BufReader::new(file);
    for (_, line) in reader.lines().enumerate() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to read line from binary: {:?}", e);
                exit(exitcode::USAGE);
            }
        };

        match run_cmd(&line, &mut interpreter, &mut client, prep) {
            Ok(_) => {}
            Err(e) => {
                error!("Failed to run line: {:?} with err {:?}", &line, e);
                exit(exitcode::USAGE);
            }
        }
    }
}

fn run_cmd(
    cmd: &str,
    interpreter: &mut interpreter::Interpreter,
    client: &mut client::ShellClient,
    prep: bool,
) -> Result<()> {
    let pwd = current_dir()?;
    // if the line begins with a comment, just return
    match cmd.to_string().starts_with("#") {
        true => return Ok(()),
        false => {}
    }
    tracing::info!("Scheduling {:?}", cmd);
    let dag = match interpreter.parse_command_line(&cmd) {
        Ok(d) => match d {
            Some(graph) => graph,
            None => {
                return Ok(());
            }
        },
        Err(e) => {
            bail!("{:?}", e);
        }
    };
    interpreter.set_pwd(pwd.clone());
    // just run the scheduling phases of this pipeline
    if prep {
        return Ok(());
    }
    run_program(dag, client, pwd.clone())?;
    Ok(())
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
