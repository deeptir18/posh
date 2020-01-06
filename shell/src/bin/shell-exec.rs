extern crate dash;
extern crate exitcode;
extern crate shell;

use dash::graph::program;
use dash::runtime::new_client as client;
use dash::util::Result;
use failure::bail;
use shell::annotations::interpreter;
use std::env::current_dir;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::exit;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "shell_exec", help = "Shell to execute dash binaries")]

struct Opt {
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
}

fn main() {
    let opt = Opt::from_args();
    let binary = opt.binary;
    let mount_info = opt.mount_file;
    let annotation_file = opt.annotation_file;
    let runtime_port = opt.runtime_port;
    let given_pwd = opt.pwd;
    let tmp_file = opt.tmp_file;
    let mut pwd = match current_dir() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Failed to find current dir: {:?}", e);
            exit(exitcode::USAGE);
        }
    };

    if given_pwd != "." {
        pwd = Path::new(&given_pwd).to_path_buf();
    }

    let mut client =
        match client::ShellClient::new(&runtime_port, &mount_info, pwd.clone(), &tmp_file) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Failed to construct a shell with given mount file: {:?}", e);
                exit(exitcode::USAGE);
            }
        };

    let mut interpreter = match interpreter::Interpreter::new(&annotation_file, &mount_info) {
        Ok(i) => i,
        Err(e) => {
            eprintln!(
                "Failed to construct intepreter with given mount file and annotation file: {:?}",
                e
            );
            exit(exitcode::USAGE);
        }
    };
    interpreter.set_pwd(pwd.clone());

    // loop over the binary, and execute all of the commands
    let file = match File::open(binary) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Failed to open binary file: {:?}", e);
            exit(exitcode::OSFILE);
        }
    };

    let reader = BufReader::new(file);
    for (_, line) in reader.lines().enumerate() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                eprintln!("Failed to read line from binary: {:?}", e);
                exit(exitcode::USAGE);
            }
        };

        match run_cmd(&line, &mut interpreter, &mut client) {
            Ok(_) => {}
            Err(e) => {
                eprintln!("Failed to run line: {:?} with err {:?}", &line, e);
                exit(exitcode::USAGE);
            }
        }
    }
}

fn run_cmd(
    cmd: &str,
    interpreter: &mut interpreter::Interpreter,
    client: &mut client::ShellClient,
) -> Result<()> {
    let pwd = current_dir()?;
    // if the line begins with a comment, just return
    match cmd.to_string().starts_with("#") {
        true => return Ok(()),
        false => {}
    }
    let dag = match interpreter.parse(&cmd) {
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
