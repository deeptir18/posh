extern crate dash;
extern crate shell;
use dash::graph::program;
use dash::runtime::new_client as client;
use dash::util::Result;
use failure::bail;
use shell::annotations::interpreter;
use std;
use std::io::{stdin, stdout, Write};
use std::process;
use structopt::StructOpt;
#[derive(Debug, StructOpt)]
#[structopt(
    name = "shell_binary",
    help = "Client shell that demonstrates split storage shell capabilities."
)]

struct Opt {
    #[structopt(
        short = "run",
        long = "runtime_port",
        default_value = "1234",
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
}

fn main() {
    let opt = Opt::from_args();
    let runtime_port = opt.runtime_port;
    let mount_file = opt.mount_file;
    let annotation_file = opt.annotation_file;

    let mut client = match client::ShellClient::new(&runtime_port, &mount_file) {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "Failed to construct shell client with given mount file: {:?}",
                e
            );
            process::exit(exitcode::USAGE);
        }
    };

    let mut interpreter = match interpreter::Interpreter::new(&annotation_file, &mount_file) {
        Ok(i) => i,
        Err(e) => {
            eprintln!(
                "Failed to construct interpreter with given mount file and annotation file: {:?}",
                e
            );
            process::exit(exitcode::USAGE);
        }
    };

    loop {
        print!(">>> ");
        let _ = stdout().flush();
        let cmd = match readline() {
            Ok(s) => s,
            Err(e) => {
                println!("Failed to read line: {:?}", e);
                continue;
            }
        };
        match interpreter.parse_cmd_graph(&cmd) {
            Ok(p) => {
                println!("parsed program: {:?}", p);
                match run_program(p, &mut client) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("{:?}", e);
                    }
                }
            }
            Err(e) => {
                println!("Failed to parse: {} -> {:?}", cmd, e);
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

fn run_program(prog: program::Program, client: &mut client::ShellClient) -> Result<()> {
    match client.run_command(prog) {
        Ok(_) => Ok(()),
        Err(e) => bail!("Error running program: {:?}", e),
    }
}
