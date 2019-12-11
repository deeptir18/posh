extern crate dash;
extern crate exitcode;
extern crate shell;
extern crate structopt_derive;
use dash::dag::node;
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
    #[structopt(short = "sa", long = "server_addr", default_value = "127.0.0.1")]
    server_addr: String,
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
    #[structopt(short = "cf", long = "client_folder")]
    client_folder: String,
    #[structopt(
        short = "a",
        long = "annotations_file",
        help = "File with annotation list."
    )]
    annotation_file: String,
}

fn main() {
    let opt = Opt::from_args();
    let server_addr = opt.server_addr;
    let runtime_port = opt.runtime_port;
    let client_folder = opt.client_folder;
    let mount_file = opt.mount_file;
    let annotation_file = opt.annotation_file;

    let mut client =
        dash::runtime::client::ShellClient::new(&server_addr, &runtime_port, &client_folder);

    let mut interpreter: interpreter::Interpreter =
        match interpreter::Interpreter::new(&annotation_file, &mount_file) {
            Ok(i) => i,
            Err(e) => {
                eprintln!(
                    "Failed to build interpreter with given annotation file and mount file: {:?}",
                    e
                );
                process::exit(exitcode::USAGE);
            }
        };

    loop {
        print!(">>> ");
        let _ = stdout().flush();
        let cmd = readline();
        match interpreter.parse_command(&cmd) {
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

/// blocks while reading the next line from stdin
fn readline() -> String {
    let mut input = String::new();
    stdin().read_line(&mut input).unwrap();
    input
}

fn run_program(prog: node::Program, client: &mut dash::runtime::client::ShellClient) -> Result<()> {
    match client.send_request(prog) {
        Ok(_) => Ok(()),
        Err(e) => bail!("Error running request at server: {:?}", e),
    }
}
