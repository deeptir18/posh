extern crate dash;
extern crate shell;
use dash::dag::node;
use dash::util::Result;
use failure::bail;
use shell::annotations::ast;
use std;
use std::io::{stdin, stdout, Write};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "shell test", about = "Client shell binary")]
struct Opt {
    #[structopt(short = "sa", long = "server_addr", default_value = "127.0.0.1")]
    server_addr: String,
    #[structopt(short = "run", long = "runtime_port", default_value = "1234")]
    runtime_port: String,
    #[structopt(short = "m", long = "mount_info")]
    mount_info: String,
    #[structopt(short = "a", long = "annotations")]
    annotations_file: String,
}
fn main() {
    let opt = Opt::from_args();
    let server_addr: String = opt.server_addr;
    let runtime_port: String = opt.runtime_port;
    let mount_info: String = opt.mount_info;
    let anns: String = opt.annotations_file;

    // TODO: change the client to not take in a specific client folder
    let parser = match ast::Parser::new(&anns, &mount_info) {
        Ok(p) => p,
        Err(e) => {
            panic!("Failed to parse mount file or annotations file: {:?}", e);
        }
    };

    // TODO: fix this
    let client_folder = parser.get_client_folder();
    let mut client =
        dash::runtime::client::ShellClient::new(&server_addr, &runtime_port, &client_folder);
    loop {
        print!(">>> ");
        let _ = stdout().flush();
        let cmd = readline();

        match parser.parse_command(cmd.as_ref()) {
            Ok(p) => match run_program(p, client.clone()) {
                Ok(_) => {}
                Err(e) => {
                    println!("{:?}", e);
                }
            },
            Err(e) => {
                println!("Failed to parse: {} -> {:?}", cmd, e);
            }
        }
    }
}

// blocks while reading the next line from stdin
fn readline() -> String {
    let mut input = String::new();
    stdin().read_line(&mut input).unwrap();
    input
}

fn run_program(prog: node::Program, client: dash::runtime::client::ShellClient) -> Result<()> {
    match client.send_request(prog) {
        Ok(_) => Ok(()),
        Err(e) => bail!("Error running request at server: {:?}", e),
    }
}
