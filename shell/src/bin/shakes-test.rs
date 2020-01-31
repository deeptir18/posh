extern crate dash;
extern crate exitcode;
extern crate shell;

use dash::graph::program;
use dash::runtime::new_client as client;
use dash::util::Result;
use failure::bail;
use shell::annotations::interpreter;
use std::env::current_dir;
use std::process::exit;
use structopt::StructOpt;
use tracing::{debug, error, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "shell_binary",
    help = "client shell that demonstrates split storage shell capabilities."
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
    #[structopt(
        short = "tmp",
        long = "tmpfile",
        help = "Place to keep temporary stuff"
    )]
    tmp_file: String,
}

fn main() {
    let opt = Opt::from_args();
    let runtime_port = opt.runtime_port;
    let mount_info = opt.mount_file;
    let annotation_file = opt.annotation_file;
    let tmp_file = opt.tmp_file;

    let pwd = match current_dir() {
        Ok(p) => p,
        Err(e) => {
            error!("Failed to find current dir: {:?}", e);
            exit(exitcode::USAGE);
        }
    };

    // global tracing settings
    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(Level::TRACE)
        // completes the builder.
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting defualt subscriber failed");

    let mut client = match client::ShellClient::new(&runtime_port, &mount_info, pwd, &tmp_file) {
        Ok(s) => s,
        Err(e) => {
            error!(
                "Failed to construct shell client with given mount file: {:?}",
                e
            );
            exit(exitcode::USAGE);
        }
    };

    let mut interpreter = match interpreter::Interpreter::new(&annotation_file, &mount_info) {
        Ok(i) => i,
        Err(e) => {
            error!(
                "Failed to construct intepreter with given mount file and annotation file: {:?}",
                e
            );
            exit(exitcode::USAGE);
        }
    };

    match test_cmd("cat /home/deeptir/mnt/dash_server/shakes_new.txt | wc | awk '{print \"Lines: \" $1 \"\tWords: \" $2 \"\tCharacter: \" $3 }' > /home/deeptir/research/fs_project/client_folders/local_extra/foo.txt", &mut interpreter, &mut client) {
        Ok(_) => debug!("Successfully ran command remotely while directing output to file"),
        Err(e) => error!("Failed to run command: {:?}", e),
    }
}

fn test_cmd(
    cmd: &str,
    interpreter: &mut interpreter::Interpreter,
    client: &mut client::ShellClient,
) -> Result<()> {
    let dag = interpreter.parse_cmd_graph(&cmd)?;
    run_program(dag, client)?;
    Ok(())
}

fn run_program(prog: program::Program, client: &mut client::ShellClient) -> Result<()> {
    // change the working directory of the client to resolve filepaths correctly
    let pwd = current_dir()?;
    client.set_pwd(pwd);
    match client.run_command(prog) {
        Ok(_) => Ok(()),
        Err(e) => bail!("Error running program: {:?}", e),
    }
}
