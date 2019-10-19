extern crate dash;
extern crate shell;
extern crate structopt_derive;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "shell_binary",
    help = "Client shell that demonstrates split storage shell capabilities."
)]

struct Opt {
    #[structopt(
        short = "sa",
        long = "server_addr",
        default_value = "127.0.0.1",
        help = "Shared filesystem server address"
    )]
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
    #[structopt(
        short = "a",
        long = "annotations_file",
        help = "File with annotation list."
    )]
    annotation_file: String,
}

fn main() {
    let opt = Opt::from_args();
    let server_addr: String = opt.server_addr;
    let runtime_port: String = opt.runtime_port;
    let client_folder: String = opt.mount_file;
    let annotation_file: String = opt.annotation_file;
}
