extern crate dash;
extern crate structopt;
extern crate structopt_derive;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "Server", about = "Server Binary")]
struct Opt {
    #[structopt(
        short = "f",
        long = "folder",
        help = "Path to store libraries and such"
    )]
    client_folder: String,
    #[structopt(short = "run", long = "runtime_port", default_value = "1234")]
    runtime_port: String,
    #[structopt(short = "debug", long = "debug")]
    debug: bool,
}

fn main() {
    let opt = Opt::from_args();
    let client_folder: String = opt.client_folder;
    let runtime_port: String = opt.runtime_port;
    let debug: bool = opt.debug;
    dash::start_shell(&runtime_port, &client_folder, debug);
}
