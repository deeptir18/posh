extern crate dash;
extern crate structopt;
extern crate structopt_derive;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::process;
use std::str::FromStr;
use structopt::StructOpt;
use tracing::{error, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "Server",
    about = "Server Binary for running Dash evaluation experiments with a single client."
)]
struct Opt {
    #[structopt(short = "ip", long = "ip_address", help = "IP address for client")]
    ip_addr: String,
    #[structopt(
        short = "f",
        long = "folder",
        help = "Path to for this client's shared folder on the server."
    )]
    client_folder: String,
    #[structopt(short = "run", long = "runtime_port", default_value = "1234")]
    runtime_port: String,
    #[structopt(short = "debug", long = "debug")]
    debug: bool,
    #[structopt(short = "tmp", long = "tmpfile")]
    tmp_file: String,
}

fn main() {
    let opt = Opt::from_args();
    let runtime_port: String = opt.runtime_port;
    let debug: bool = opt.debug;
    let ip_addr = opt.ip_addr;
    let client_folder = opt.client_folder;
    let tmp_file = opt.tmp_file;
    let mut client_map: HashMap<IpAddr, String> = HashMap::default();

    // tracing
    // a builder for `FmtSubscriber`.
    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(Level::TRACE)
        // completes the builder.
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting defualt subscriber failed");

    // local loopback
    let addr = match Ipv4Addr::from_str(&ip_addr) {
        Ok(a) => a,
        Err(e) => {
            error!("Not a valid IPV4Addr: {:?} -> {:?}", ip_addr, e);
            process::exit(exitcode::USAGE);
        }
    };
    client_map.insert(IpAddr::V4(addr), client_folder.clone());
    dash::start_runtime(&runtime_port, client_map, debug, &tmp_file);
}
