#![feature(rustc_private)]
#![feature(never_type)]
#[macro_use]
extern crate serde;
extern crate crossbeam;
extern crate failure;
extern crate nix;
extern crate serde_derive;
pub mod dag;
pub mod graph;
pub mod runtime;
pub mod serialize;
pub mod util;
use crate::runtime::new_runtime::{ClientMap, ServerRuntime};
use crate::runtime::runtime::ShellServer;
use crate::runtime::runtime_util::Server;
use std::thread;
use tracing::error;

pub fn start_shell(runtime_port: &str, client_folder: &str, debug: bool) {
    let localhost = "0.0.0.0";
    let mut shell_server = ShellServer::new(localhost, runtime_port, client_folder, debug).unwrap();
    let child = thread::spawn(move || match shell_server.handle_incoming() {
        Ok(_) => unreachable!(),
        Err(e) => {
            error!("Shell server error: {:?}", e);
        }
    });
    let _ = child.join();
}

pub fn start_runtime(runtime_port: &str, client_map: ClientMap, debug: bool, tmp_file: &str) {
    let localhost = "0.0.0.0";
    let mut runtime =
        ServerRuntime::new(localhost, runtime_port, client_map, debug, tmp_file).unwrap();
    let child = thread::spawn(move || match runtime.handle_incoming() {
        Ok(_) => unreachable!(),
        Err(e) => {
            error!("Shell server error: {:?}", e);
        }
    });
    let _ = child.join();
}
