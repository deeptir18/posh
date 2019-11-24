#![feature(rustc_private)]
#[macro_use]
extern crate serde;
extern crate failure;
extern crate serde_derive;
pub mod dag;
pub mod graph;
pub mod runtime;
pub mod serialize;
pub mod util;
use crate::runtime::runtime::ShellServer;
use crate::runtime::runtime_util::Server;
use std::thread;

pub fn start_shell(runtime_port: &str, client_folder: &str, debug: bool) {
    let localhost = "0.0.0.0";
    let mut shell_server = ShellServer::new(localhost, runtime_port, client_folder, debug).unwrap();
    let child = thread::spawn(move || match shell_server.handle_incoming() {
        Ok(_) => unreachable!(),
        Err(e) => {
            println!("Shell server error: {:?}", e);
        }
    });
    let _ = child.join();
}
