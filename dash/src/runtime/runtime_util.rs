use super::Result;
use std::net::{TcpListener, TcpStream};

pub fn new_server(ip_addr: &str, port: &str) -> Result<TcpListener> {
    let mut addr = String::from(ip_addr.clone());
    addr.push_str(":");
    addr.push_str(port);
    Ok(TcpListener::bind(addr.clone())?)
}

pub fn new_addr(server_addr: &str, server_port: &str) -> String {
    let mut addr = String::from(server_addr);
    addr.push_str(":");
    addr.push_str(server_port);
    addr
}

pub trait Server {
    fn handle_incoming(&mut self) -> Result<!>;

    fn server_name(&self) -> String;

    fn handle_client(&mut self, stream: TcpStream) -> Result<()>;

    fn get_clone(&mut self) -> Result<TcpListener>;
}
