use super::Result;
use failure::bail;
use std::net::{TcpListener, TcpStream};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Hash, Eq)]
pub struct Addr {
    ip: String,
    port: String,
}

impl Addr {
    pub fn new(ip: &str, port: &str) -> Self {
        Addr {
            ip: ip.to_string(),
            port: port.to_string(),
        }
    }

    pub fn get_port(&self) -> String {
        return self.port.clone();
    }

    pub fn get_ip(&self) -> String {
        return self.ip.clone();
    }

    pub fn get_addr(&self) -> String {
        new_addr(&self.ip, &self.port)
    }

    pub fn new_server(&self) -> Result<TcpListener> {
        new_server(&self.ip, &self.port)
    }

    pub fn new_listener(&self) -> Result<TcpStream> {
        match TcpStream::connect(self.get_addr()) {
            Ok(s) => Ok(s),
            Err(e) => bail!("Error connecting to port on server: {:?}", e),
        }
    }
}
pub fn new_server(ip_addr: &str, port: &str) -> Result<TcpListener> {
    match TcpListener::bind(new_addr(ip_addr, port)) {
        Ok(t) => Ok(t),
        Err(e) => bail!("Error binding to port: {:?}", e),
    }
}

pub fn new_addr(server_addr: &str, server_port: &str) -> String {
    let mut addr = String::from(server_addr);
    addr.push_str(":");
    addr.push_str(server_port);
    addr
}

pub trait Server {
    fn handle_incoming(&mut self) -> Result<()>;

    fn server_name(&self) -> String;

    fn handle_client(&mut self, stream: TcpStream) -> Result<()>;

    fn get_clone(&mut self) -> Result<TcpListener>;
}
