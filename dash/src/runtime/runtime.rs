use super::dag::node;
use super::runtime_util::{new_server, Server};
use super::serialize::{read_msg, rpc, write_msg};
use super::Result;
use bincode::{deserialize, serialize};
use failure::bail;
use std::net::{SocketAddr, TcpListener, TcpStream};
//use std::time::Instant;
use std::{fs, thread};

pub struct ShellServer {
    server: TcpListener,
    client_folder: String,
    debug: bool,
    //start: Instant,
    // TODO: add in system stats so the server can see it
}

impl ShellServer {
    pub fn new(ip: &str, port: &str, client_folder: &str, debug: bool) -> Result<Self> {
        fs::create_dir_all(client_folder)?;
        Ok(ShellServer {
            server: new_server(ip, port)?,
            client_folder: String::from(client_folder),
            debug: debug,
            //start: Instant::now(),
        })
    }
}

impl Server for ShellServer {
    fn get_clone(&mut self) -> Result<TcpListener> {
        Ok(self.server.try_clone()?)
    }

    fn server_name(&self) -> String {
        String::from("Shell Server")
    }

    fn handle_client(&mut self, _stream: TcpStream) -> Result<()> {
        unimplemented!();
    }

    fn handle_incoming(&mut self) -> Result<!> {
        let clone = self.get_clone()?;
        for stream in clone.incoming() {
            match stream {
                Ok(s) => {
                    let peer_addr = s.peer_addr()?;
                    let folder = self.client_folder.clone();
                    let server_name = self.server_name();
                    let debug = self.debug.clone();
                    thread::spawn(
                        move || match handle_spawned_client(s, folder, debug.clone()) {
                            Ok(_) => {
                                println!(
                                    "{}: Successfully handled request from {}",
                                    server_name, peer_addr
                                );
                            }
                            Err(e) => {
                                println!(
                                    "{}: Error handling request from {}: {:?}",
                                    server_name, peer_addr, e
                                );
                            }
                        },
                    );
                }
                Err(e) => {
                    if self.debug {
                        println!(
                            "{}: Err handling client stream: {:?}",
                            self.server_name(),
                            e
                        );
                    }
                }
            }
        }
        unreachable!();
    }
}

fn get_available_port(used_ports: &mut Vec<u16>) -> Result<u16> {
    if let Some(port) = (1025..65535).find(|port| port_is_available(*port, used_ports)) {
        return Ok(port);
    }
    bail!("No more ports available");
}

fn port_is_available(port: u16, used_ports: &mut Vec<u16>) -> bool {
    if used_ports.iter().any(|x| port == *x) {
        return false;
    }
    match TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], port))) {
        Ok(_) => true,
        Err(_) => false,
    }
}

fn redirect_thread(port: u16) -> Result<TcpStream> {
    let listener =
        TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], port))).expect("port should be free");
    for conn_result in listener.incoming() {
        let conn = conn_result.expect("Error on receiving connection");
        // TODO: some sort of authentication that it's actually the right client
        return Ok(conn);
    }
    bail!("Should have returned first connection".to_string())
}

fn handle_spawned_client(mut stream: TcpStream, client_folder: String, debug: bool) -> Result<()> {
    // TODO: if the server is too busy - say no to the request
    // add in load balancing here
    let buf = read_msg(&mut stream)?;

    // read the RPC request
    let mut program: node::Program = match deserialize(&buf[..]) {
        Ok(req) => req,
        Err(e) => {
            let response = serialize(&rpc::ClientReturnCode::Failure)?;
            write_msg(response.to_vec(), &mut stream)?;
            bail!("Could not deserialize client request: {:?}", e)
        }
    };

    // spawn two more TCP connections for sending stdout and stdin
    let stdout_port = get_available_port(&mut vec![])?;
    let stderr_port = get_available_port(&mut vec![stdout_port])?;
    let port_msg = serialize(&rpc::StreamSetupMsg {
        stdout_port: stdout_port,
        stderr_port: stderr_port,
    })?;

    let stdout_thread: thread::JoinHandle<Result<TcpStream>> =
        thread::spawn(move || redirect_thread(stdout_port));
    let stderr_thread: thread::JoinHandle<Result<TcpStream>> =
        thread::spawn(move || redirect_thread(stderr_port));

    // tell the client to keep these two ports open
    write_msg(port_msg.to_vec(), &mut stream)?;
    let stdout_stream = match stdout_thread.join() {
        Ok(res) => res.expect("Error on getting stdout tcp connection"),
        Err(e) => {
            bail!("stdout thread did not join: {:?}", e);
        }
    };
    let stderr_stream = match stderr_thread.join() {
        Ok(res) => res.expect("Error on getting stderr tcp connection"),
        Err(e) => {
            bail!("stderr thread did not join: {:?}", e);
        }
    };

    // execute the program, directing stdout and stderr to the right places
    match program.execute(stdout_stream, stderr_stream, &client_folder, debug) {
        Ok(_) => {}
        Err(e) => {
            let bytes = serialize(&rpc::ClientReturnCode::Failure)?;
            write_msg(bytes.to_vec(), &mut stream)?;
            bail!("Program failed to execute on the server: {:?}", e);
        }
    }

    let bytes = serialize(&rpc::ClientReturnCode::Success)?;
    write_msg(bytes.to_vec(), &mut stream)?;
    Ok(())
}
