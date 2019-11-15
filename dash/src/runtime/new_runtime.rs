use super::graph::{program, stream};
use super::runtime_util::{new_server, Addr, Server};
use super::serialize::{read_msg_and_type, rpc, write_msg_and_type};
use super::Result;
use bincode::{deserialize, serialize};
use failure::bail;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::{fs, thread};
use stream::SharedStreamMap;
/// matches client IP to folder name
type ClientMap = HashMap<IpAddr, String>;

/// Map from client Ip to data structure that facilitates sharing streams across threads.
type ClientStreamMap = HashMap<IpAddr, SharedStreamMap>;
/// Runtime on server that services client requests.
pub struct ServerRuntime {
    addr: Addr,
    server: TcpListener,
    client_map: ClientMap,
    client_stream_map: ClientStreamMap,
    debug: bool,
}

impl ServerRuntime {
    /// Constructs a new server runtime with the given public IP, port, and ClientMap.
    pub fn new(ip: &str, port: &str, client_map: ClientMap, debug: bool) -> Result<Self> {
        for (_, folder) in client_map.iter() {
            fs::create_dir_all(folder)?;
        }
        let new_client_stream_map: ClientStreamMap = Default::default();
        Ok(ServerRuntime {
            addr: Addr::new(ip, port),
            server: new_server(ip, port)?,
            client_map: client_map,
            client_stream_map: new_client_stream_map,
            debug: debug,
        })
    }

    fn find_client_folder(&self, addr: SocketAddr) -> Result<String> {
        match self.client_map.get(&addr.ip()) {
            Some(v) => Ok(v.clone()),
            None => {
                bail!("Could not find client in client_mapping");
            }
        }
    }

    /// Gets shared stream map if it exists for this client,
    /// otherwise constructs a new one.
    fn get_stream_map(&mut self, addr: SocketAddr) -> SharedStreamMap {
        match self.client_stream_map.get(&addr.ip()) {
            Some(v) => v.clone(),
            None => {
                let new_map = SharedStreamMap::new();
                self.client_stream_map.insert(addr.ip(), new_map.clone());
                new_map
            }
        }
    }
}

impl Server for ServerRuntime {
    fn get_clone(&mut self) -> Result<TcpListener> {
        Ok(self.server.try_clone()?)
    }

    fn server_name(&self) -> String {
        self.addr.get_addr()
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
                    // find the folder that corresponds to this client
                    let folder_result = self.find_client_folder(peer_addr.clone());
                    // find, or create a new stream map for this client
                    let stream_map = self.get_stream_map(peer_addr.clone());
                    let server_name = self.server_name();
                    let debug = self.debug.clone();
                    thread::spawn(move || {
                        match handle_spawned_client(s, folder_result, stream_map, debug.clone()) {
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
                        }
                    });
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

fn handle_spawned_client(
    mut stream: TcpStream,
    folder_result: Result<String>,
    stream_map: SharedStreamMap,
    _debug: bool,
) -> Result<()> {
    let folder = match folder_result {
        Ok(f) => f,
        Err(e) => {
            bail!("Could not find folder: {:?}", e);
        }
    };

    let (msg_type, buf) = read_msg_and_type(&mut stream)?;
    // read the type of the message, and execute accordingly.
    match msg_type {
        // handle setting up the stream to another machine
        rpc::MessageType::SetupStreams => {
            let mut _msg: rpc::NetworkStreamInfo = match deserialize(&buf[..]) {
                Ok(info) => info,
                Err(e) => {
                    let response = serialize(&rpc::ClientReturnCode::Failure)?;
                    write_msg_and_type(response.to_vec(), rpc::MessageType::Control, &mut stream)?;
                    bail!("Could not deserialize program: {:?}", e)
                }
            };

            // TODO: send an ack to another server to open up this pipe
            // Send the client an ACK that it made all the pipe requests
            let ack = serialize(&rpc::ClientReturnCode::Success)?;
            write_msg_and_type(ack.to_vec(), rpc::MessageType::Control, &mut stream)?;
            Ok(())
            // Just setup any TCP streams
        }
        rpc::MessageType::ProgramExecution => {
            let mut program: program::Program = match deserialize(&buf[..]) {
                Ok(prog) => prog,
                Err(e) => {
                    let response = serialize(&rpc::ClientReturnCode::Failure)?;
                    write_msg_and_type(response.to_vec(), rpc::MessageType::Control, &mut stream)?;
                    bail!("Could not deserialize program: {:?}", e)
                }
            };

            // all the streams must be setup for this part of the program,
            // so execute the program!
            program.resolve_args(&folder)?;
            let response = match program.execute(stream_map) {
                Ok(_) => serialize(&rpc::ClientReturnCode::Success)?,
                Err(e) => {
                    println!("Could not execute program because {:?}", e);
                    serialize(&rpc::ClientReturnCode::Failure)?
                }
            };
            write_msg_and_type(response.to_vec(), rpc::MessageType::Control, &mut stream)?;
            Ok(())
        }
        rpc::MessageType::Pipe => {
            // read the network stream message
            let stream_info: rpc::NetworkStreamInfo = match deserialize(&buf[..]) {
                Ok(info) => info,
                Err(e) => {
                    let response = serialize(&rpc::ClientReturnCode::Failure)?;
                    write_msg_and_type(response.to_vec(), rpc::MessageType::Control, &mut stream)?;
                    bail!("Could not deserialize stream info: {:?}", e)
                }
            };

            // insert this stream into the client's map
            let mut map = match stream_map.0.lock() {
                Ok(m) => m,
                Err(e) => {
                    bail!("Lock is poisoned!: {:?}", e);
                }
            };
            let stream_clone = stream.try_clone()?;
            map.insert(stream_info.stream_identifier, stream_clone);

            // send a success message back to the sender saying this stream was inserted
            let response = serialize(&rpc::ClientReturnCode::Success)?;
            write_msg_and_type(response.to_vec(), rpc::MessageType::Control, &mut stream)?;
            Ok(())
        }
        _ => Ok(()),
    }
}
