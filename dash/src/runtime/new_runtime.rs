extern crate walkdir;
use super::graph::{filestream::FileStream, program, stream, Location};
use super::runtime_util::{new_server, Addr, Server};
use super::serialize::{read_msg_and_type, rpc, write_msg_and_type};
use super::Result;
use bincode::{deserialize, serialize};
use failure::bail;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::{fs, thread};
use stream::SharedStreamMap;
use tracing::{debug, error, info};
use walkdir::WalkDir;
/// matches client IP to folder name
pub type ClientMap = HashMap<IpAddr, String>;

/// Map from client Ip to data structure that facilitates sharing streams across threads.
type ClientStreamMap = HashMap<IpAddr, SharedStreamMap>;
/// Runtime on server that services client requests.
pub struct ServerRuntime {
    addr: Addr,
    server: TcpListener,
    client_map: ClientMap,
    client_stream_map: ClientStreamMap,
    debug: bool,
    tmp: String,
}

impl ServerRuntime {
    /// Constructs a new server runtime with the given public IP, port, and ClientMap.
    pub fn new(
        ip: &str,
        port: &str,
        client_map: ClientMap,
        debug: bool,
        tmp: &str,
    ) -> Result<Self> {
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
            tmp: tmp.to_string(),
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
                    let tmp = self.tmp.clone();
                    let addr = self.addr.clone();
                    thread::spawn(move || {
                        match handle_spawned_client(
                            s,
                            folder_result,
                            stream_map,
                            addr,
                            debug.clone(),
                            tmp,
                        ) {
                            Ok(_) => {
                                info!(
                                    "{}: Successfully handled request from {}",
                                    server_name, peer_addr
                                );
                            }
                            Err(e) => {
                                error!(
                                    "{}: Error handling request from {}: {:?}",
                                    server_name, peer_addr, e
                                );
                            }
                        }
                    });
                }
                Err(e) => {
                    if self.debug {
                        error!(
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
    mut stream_map: SharedStreamMap,
    addr: Addr,
    _debug: bool,
    tmp_folder: String,
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
            let msg: rpc::NetworkStreamInfo = match deserialize(&buf[..]) {
                Ok(info) => info,
                Err(e) => {
                    let response = serialize(&rpc::ClientReturnCode::Failure)?;
                    write_msg_and_type(response.to_vec(), rpc::MessageType::Control, &mut stream)?;
                    bail!("Error deserializing setup stream msg: {:?}", e);
                }
            };
            let connection_addr = match msg.netstream.get_receiving_side() {
                Location::Server(ip) => Addr::new(&ip, &msg.port).get_addr(),
                Location::Client => {
                    bail!("Server {:?} cannot initiate connection to the client", addr);
                }
            };

            // start a connection to another server
            let mut connection = TcpStream::connect(connection_addr)?;
            // send a pipe message to another server to setup a stream
            let netstream_info: rpc::NetworkStreamInfo = rpc::NetworkStreamInfo {
                loc: Location::Server(addr.get_ip()),
                port: msg.port.clone(),
                prog_id: msg.prog_id,
                netstream: msg.netstream.clone(),
            };
            let outermsg = serialize(&netstream_info)?;
            write_msg_and_type(outermsg.to_vec(), rpc::MessageType::Pipe, &mut connection)?;

            // wait for success:
            let (_, response_buf) = read_msg_and_type(&mut connection)?;
            let response: rpc::ClientReturnCode = deserialize(&response_buf[..])?;
            match response {
                rpc::ClientReturnCode::Success => {
                    // Send the client an ACK that it made all the pipe requests
                    let ack = serialize(&rpc::ClientReturnCode::Success)?;
                    write_msg_and_type(ack.to_vec(), rpc::MessageType::Control, &mut stream)?;

                    // save the connection in the shared map
                    connection.set_nonblocking(true)?;
                    stream_map.insert(msg.netstream, connection)?;
                    Ok(())
                }
                rpc::ClientReturnCode::Failure => {
                    // Send the client a message saying that it failed
                    let nack = serialize(&rpc::ClientReturnCode::Failure)?;
                    write_msg_and_type(nack.to_vec(), rpc::MessageType::Control, &mut stream)?;
                    bail!(
                        "Failed to setup stream to another server: {:?} on server {:?}",
                        netstream_info,
                        addr
                    );
                }
            }
            // Just setup any TCP streams
        }
        rpc::MessageType::ProgramExecution => {
            let mut program: program::Program = match deserialize(&buf[..]) {
                Ok(prog) => prog,
                Err(e) => {
                    let response = serialize(&rpc::ClientReturnCode::Failure)?;
                    write_msg_and_type(response.to_vec(), rpc::MessageType::Control, &mut stream)?;
                    bail!(
                        "Could not deserialize program: from program execution {:?}",
                        e
                    )
                }
            };

            // all the streams must be setup for this part of the program,
            // so execute the program!
            program.resolve_args(&folder)?;
            let response = match program.execute(stream_map, tmp_folder) {
                Ok(_) => serialize(&rpc::ClientReturnCode::Success)?,
                Err(e) => {
                    error!("Could not execute program because {:?}", e);
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

            // insert this stream into the shared map
            debug!("received stream: {:?}", stream_info);
            let stream_clone = stream.try_clone()?;
            stream_map.insert(stream_info.netstream, stream_clone)?;

            // send a success message back to the sender saying this stream was inserted
            let response = serialize(&rpc::ClientReturnCode::Success)?;
            write_msg_and_type(response.to_vec(), rpc::MessageType::Control, &mut stream)?;
            Ok(())
        }
        rpc::MessageType::SizeRequest => {
            let mut size_request: rpc::SizeRequest = match deserialize(&buf[..]) {
                Ok(info) => info,
                Err(e) => {
                    let response = serialize(&rpc::SizeRequest {
                        files: vec![],
                        sizes: vec![],
                        failed: true,
                    })?;
                    write_msg_and_type(
                        response.to_vec(),
                        rpc::MessageType::SizeRequest,
                        &mut stream,
                    )?;
                    bail!("Could not deserialize stream info: {:?}", e)
                }
            };

            // need to resolve and query each path
            // filestream.prepend_directory(parent_dir); -> where parent_dir is folder
            let mut sizes: Vec<(PathBuf, u64)> = Vec::new();
            for file in size_request.files.iter() {
                let mut fs = FileStream::new(&file, Location::default());
                fs.prepend_directory(&Path::new(&folder));
                let resolved = fs.get_path();
                let size = match resolved.as_path().is_dir() {
                    false => {
                        let metadata = resolved.as_path().metadata()?;
                        metadata.len()
                    }
                    true => {
                        // TODO: actually run du -sh
                        let total_size = WalkDir::new(resolved.as_path())
                            .min_depth(1)
                            .max_depth(10)
                            .into_iter()
                            .filter_map(|entry| entry.ok())
                            .filter_map(|entry| entry.metadata().ok())
                            .filter(|metadata| metadata.is_file())
                            .fold(0, |acc, m| acc + m.len());
                        total_size
                    }
                };
                sizes.push((file.clone(), size));
            }

            size_request.sizes = sizes;
            size_request.failed = false;
            let response = serialize(&size_request)?;
            write_msg_and_type(
                response.to_vec(),
                rpc::MessageType::SizeRequest,
                &mut stream,
            )?;

            Ok(())
        }
        _ => Ok(()),
    }
}
