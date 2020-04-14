extern crate rand;
use bincode::{deserialize, serialize};
use dash::graph::program::Program;
use dash::graph::stream::SharedStreamMap;
use dash::graph::Location;
use dash::runtime::new_client::execute_subprogram;
use dash::runtime::runtime_util::{new_server, Addr};
use dash::serialize::{read_msg_and_type, rpc, write_msg_and_type};
use dash::util::Result;
use failure::bail;
use rand::Rng;
use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;
pub static SERVER: &str = "127.0.0.1";

struct LiteServer {
    server: TcpListener,
    connections: SharedStreamMap,
    tmp_folder: String,
}

impl LiteServer {
    // Port is unique as tests may run simultaneously.
    fn new(port: &str, tmp_folder: &str) -> Result<Self> {
        Ok(LiteServer {
            server: new_server(SERVER, port)?,
            connections: SharedStreamMap::new(),
            tmp_folder: String::from(tmp_folder),
        })
    }

    /// Loop to listen for incoming question
    fn handle_connections(&mut self) -> Result<()> {
        let clone = self.server.try_clone()?;
        for stream in clone.incoming() {
            match stream {
                Ok(s) => {
                    let done = self.handle_connection(s)?;
                    if done {
                        break;
                    } else {
                        continue;
                    }
                }
                Err(e) => {
                    println!("Error on looping on incoming streams: {:?}", e);
                }
            }
        }
        Ok(())
    }

    fn handle_connection(&mut self, mut stream: TcpStream) -> Result<bool> {
        let (msg_type, buf) = read_msg_and_type(&mut stream)?;
        match msg_type {
            rpc::MessageType::Pipe => {
                let stream_info: rpc::NetworkStreamInfo = match deserialize(&buf[..]) {
                    Ok(info) => info,
                    Err(e) => {
                        let response = serialize(&rpc::ClientReturnCode::Failure)?;
                        write_msg_and_type(
                            response.to_vec(),
                            rpc::MessageType::Control,
                            &mut stream,
                        )?;
                        bail!("Could not deserialize stream info: {:?}", e)
                    }
                };
                let stream_clone = stream.try_clone()?;
                self.connections
                    .insert(stream_info.netstream, stream_clone)?;
                // send a success message back to the sender saying this stream was inserted
                let response = serialize(&rpc::ClientReturnCode::Success)?;
                write_msg_and_type(response.to_vec(), rpc::MessageType::Control, &mut stream)?;
                // write back success on stream
                Ok(false)
            }
            rpc::MessageType::ProgramExecution => {
                let mut program: Program = match deserialize(&buf[..]) {
                    Ok(prog) => prog,
                    Err(e) => {
                        let response = serialize(&rpc::ClientReturnCode::Failure)?;
                        write_msg_and_type(
                            response.to_vec(),
                            rpc::MessageType::Control,
                            &mut stream,
                        )?;
                        bail!("Could not deserialize program: {:?}", e);
                    }
                };

                program.execute(self.connections.clone(), self.tmp_folder.clone())?;
                // send a success message back to the sender saying this stream was inserted
                let response = serialize(&rpc::ClientReturnCode::Success)?;
                write_msg_and_type(response.to_vec(), rpc::MessageType::Control, &mut stream)?;
                Ok(true)
            }
            _ => {
                unreachable!();
            }
        }
    }
}

fn run_setup(
    program_map: &mut HashMap<Location, Program>,
    shared_map: &mut SharedStreamMap,
    port: &str,
) -> Result<()> {
    for (loc, prog) in program_map.iter_mut() {
        let outward_connections = prog.get_outward_streams(loc.clone());
        for netstream in outward_connections.iter() {
            match loc {
                Location::Client => {
                    let addr = match netstream.get_receiving_side() {
                        Location::Server(ip) => Addr::new(&ip, &port).get_addr(),
                        Location::Client => {
                            bail!("From loc and to loc are both client");
                        }
                    };
                    let mut stream = TcpStream::connect(addr)?;
                    let netstream_info: rpc::NetworkStreamInfo = rpc::NetworkStreamInfo {
                        loc: Location::Client,
                        port: port.to_string(),
                        prog_id: prog.get_id(),
                        netstream: netstream.clone(),
                    };
                    let msg = serialize(&netstream_info)?;
                    write_msg_and_type(msg.to_vec(), rpc::MessageType::Pipe, &mut stream)?;
                    // wait for the success:
                    let (_, response_buf) = read_msg_and_type(&mut stream)?;
                    let response: rpc::ClientReturnCode = deserialize(&response_buf[..])?;
                    match response {
                        rpc::ClientReturnCode::Success => {}
                        rpc::ClientReturnCode::Failure => {
                            bail!("Failed to setup stream to send on server");
                        }
                    }

                    stream.set_nonblocking(true)?;
                    let clone = stream.try_clone()?;
                    shared_map.insert(netstream.clone(), clone)?;
                    drop(stream);
                }
                Location::Server(_server) => {
                    // in test, server should not be initiating any network connections
                    unreachable!();
                }
            }
        }
    }
    Ok(())
}

fn send_program(
    tmp_folder: &str,
    program_map: &mut HashMap<Location, Program>,
    shared_map: &mut SharedStreamMap,
    port: &str,
) -> Result<()> {
    let mut execution_threads: Vec<thread::JoinHandle<Result<()>>> = Vec::new();
    for (loc, prog) in program_map.iter_mut() {
        let location = loc.clone();
        let program = prog.clone();
        let shared_map_copy = shared_map.clone();
        let port = port.to_string();
        let tmp_folder = tmp_folder.to_string();
        execution_threads.push(thread::spawn(move || {
            let ret =
                execute_subprogram(location.clone(), program, shared_map_copy, port, tmp_folder);
            ret
        }));
    }

    for handle in execution_threads {
        match handle.join() {
            Ok(res) => match res {
                Ok(_) => {}
                Err(e) => {
                    bail!("One Execution thread had an error: {:?}", e);
                }
            },
            Err(e) => {
                bail!("Error in joining the execution thread: {:?}", e);
            }
        }
    }
    Ok(())
}

/// Sets up a client and server in order to execute the given program.
pub fn execute_test_program(tmp_folder: &str, program: &Program) -> Result<()> {
    let mut shared_connections = SharedStreamMap::new();
    let port = get_available_port()?;
    // start a server and run it in a separate thread
    let mut server = LiteServer::new(&port.to_string(), tmp_folder)?;
    let server_handle = thread::spawn(move || server.handle_connections());
    // split the program into portions that each node needs execute
    let mut program_map = match program.split_by_machine() {
        Ok(m) => m,
        Err(e) => {
            bail!("Could not split given program: {:?}", e);
        }
    };

    // client needs a shared stream map for handling copying standard in to nodes,
    // for the portions of the graph *it needs to execute*
    run_setup(&mut program_map, &mut shared_connections, &port.to_string())?;
    // now try to execute each portion of the program:
    send_program(
        tmp_folder,
        &mut program_map,
        &mut shared_connections,
        &port.to_string(),
    )?;

    // wait for the server to join
    match server_handle.join() {
        Ok(val) => match val {
            Ok(_) => {}
            Err(e) => {
                bail!("Server returned with error {:?}", e);
            }
        },
        Err(e) => {
            bail!("Error joining on server thread: {:?}", e);
        }
    }
    Ok(())
}

fn get_available_port() -> Result<u16> {
    let min = 8000;
    let max = 9000;
    // tests may fail if two tests use the same port
    // ideally for a big enough range, the chance that two tests pick the same port is low
    let mut rng = rand::thread_rng();
    let mut port = rng.gen_range(min, max);
    while !port_is_available(port) {
        port = rng.gen_range(min, max);
    }
    return Ok(port);
}

fn port_is_available(port: u16) -> bool {
    match TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], port))) {
        Ok(_) => true,
        Err(_) => false,
    }
}

pub fn server() -> Location {
    Location::Server("127.0.0.1".to_string())
}
