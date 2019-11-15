use super::graph::{program, stream, Location};
use super::runtime_util::Addr;
use super::serialize::{read_msg_and_type, rpc, write_msg_and_type};
use super::Result;
use bincode::{deserialize, serialize};
use failure::bail;
use std::collections::HashMap;
use std::net::TcpStream;
use std::thread;
use stream::{SharedStreamMap, StreamIdentifier};
use thread::JoinHandle;

pub type MountMap = HashMap<Addr, String>;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct ShellClient {
    /// Map from client folders to addresses
    mount_map: MountMap,
    /// Server port
    port: String,
}

impl ShellClient {
    pub fn new(server_port: &str) -> Self {
        let mount_map: HashMap<Addr, String> = HashMap::default();
        ShellClient {
            mount_map: mount_map,
            port: server_port.to_string(),
        }
    }

    /// Runs the setup portion of the command.
    fn run_setup(
        &self,
        program_map: &mut HashMap<Location, program::Program>,
        shared_map: &mut SharedStreamMap,
    ) -> Result<()> {
        let mut setup_threads: Vec<JoinHandle<Result<()>>> = Vec::new();
        // 1: wait for all the servers to setup their connections
        for (loc, prog) in program_map.iter_mut() {
            // get all the connections (e.g., stream identifiers) this part of the graph should
            // initiate
            let outward_connections = prog.get_outward_streams(loc.clone());
            for (to_loc, stream_identifier) in outward_connections.iter() {
                let map_clone = shared_map.clone();
                let prog_id = prog.get_id();
                let to_loc_clone = to_loc.clone();
                let stream_identifier_clone = stream_identifier.clone();
                let port = self.port.clone();
                setup_threads.push(match loc.clone() {
                    Location::Client => thread::spawn(move || {
                        run_stream_setup(
                            Location::Client,
                            to_loc_clone,
                            stream_identifier_clone,
                            port,
                            map_clone,
                            prog_id,
                        )
                    }),
                    Location::Server(ip) => thread::spawn(move || {
                        run_stream_setup(
                            Location::Server(ip.to_string()),
                            to_loc_clone,
                            stream_identifier_clone,
                            port,
                            map_clone,
                            prog_id,
                        )
                    }),
                });
            }
        }

        // When all these setup threads are joined,
        // safe to start executing the program.
        for handle in setup_threads {
            match handle.join() {
                Ok(res) => match res {
                    Ok(_) => {}
                    Err(e) => {
                        bail!("One thread had an error: {:?}", e);
                    }
                },
                Err(e) => {
                    bail!("Error in joining the setup threadi: {:?}", e);
                }
            }
        }

        Ok(())
    }

    fn send_program(
        &self,
        program_map: &mut HashMap<Location, program::Program>,
        shared_map: &mut SharedStreamMap,
    ) -> Result<()> {
        let mut execution_threads: Vec<JoinHandle<Result<()>>> = Vec::new();
        for (loc, prog) in program_map.iter_mut() {
            let location = loc.clone();
            let program = prog.clone();
            let shared_map_copy = shared_map.clone();
            let port = self.port.clone();
            execution_threads.push(thread::spawn(move || {
                execute_subprogram(location, program, shared_map_copy, port)
            }));
        }

        for handle in execution_threads {
            match handle.join() {
                Ok(res) => match res {
                    Ok(_) => {}
                    Err(e) => {
                        bail!("One execution thread had an error: {:?}", e);
                    }
                },
                Err(e) => {
                    bail!("Error in joining the execution thread: {:?}", e);
                }
            }
        }
        Ok(())
    }

    /// Executes the given program by offloading the relevant nodes to the correct machines.
    pub fn run_command(&self, program: program::Program) -> Result<()> {
        // split the program into portions that each node needs execute
        let mut program_map = match program.split_by_machine() {
            Ok(m) => m,
            Err(e) => {
                bail!("Could not split given program: {:?}", e);
            }
        };

        // client needs a shared stream map for handling copying standard in to nodes,
        // for the portions of the graph *it needs to execute*
        let mut shared_map = SharedStreamMap::new();
        self.run_setup(&mut program_map, &mut shared_map)?;

        // now try to execute each portion of the program:
        self.send_program(&mut program_map, &mut shared_map)?;
        Ok(())
    }
}

/// Makes open stream requests
/// from_loc: server to initiate the pipe message
/// to_loc: server to receive the pipe message
/// stream_identifier: Information about the stream metadata
/// port: Port on which client sends messages to the servers
/// map: SharedStreamMap - client will need to insert the resulting streams into a map in order to
/// later use them when executing the client's portion of the program
fn run_stream_setup(
    from_loc: Location,
    to_loc: Location,
    stream_identifier: StreamIdentifier,
    port: String,
    map: SharedStreamMap,
    prog_id: program::ProgId,
) -> Result<()> {
    match from_loc {
        Location::Client => {
            let addr = match to_loc {
                Location::Server(ip) => Addr::new(&ip, &port).get_addr(),
                Location::Client => {
                    bail!("From loc and to loc are client");
                }
            };
            let mut stream = TcpStream::connect(addr)?;
            // send a stream connection message
            // TODO: do we need to convert the stream_identifier in anyway?
            let stream_info: rpc::NetworkStreamInfo = rpc::NetworkStreamInfo {
                loc: Location::Client,
                prog_id: prog_id,
                stream_identifier: stream_identifier.clone(),
            };
            let msg = serialize(&stream_info)?;
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

            // the client thread that runs the programs needs access to these streams as well
            let mut unlocked_map = match map.0.lock() {
                Ok(m) => m,
                Err(e) => {
                    bail!("Lock to insert stream was poisoned!i: {:?}", e);
                }
            };
            // clone the stream and keep a copy in the shared map for the client to later use
            // during execution
            let clone = stream.try_clone()?;
            unlocked_map.insert(stream_identifier.clone(), clone);
            Ok(())
        }
        Location::Server(ip) => {
            let addr = Addr::new(&ip, &port).get_addr();
            let mut stream = TcpStream::connect(addr)?;
            let info = rpc::NetworkStreamInfo {
                loc: to_loc.clone(),
                prog_id: prog_id,
                stream_identifier: stream_identifier.clone(),
            };
            let message = serialize(&info)?;
            write_msg_and_type(
                message.to_vec(),
                rpc::MessageType::SetupStreams,
                &mut stream,
            )?;
            let (_, next_msg) = read_msg_and_type(&mut stream)?;
            let msg = deserialize(&next_msg[..])?;
            match msg {
                rpc::ClientReturnCode::Success => Ok(()),
                rpc::ClientReturnCode::Failure => {
                    bail!("Server returned failure to open new stream")
                }
            }
        }
    }
}

/// Executes a subprogram by either:
/// executing the program on the client,
/// or executing the program on the server and waiting
/// for the results.
/// loc: Location -> location of this subprogram.
/// program: Program -> subprogram to be executed.
/// shared_map: SharedStreamMap: handle for map with client's subprogram TCP streams.
/// port: String -> port that server is listening to
fn execute_subprogram(
    loc: Location,
    mut prog: program::Program,
    shared_stream_map: SharedStreamMap,
    port: String,
) -> Result<()> {
    match loc {
        Location::Client => {
            // execute the subprogram
            prog.execute(shared_stream_map)
        }
        Location::Server(ip) => {
            // send a request to the server to execute this subprogram
            let addr = Addr::new(&ip, &port).get_addr();
            let mut stream = TcpStream::connect(addr)?;
            let message = serialize(&prog)?;
            write_msg_and_type(
                message.to_vec(),
                rpc::MessageType::SetupStreams,
                &mut stream,
            )?;
            let (_, next_msg) = read_msg_and_type(&mut stream)?;
            let msg = deserialize(&next_msg[..])?;
            match msg {
                rpc::ClientReturnCode::Success => Ok(()),
                rpc::ClientReturnCode::Failure => {
                    bail!("Server returned failure to execute program.")
                }
            }
        }
    }
}
