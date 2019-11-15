use super::dag::{node, stream};
use super::runtime_util::new_addr;
use super::serialize::{read_msg, rpc, write_msg};
use super::Result;
use bincode::{deserialize, serialize};
use failure::bail;
use std::net::TcpStream;
use std::{fs, io, thread};
use stream::StreamType;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct ShellClient {
    addr: String,
    client_folder: String,
    server_ip: String,
}

enum OutputRedirection {
    FILE(String), // direct to a file
    STDOUT,
    STDERR,
    NONE, // don't redirect anything
}

fn output_thread(addr: String, redirection: OutputRedirection) -> Result<()> {
    // spawns a thread to listen for the stdout and stderr output for the commalet mut stream = TcpStream::connect(addr.as_ref())?;
    let mut stream = TcpStream::connect(addr.clone())?;
    match redirection {
        // TODO: this doesn't exactly work: need to fix it
        OutputRedirection::FILE(filename) => {
            let mut handle = fs::File::create(filename.clone())?;
            io::copy(&mut stream, &mut handle)
                .expect("Copying from stream into file handle failed");
        }
        OutputRedirection::STDOUT => {
            io::copy(&mut stream, &mut io::stdout()).expect("Copying from stream to stdout failed");
        }
        OutputRedirection::STDERR => {
            io::copy(&mut stream, &mut io::stderr()).expect("Copying from stream to stderr failed");
        }
        OutputRedirection::NONE => {}
    }
    Ok(())
}

impl ShellClient {
    pub fn new(server_addr: &str, server_port: &str, client_folder: &str) -> Self {
        let addr = new_addr(server_addr, server_port);
        ShellClient {
            addr: addr,
            client_folder: client_folder.to_string(),
            server_ip: server_addr.to_string(),
        }
    }

    pub fn send_request(&self, program: node::Program) -> Result<()> {
        let mut stream = TcpStream::connect(self.addr.clone())?;
        // figure out the final output redirection
        let stdout_redirection = match program.last_stdout() {
            Some(s) => match s.get_type() {
                StreamType::LocalFile => {
                    let loc = match s.prepend_directory(&self.client_folder) {
                        Ok(l) => l,
                        Err(e) => {
                            bail!("Could not get local file address: {:?}", e);
                        }
                    };
                    OutputRedirection::FILE(loc)
                }
                StreamType::RemoteFile => OutputRedirection::NONE,
                StreamType::LocalStdout => OutputRedirection::STDOUT,
                _ => OutputRedirection::NONE,
            },
            None => OutputRedirection::NONE,
        };

        let stderr_redirection = OutputRedirection::STDERR;
        // 1: send the program to the server
        let encoded_program: Vec<u8> = serialize(&program)?;
        write_msg(encoded_program, &mut stream)?;

        // 2: server should send back ports for STDOUT and STDERR output
        let response_bytes = read_msg(&mut stream)?;
        let stream_setup: rpc::StreamSetupMsg = deserialize(&response_bytes[..])?;
        let stdout_addr = new_addr(&self.server_ip, &stream_setup.stdout_port.to_string());
        let stderr_addr = new_addr(&self.server_ip, &stream_setup.stderr_port.to_string());

        // 3: spawn thread to listen for stdout and stderr output
        let stdout_handle: thread::JoinHandle<Result<()>> =
            thread::spawn(move || output_thread(stdout_addr, stdout_redirection));
        let stderr_handle: thread::JoinHandle<Result<()>> =
            thread::spawn(move || output_thread(stderr_addr, stderr_redirection));

        // TODO: listen on the control loop for messages (to send resources over)
        // multiple streams
        //
        //
        // 4: wait for the stdout and stderr handles to return
        let _ = stdout_handle.join();
        let _ = stderr_handle.join();

        // 5: wait for successful completion of the program
        let next_msg = read_msg(&mut stream)?;
        let response: rpc::ClientReturnCode = deserialize(&next_msg[..])?;
        match response {
            rpc::ClientReturnCode::Success => Ok(()),
            rpc::ClientReturnCode::Failure => {
                bail!("Program failed to execute on server");
            }
        }
    }
}
