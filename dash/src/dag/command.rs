use super::{node, stream, Result};
use failure::bail;
use node::SharedPipeMap;
use std::fs;
use std::net::TcpStream;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::process::{Child, Command, Stdio};
use stream::{DataStream, StreamType};
use which;

/// TODO: actually rewrite the subprocess layer from scratch
/// There's some issues happening with this one we need to debug
pub struct ShellCommandWrapper {
    command: Command,
    name: String,
    location: node::ExecutionLocation,
}

impl ShellCommandWrapper {
    pub fn new(cmd: &str, loc: node::ExecutionLocation) -> Result<Self> {
        let cmd_path = match which::which(cmd) {
            Ok(p) => p,
            Err(e) => bail!("Could not find command: {:?}", e),
        };
        Ok(ShellCommandWrapper {
            // TODO: maybe modify this or create your own execution pipeline?
            // Need to fix the fact that when you try to output to a file on the other side -- it's
            // sent to standard error
            command: Command::new(cmd_path.into_os_string()),
            name: cmd.to_string(),
            location: loc,
        })
    }

    pub fn get_name(&self) -> String {
        return self.name.clone();
    }

    // instead of setting stdin, you always want to copy stdin to the correct thing
    pub fn set_stdin(&mut self, stream: &mut DataStream, map: SharedPipeMap) -> Result<()> {
        match stream.get_type() {
            StreamType::RemoteFile => {
                unimplemented!();
            }
            StreamType::LocalFile => {
                unimplemented!();
            }
            StreamType::Pipe => {
                // TODO: fix how this is actually unlocked so we don't need to do this unwrap
                // everytime
                let mut unlocked_map = match map.lock() {
                    Ok(m) => m,
                    Err(e) => bail!("Lock is poisoned: {:?}!", e),
                };
                let prev_command = match unlocked_map.remove(&stream.name) {
                    Some(inp) => inp,
                    None => {
                        bail!("Could not find pipe fd fron prev command: {}", stream.name);
                    }
                };
                // TODO: there might be cases where you want to pipe previous stderr
                let stdout = match prev_command.stdout {
                    Some(out) => out,
                    None => bail!("Error getting prev stdout"),
                };
                self.command.stdin(stdout);
                drop(unlocked_map);
            }
            StreamType::LocalStdout => {
                // this should never be used for stdin
                unreachable!();
            }
            StreamType::NoStream => {}
        }
        Ok(())
    }

    // you just want to *copy* the stdout handle to be a stream
    // virtually the same as writing it directly to the file
    pub fn set_stdout(
        &mut self,
        stream: &mut DataStream,
        conn: &mut TcpStream,
        folder: &str,
    ) -> Result<()> {
        // LocalFile always refers to "on the client"
        // RemoteFile always refers to "on the server"
        // Whether it is a TCP connection or not is based on what the execution location of the
        // node is
        match stream.get_type() {
            StreamType::LocalFile => {
                // TODO: this isn't working
                // it somehow becomes standard error
                // Maybe this can be resolved by writing your own internal process layer
                if self.location == node::ExecutionLocation::StorageServer {
                    self.command
                        .stdout(unsafe { Stdio::from_raw_fd(conn.as_raw_fd()) });
                } else {
                    let handler = match fs::File::create(stream.get_name()) {
                        Ok(h) => h,
                        Err(e) => {
                            bail!("Could not create file {} => {:?}", stream.get_name(), e);
                        }
                    };
                    self.command.stdout(handler);
                }
            }
            StreamType::RemoteFile => {
                if self.location == node::ExecutionLocation::StorageServer {
                    let resolved_file = stream.prepend_directory(folder)?;
                    let handler = match fs::File::create(resolved_file.clone().as_str()) {
                        Ok(h) => h,
                        Err(e) => {
                            bail!("Could not create file {} => {:?}", resolved_file, e);
                        }
                    };
                    self.command.stdout(handler);
                } else {
                    // TODO: this assumes that the client has NFS access to the server
                    let handler = match fs::File::create(stream.get_name()) {
                        Ok(h) => h,
                        Err(e) => {
                            bail!("Could not create file {} => {:?}", stream.get_name(), e);
                        }
                    };
                    self.command.stdout(handler);
                }
            }
            StreamType::Pipe => {
                self.command.stdout(Stdio::piped());
            }
            StreamType::LocalStdout => {
                self.command
                    .stdout(unsafe { Stdio::from_raw_fd(conn.as_raw_fd()) });
            }
            StreamType::NoStream => {}
        }
        Ok(())
    }

    pub fn set_stderr(
        &mut self,
        stream: &mut DataStream,
        conn: &mut TcpStream,
        folder: &str,
    ) -> Result<()> {
        println!("stream: {}, {:?}", stream.get_name(), stream.get_type());
        match stream.get_type() {
            StreamType::LocalFile => {
                // TODO: also send on the file but ideally callee redirects it to a file
                // unclear how the sender here enforces that
                if self.location == node::ExecutionLocation::StorageServer {
                    self.command
                        .stdout(unsafe { Stdio::from_raw_fd(conn.as_raw_fd()) });
                } else {
                    let handler = match fs::File::create(stream.get_name()) {
                        Ok(h) => h,
                        Err(e) => {
                            bail!("Could not create file {} => {:?}", stream.get_name(), e);
                        }
                    };
                    self.command.stdout(handler);
                }
            }
            StreamType::RemoteFile => {
                if self.location == node::ExecutionLocation::StorageServer {
                    let resolved_file = stream.prepend_directory(folder)?;
                    let handler = match fs::File::create(resolved_file.clone().as_str()) {
                        Ok(h) => h,
                        Err(e) => {
                            bail!("Could not create file {} => {:?}", resolved_file, e);
                        }
                    };
                    self.command.stdout(handler);
                } else {
                    // TODO: this assumes that the client has NFS access to the server
                    let handler = match fs::File::create(stream.get_name()) {
                        Ok(h) => h,
                        Err(e) => {
                            bail!("Could not create file {} => {:?}", stream.get_name(), e);
                        }
                    };
                    self.command.stdout(handler);
                }
            }
            StreamType::Pipe => {
                self.command.stderr(Stdio::piped());
            }
            StreamType::LocalStdout => {
                self.command
                    .stderr(unsafe { Stdio::from_raw_fd(conn.as_raw_fd()) });
            }
            StreamType::NoStream => {}
        }
        Ok(())
    }

    pub fn set_arg(&mut self, arg: &str) {
        self.command.arg(arg);
    }

    pub fn spawn(&mut self) -> Result<Child> {
        match self.command.spawn() {
            Ok(ch) => Ok(ch),
            Err(e) => {
                bail!("Could not spawn: {:?}", e);
            }
        }
    }
}
