use super::{node, stream, Result};
use failure::bail;
use node::SharedPipeMap;
use std::fs;
use std::net::TcpStream;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::process::{Child, Command, Stdio};
use stream::{DataStream, StreamType};
use which;

pub struct ShellCommandWrapper {
    command: Command,
    name: String,
}

impl ShellCommandWrapper {
    pub fn new(cmd: &str) -> Result<Self> {
        let cmd_path = match which::which(cmd) {
            Ok(p) => p,
            Err(e) => bail!("Could not find command: {:?}", e),
        };
        Ok(ShellCommandWrapper {
            command: Command::new(cmd_path.into_os_string()),
            name: cmd.to_string(),
        })
    }

    pub fn get_name(&self) -> String {
        return self.name.clone();
    }

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
        }
        Ok(())
    }
    pub fn set_stdout(
        &mut self,
        stream: &mut DataStream,
        conn: &mut TcpStream,
        folder: &str,
    ) -> Result<()> {
        println!("stream: {}, {:?}", stream.get_name(), stream.get_type());
        match stream.get_type() {
            StreamType::LocalFile => {
                // TODO: this isn't working
                // it somehow becomes standard error
                // Maybe this can be resolved by writing your own internal process layer
                self.command
                    .stdout(unsafe { Stdio::from_raw_fd(conn.as_raw_fd()) });
            }
            StreamType::RemoteFile => {
                let resolved_file = stream.prepend_directory(folder)?;
                let handler = match fs::File::create(resolved_file.clone().as_str()) {
                    Ok(h) => h,
                    Err(e) => {
                        bail!("Could not create file {} => {:?}", resolved_file, e);
                    }
                };
                self.command.stdout(handler);
            }
            StreamType::Pipe => {
                self.command.stdout(Stdio::piped());
            }
            StreamType::LocalStdout => {
                self.command
                    .stdout(unsafe { Stdio::from_raw_fd(conn.as_raw_fd()) });
            }
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
                self.command
                    .stderr(unsafe { Stdio::from_raw_fd(conn.as_raw_fd()) });
            }
            StreamType::RemoteFile => {
                let resolved_file = stream.prepend_directory(folder)?;
                let handler = match fs::File::create(resolved_file.clone().as_str()) {
                    Ok(h) => h,
                    Err(e) => {
                        bail!("Could not create file {} => {:?}", resolved_file, e);
                    }
                };
                self.command.stderr(handler);
            }
            StreamType::Pipe => {
                self.command.stderr(Stdio::piped());
            }
            StreamType::LocalStdout => {
                self.command
                    .stderr(unsafe { Stdio::from_raw_fd(conn.as_raw_fd()) });
            }
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
