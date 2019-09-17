use super::{command, stream, Result};
use failure::bail;
use fnv::FnvHashMap as HashMap;
use serde::{Deserialize, Serialize};
use std::net::TcpStream;
use std::process::Child;
use std::sync::{Arc, Mutex};
use stream::StreamType;

// TODO: add something to the program that tells you what to execute where
// Could be more like a DAG rather than a linked list of operations
// Because you could have multiple things piping in that need to run on different parts
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Program {
    operations: Vec<Op>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum OpArg {
    Stream(stream::DataStream),
    Arg(String),
}
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub enum OpAction {
    Spawn,
    Run,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Op {
    ShellCommand {
        name: String,
        arguments: Vec<OpArg>,
        stdin: Option<stream::DataStream>, // input fd for stdin
        stdout: stream::DataStream,        // fd for stdout
        stderr: stream::DataStream,        // fd for stderr
        action: OpAction,
    },
}

impl Op {
    pub fn execute(
        &mut self,
        mut stderr_stream: TcpStream,
        mut stdout_stream: TcpStream,
        folder: &str, // "remote" folder
        debug: bool,
        map: SharedPipeMap, // keeps track of file descriptors for each pipe
    ) -> Result<()> {
        match self {
            Op::ShellCommand {
                name,
                arguments,
                stdin,
                stdout,
                stderr,
                action,
            } => {
                if debug {
                    println!("op: {:?}", &name);
                }
                // create new command obj
                let mut exec = command::ShellCommandWrapper::new(&name)?;

                // match the "input" to the command to be a previous data stream
                if let Some(inp) = stdin {
                    exec.set_stdin(inp, map.clone())?;
                }

                // pass the arguments into the command
                let mut arg_iterator = arguments.iter_mut();
                while let Some(arg) = arg_iterator.next() {
                    match arg {
                        OpArg::Stream(s) => {
                            if s.stream_type == stream::StreamType::RemoteFile {
                                let resolved_file = s.prepend_directory(folder)?;
                                exec.set_arg(resolved_file.as_str());
                            } else {
                                unreachable!();
                            }
                        }
                        OpArg::Arg(a) => {
                            println!("arg: {:?}", a.clone());
                            exec.set_arg(&a);
                        }
                    }
                }

                exec.set_stdout(stdout, &mut stdout_stream, folder)?;
                exec.set_stderr(stderr, &mut stderr_stream, folder)?;

                match action {
                    OpAction::Spawn => {
                        let child = exec.spawn()?;
                        let mut unlocked_map = match map.lock() {
                            Ok(m) => m,
                            Err(e) => bail!("Lock is poisoned: {:?}!", e),
                        };
                        if stdout.get_type() == StreamType::Pipe {
                            unlocked_map.insert(stdout.get_name(), child);
                        }
                        // TODO: figure out also how to handle the case where you direct stderr to
                        // the pipe too
                        /*if self.stderr.get_type() == StreamType::Pipe {
                            unlocked_map.insert(self.stderr.get_name(), cmd);
                        }*/
                    }
                    OpAction::Run => {
                        let mut child = exec.spawn()?;
                        child.wait().expect("error in running child");
                    }
                }
                Ok(())
            }
        }
    }
}

pub type SharedPipeMap = Arc<Mutex<HashMap<String, Child>>>;
impl Program {
    // TODO: need to figure out how to write tests for this class
    pub fn new(ops: Vec<Op>) -> Self {
        Program { operations: ops }
    }

    pub fn last_stdout(&self) -> Option<stream::DataStream> {
        let last_op = &self.operations[self.operations.len() - 1];
        match last_op {
            Op::ShellCommand { stdout, .. } => Some(stdout.clone()),
        }
    }

    // with the tcp stream
    pub fn execute(
        &mut self,
        stdout: TcpStream,
        stderr: TcpStream,
        folder: &str,
        debug: bool,
    ) -> Result<()> {
        let map = Arc::new(Mutex::new(HashMap::default()));
        let mut op_iterator = self.operations.iter_mut();
        while let Some(op) = op_iterator.next() {
            let stdout_clone = stdout.try_clone()?;
            let stderr_clone = stderr.try_clone()?;
            op.execute(
                stdout_clone,
                stderr_clone,
                folder,
                debug.clone(),
                map.clone(),
            )?;
        }
        Ok(())
    }
}
