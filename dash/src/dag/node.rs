use super::{command, stream, Result};
use failure::bail;
use fnv::FnvHashMap as HashMap;
use serde::{Deserialize, Serialize};
use std::net::TcpStream;
use std::process::Child;
use std::sync::{Arc, Mutex};

/// Program is a list of operations (// TODO: later could be a DAG)
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Program {
    operations: Vec<Node>,
}

/*struct IterProgram {
    inner: &Node,
    pos: usize,
}

impl Iterator for IterProgram {
    type Item = &Node;
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.inner.len() {
            None
        } else {
            self.pos += 1;
            self.inner.get(self.pos - 1)
        }
    }
}*/

/// Where a single operation is executed
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub enum ExecutionLocation {
    StorageServer,
    Client,
}

impl Default for ExecutionLocation {
    fn default() -> Self {
        ExecutionLocation::Client
    }
}

/// If a single command is spawned (for piping to another process)
/// Or just run.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum OpAction {
    Spawn,
    Run,
}

impl Default for OpAction {
    fn default() -> Self {
        OpAction::Run
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum OpArg {
    Stream(stream::DataStream),
    Arg(String),
}

impl OpArg {
    pub fn is_local_file(&self) -> bool {
        match &*self {
            OpArg::Stream(datastream) => match datastream.get_type() {
                stream::StreamType::LocalFile => {
                    return true;
                }
                _ => {
                    return false;
                }
            },
            _ => return false,
        }
    }
}

/// Node is the execution of a single command
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Node {
    pub name: String,                // name of command to be invoked
    pub arguments: Vec<OpArg>,       // list of arguments to pass in to the command
    pub stdin: stream::DataStream,   // fd for stdin
    pub stdout: stream::DataStream,  // fd for stdout
    pub stderr: stream::DataStream,  // fd for stderr
    pub action: OpAction,            // run or spawn the command
    pub location: ExecutionLocation, // execute locally or remotely
}

impl Default for Node {
    fn default() -> Self {
        Node {
            name: Default::default(),
            arguments: vec![],
            stdin: Default::default(),
            stdout: Default::default(),
            stderr: Default::default(),
            action: Default::default(),
            location: Default::default(),
        }
    }
}

impl Node {
    /// Constructs a new Node object with the given name.
    pub fn new(name: String) -> Self {
        Node {
            name: name.clone(),
            ..Default::default()
        }
    }

    pub fn construct(
        name: String,
        arguments: Vec<OpArg>,
        stdin: stream::DataStream,
        stdout: stream::DataStream,
        stderr: stream::DataStream,
        action: OpAction,
        location: ExecutionLocation,
    ) -> Self {
        Node {
            name: name,
            arguments: arguments,
            stdin: stdin,
            stdout: stdout,
            stderr: stderr,
            action: action,
            location: location,
        }
    }

    pub fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }

    /// Adds an argument to the command's inputs
    pub fn add_arg(&mut self, arg: OpArg) {
        self.arguments.push(arg);
    }

    pub fn has_local_dependencies(&self) -> bool {
        for arg in self.arguments.iter() {
            if arg.is_local_file() {
                return true;
            }
        }
        match self.stdin.stream_type {
            stream::StreamType::LocalFile => {
                return true;
            }
            _ => {}
        }
        return false;
    }

    pub fn set_stderr(&mut self, datastream: stream::DataStream) {
        self.stderr = datastream;
    }

    pub fn set_stdout(&mut self, datastream: stream::DataStream) {
        self.stdout = datastream;
    }

    pub fn set_stdin(&mut self, datastream: stream::DataStream) {
        self.stdin = datastream;
    }

    pub fn set_location(&mut self, location: ExecutionLocation) {
        self.location = location;
    }

    pub fn set_action(&mut self, action: OpAction) {
        self.action = action;
    }

    pub fn execute(
        &mut self,
        mut stderr_stream: TcpStream,
        mut stdout_stream: TcpStream,
        folder: &str, // "remote" folder
        debug: bool,
        map: SharedPipeMap, // keeps track of file descriptors for each pipe
    ) -> Result<()> {
        if debug {
            println!("op: {:?}", &self.name);
        }
        // create new command obj
        let mut exec = command::ShellCommandWrapper::new(&self.name, self.location)?;

        // match the "input" to the command to be a previous data stream
        // No-op if streamtype is NoStream
        exec.set_stdin(&mut self.stdin, map.clone())?;

        // pass the arguments into the command
        let mut arg_iterator = self.arguments.iter_mut();
        while let Some(arg) = arg_iterator.next() {
            match arg {
                OpArg::Stream(s) => {
                    if s.stream_type == stream::StreamType::RemoteFile {
                        let resolved_file = s.prepend_directory(folder)?;
                        exec.set_arg(resolved_file.as_str());
                    } else {
                        // to execute locally on the client
                        exec.set_arg(s.get_name().as_str());
                    }
                }
                OpArg::Arg(a) => {
                    println!("arg: {:?}", a.clone());
                    exec.set_arg(&a);
                }
            }
        }

        exec.set_stdout(&mut self.stdout, &mut stdout_stream, folder)?;
        exec.set_stderr(&mut self.stderr, &mut stderr_stream, folder)?;

        match self.action {
            OpAction::Spawn => {
                let child = exec.spawn()?;
                let mut unlocked_map = match map.lock() {
                    Ok(m) => m,
                    Err(e) => bail!("Lock is poisoned: {:?}!", e),
                };
                if self.stdout.get_type() == stream::StreamType::Pipe {
                    unlocked_map.insert(self.stdout.get_name(), child);
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

pub type SharedPipeMap = Arc<Mutex<HashMap<String, Child>>>;

impl Default for Program {
    fn default() -> Self {
        Program { operations: vec![] }
    }
}
impl Program {
    // TODO: need to figure out how to write tests for this class
    pub fn new(ops: Vec<Node>) -> Self {
        Program { operations: ops }
    }

    pub fn len(&self) -> usize {
        self.operations.len()
    }

    pub fn get_mut(&mut self, ind: usize) -> Option<&mut Node> {
        if ind < self.operations.len() {
            return self.operations.get_mut(ind);
        }
        None
    }

    pub fn add_op(&mut self, op: Node) {
        self.operations.push(op);
    }

    pub fn last_stdout(&self) -> Option<stream::DataStream> {
        let last_op = &self.operations[self.operations.len() - 1];
        match last_op {
            Node { stdout, .. } => Some(stdout.clone()),
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
