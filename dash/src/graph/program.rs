use super::rapper::Rapper;
use super::{cmd, read, stream, write, Location, Result};
use failure::bail;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::thread;
use stream::{DashStream, IOType, NetStream, SharedPipeMap, SharedStreamMap};
use thread::{spawn, JoinHandle};

pub type NodeId = u32;
pub type ProgId = u32;

/// Elements can be read, write, or command nodes
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Elem {
    Write(write::WriteNode),
    Read(read::ReadNode),
    Cmd(cmd::CommandNode),
}

impl Rapper for Elem {
    fn get_stdin(&self) -> Vec<DashStream> {
        match self {
            Elem::Write(write_node) => write_node.get_stdin(),
            Elem::Read(read_node) => read_node.get_stdin(),
            Elem::Cmd(cmd_node) => cmd_node.get_stdin(),
        }
    }

    fn get_stdout(&self) -> Vec<DashStream> {
        match self {
            Elem::Write(write_node) => write_node.get_stdin(),
            Elem::Read(read_node) => read_node.get_stdin(),
            Elem::Cmd(cmd_node) => cmd_node.get_stdin(),
        }
    }

    fn get_stderr(&self) -> Vec<DashStream> {
        match self {
            Elem::Write(write_node) => write_node.get_stdin(),
            Elem::Read(read_node) => read_node.get_stdin(),
            Elem::Cmd(cmd_node) => cmd_node.get_stdin(),
        }
    }

    fn add_stdin(&mut self, stream: DashStream) -> Result<()> {
        match self {
            Elem::Write(write_node) => write_node.add_stdin(stream),
            Elem::Read(read_node) => read_node.add_stdin(stream),
            Elem::Cmd(cmd_node) => cmd_node.add_stdin(stream),
        }
    }

    fn add_stdout(&mut self, stream: DashStream) -> Result<()> {
        match self {
            Elem::Write(write_node) => write_node.add_stdout(stream),
            Elem::Read(read_node) => read_node.add_stdout(stream),
            Elem::Cmd(cmd_node) => cmd_node.add_stdout(stream),
        }
    }

    fn add_stderr(&mut self, stream: DashStream) -> Result<()> {
        match self {
            Elem::Write(write_node) => write_node.add_stderr(stream),
            Elem::Read(read_node) => read_node.add_stderr(stream),
            Elem::Cmd(cmd_node) => cmd_node.add_stderr(stream),
        }
    }

    fn execute(
        &mut self,
        pipes: SharedPipeMap,
        network_connections: SharedStreamMap,
    ) -> Result<()> {
        match self {
            Elem::Write(write_node) => write_node.execute(pipes, network_connections),
            Elem::Read(read_node) => read_node.execute(pipes, network_connections),
            Elem::Cmd(cmd_node) => cmd_node.execute(pipes, network_connections),
        }
    }

    fn get_outward_streams(&self, iotype: stream::IOType, is_server: bool) -> Vec<NetStream> {
        match self {
            Elem::Write(write_node) => write_node.get_outward_streams(iotype, is_server),
            Elem::Read(read_node) => read_node.get_outward_streams(iotype, is_server),
            Elem::Cmd(cmd_node) => cmd_node.get_outward_streams(iotype, is_server),
        }
    }

    fn get_loc(&self) -> Location {
        match self {
            Elem::Write(write_node) => write_node.get_loc(),
            Elem::Read(read_node) => read_node.get_loc(),
            Elem::Cmd(cmd_node) => cmd_node.get_loc(),
        }
    }

    fn run_redirection(
        &mut self,
        pipes: SharedPipeMap,
        network_connections: SharedStreamMap,
    ) -> Result<()> {
        match self {
            Elem::Write(write_node) => write_node.run_redirection(pipes, network_connections),
            Elem::Read(read_node) => read_node.run_redirection(pipes, network_connections),
            Elem::Cmd(cmd_node) => cmd_node.run_redirection(pipes, network_connections),
        }
    }

    fn resolve_args(&mut self, parent_dir: &str) -> Result<()> {
        match self {
            Elem::Write(write_node) => write_node.resolve_args(parent_dir),
            Elem::Read(read_node) => read_node.resolve_args(parent_dir),
            Elem::Cmd(cmd_node) => cmd_node.resolve_args(parent_dir),
        }
    }
}

/// A single execution node in the graph
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Node {
    /// data structure that implements the rapper trait
    elem: Elem,
    /// id number
    id: NodeId,
}

impl Node {
    pub fn get_id(&self) -> NodeId {
        self.id
    }

    pub fn get_stdin(&self) -> Vec<DashStream> {
        self.elem.get_stdin()
    }

    pub fn get_stdout(&self) -> Vec<DashStream> {
        self.elem.get_stdout()
    }

    pub fn get_stderr(&self) -> Vec<DashStream> {
        self.elem.get_stderr()
    }

    pub fn get_outward_streams(&self, iotype: stream::IOType, is_server: bool) -> Vec<NetStream> {
        self.elem.get_outward_streams(iotype, is_server)
    }

    pub fn run_redirection(
        &mut self,
        pipes: SharedPipeMap,
        network_connections: SharedStreamMap,
    ) -> Result<()> {
        self.elem.run_redirection(pipes, network_connections)
    }

    pub fn execute(
        &mut self,
        pipes: SharedPipeMap,
        network_connections: SharedStreamMap,
    ) -> Result<()> {
        self.elem.execute(pipes, network_connections)
    }

    pub fn get_loc(&self) -> Location {
        self.elem.get_loc()
    }

    pub fn resolve_args(&mut self, parent_dir: &str) -> Result<()> {
        self.elem.resolve_args(parent_dir)
    }
}

/// One sided edges in the program graph
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Link {
    /// Left edge node id
    left: NodeId,
    /// Right edge node id
    right: NodeId,
}

impl Link {
    pub fn get_left(&self) -> NodeId {
        self.left
    }

    pub fn get_right(&self) -> NodeId {
        self.right
    }
}

/// Program represents a group of nodes linked together.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Program {
    id: ProgId,
    nodes: HashMap<u32, Node>,
    edges: Vec<Link>,
    counter: u32,
    sink_nodes: Vec<NodeId>,
}

impl Default for Program {
    fn default() -> Self {
        let map: HashMap<u32, Node> = Default::default();
        Program {
            id: 0,
            nodes: map,
            edges: vec![],
            counter: 0,
            sink_nodes: vec![],
        }
    }
}

impl Program {
    pub fn get_id(&self) -> ProgId {
        self.id
    }

    /// Splits the program into different sub-graphs that need to be executed on different
    /// machines.
    /// Makes sure to preserve the nodeIds.
    pub fn split_by_machine(&self) -> Result<HashMap<Location, Program>> {
        let mut map: HashMap<Location, Program> = HashMap::default();

        // add all the nodes to each subprogram by location
        for (_, node) in self.nodes.iter() {
            let location = node.get_loc();
            match map.get_mut(&location) {
                Some(prog) => {
                    prog.add_unique_node(node.clone());
                }
                None => {
                    let mut prog = Program::default();
                    prog.add_unique_node(node.clone());
                    map.insert(location.clone(), prog);
                }
            }
        }

        // add all the relevant edges to each subprogram
        for (_, prog) in map.iter_mut() {
            let nodes = prog.get_nodes();
            let edges = self.find_contained_edges(nodes);
            for edge in edges.iter() {
                prog.add_unique_edge(edge.get_left(), edge.get_right());
            }
        }

        Ok(map)
    }

    pub fn get_nodes(&self) -> Vec<NodeId> {
        self.nodes.iter().map(|(k, _)| k).cloned().collect()
    }

    pub fn add_elem(&mut self, elem: Elem) {
        let node: Node = Node {
            elem: elem,
            id: self.counter + 1,
        };
        self.nodes.insert(self.counter + 1, node);
        self.counter += 1;
        self.sink_nodes.push(self.counter); // node with no dependencies is automatically a sink
    }

    pub fn add_unique_node(&mut self, node: Node) {
        let mut is_connected = false;
        for edge in self.edges.iter() {
            if edge.get_left() == node.get_id() {
                is_connected = true;
            }
        }
        if !is_connected {
            self.sink_nodes.push(node.get_id());
        }
        self.nodes.insert(node.get_id(), node);
    }

    /// Finds the subset of edges contained within this subcomponent of the graph.
    /// Doesn't include edges that go out of the subcomponent
    pub fn find_contained_edges(&self, nodes: Vec<NodeId>) -> Vec<Link> {
        self.edges
            .iter()
            .filter(|edge| nodes.contains(&edge.get_left()) && nodes.contains(&edge.get_right()))
            .cloned()
            .collect()
    }

    pub fn add_unique_edge(&mut self, left: NodeId, right: NodeId) {
        self.edges.push(Link {
            left: left,
            right: right,
        });
        if self.sink_nodes.contains(&left) {
            self.sink_nodes.retain(|&x| x != left);
        }
    }

    pub fn add_edge(&mut self, left: &Node, right: &Node) {
        self.edges.push(Link {
            left: left.get_id(),
            right: right.get_id(),
        });
        if self.sink_nodes.contains(&left.get_id()) {
            self.sink_nodes.retain(|&x| x != left.get_id());
        }
    }

    /// Adds an entire pipeline of commands (e.g., a single line of commands connected to each
    /// other)
    pub fn add_pipeline(&mut self, elems: Vec<Elem>) {
        let mut last_node_id: Option<u32> = None;
        for elem in elems {
            let node: Node = Node {
                elem: elem,
                id: self.counter + 1,
            };
            self.nodes.insert(self.counter + 1, node);
            self.counter += 1;
            match last_node_id {
                Some(id) => self.edges.push(Link {
                    left: id,
                    right: self.counter,
                }),
                None => {}
            }
            last_node_id = Some(self.counter);
        }
        self.sink_nodes.push(last_node_id.unwrap()); // safe to append the last line in this vector
    }

    /// finds the dependent n
    fn find_dependent_nodes(&self, node_id: NodeId) -> Vec<NodeId> {
        self.edges
            .iter()
            .filter(|&link| link.get_right() == node_id)
            .map(|link| link.get_left())
            .collect()
    }

    /// Finds an execution order for the nodes
    /// All dependencies for a sink need to be executed before a sink
    /// So maybe traverse from the sinks backwards (insert into a list)
    fn execution_order(&self) -> Vec<NodeId> {
        let mut path: Vec<NodeId> = Vec::new();
        for node_id in self.sink_nodes.iter() {
            // find the dependent nodes
            let dependent_nodes = self.find_dependent_nodes(*node_id);
            for dependence in dependent_nodes.iter() {
                if !(path.contains(dependence)) {
                    path.insert(0, *dependence);
                }
            }
        }
        path
    }

    /// Resolves all the nodes in this program with the given folder.
    pub fn resolve_args(&mut self, folder: &str) -> Result<()> {
        for (_, node) in self.nodes.iter_mut() {
            node.resolve_args(folder)?;
        }
        Ok(())
    }

    /// Executes a program on the current server.
    /// stream_map: SharedStreamMap that contains handles to any tcp streams needed by any nodes to
    /// execute.
    /// when executing the node. Note that if it's a client, folder should be none; no filepaths
    /// need to be resolved.
    pub fn execute(&mut self, stream_map: SharedStreamMap) -> Result<()> {
        let pipe_map = SharedPipeMap::new();
        let execution_order = self.execution_order();
        let mut node_threads: Vec<JoinHandle<Result<()>>> = Vec::new();
        // First execute any commands
        for node_id in execution_order.iter() {
            let node = match self.nodes.get_mut(node_id) {
                Some(n) => n,
                None => bail!(
                    "Execution order produced node_id {:?} not in node map",
                    node_id
                ),
            };
            let pipe_map_copy = pipe_map.clone();
            let stream_map_copy = stream_map.clone();
            let mut node_clone = node.clone();
            // This call is non-blocking
            node_clone.execute(pipe_map_copy, stream_map_copy)?;
        }

        // Next, loop over and run redirection commands
        for node_id in execution_order.iter() {
            let node = match self.nodes.get_mut(node_id) {
                Some(n) => n,
                None => bail!(
                    "Execution order produced node_id {:?} not in node_map",
                    node_id
                ),
            };
            let pipe_map_copy = pipe_map.clone();
            let stream_map_copy = stream_map.clone();
            let mut node_clone = node.clone();
            // This call is non-blocking
            node_threads.push(spawn(move || {
                node_clone.run_redirection(pipe_map_copy, stream_map_copy)
            }));
        }

        // Join all the threads to make sure it worked
        // TODO: is this too many threads?
        for thread in node_threads {
            match thread.join() {
                Ok(res) => match res {
                    Ok(_) => {}
                    Err(e) => {
                        bail!("Node failed to execute: {:?}", e);
                    }
                },
                Err(e) => {
                    bail!("Thread failed to join!: {:?}", e);
                }
            }
        }

        Ok(())
    }

    /// Returns a list of outward streams this server should initiate
    /// An outward stream is sending stdout or stderr to another node.
    /// If location is the client, outward stream is ANY connection to another node (either sending
    /// stdout or stderr or receiving stdin from any other node).
    /// For other servers, outward streams are stdout/stderr connections to other nodes not
    /// involving the client.
    pub fn get_outward_streams(&self, loc: Location) -> Vec<NetStream> {
        let mut ret: Vec<NetStream> = Vec::new();
        match loc {
            Location::Client => {
                for (_id, node) in self.nodes.iter() {
                    // check for any streams that connect to the network
                    ret.append(&mut node.get_outward_streams(IOType::Stdin, false));
                    ret.append(&mut node.get_outward_streams(IOType::Stdout, false));
                    ret.append(&mut node.get_outward_streams(IOType::Stderr, false));
                }
            }
            Location::Server(_) => {
                for (_id, node) in self.nodes.iter() {
                    ret.append(&mut node.get_outward_streams(IOType::Stdout, true));
                    ret.append(&mut node.get_outward_streams(IOType::Stderr, true));
                }
            }
        }
        ret
    }
}
