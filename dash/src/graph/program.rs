use super::rapper::Rapper;
use super::{cmd, read, stream, write, Location, Result};
use failure::bail;
use serde::{Deserialize, Serialize};
use std::collections::hash_map;
use std::collections::HashMap;
use std::fmt;
use std::slice;
use std::thread;
use stream::{DashStream, IOType, NetStream, PipeStream, SharedPipeMap, SharedStreamMap};
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
    fn replace_pipe_with_net(
        &mut self,
        pipe: PipeStream,
        net: NetStream,
        iotype: IOType,
    ) -> Result<()> {
        match self {
            Elem::Write(write_node) => write_node.replace_pipe_with_net(pipe, net, iotype),
            Elem::Read(read_node) => read_node.replace_pipe_with_net(pipe, net, iotype),
            Elem::Cmd(cmd_node) => cmd_node.replace_pipe_with_net(pipe, net, iotype),
        }
    }
    fn get_stdin_len(&self) -> usize {
        match self {
            Elem::Write(write_node) => write_node.get_stdin_len(),
            Elem::Read(read_node) => read_node.get_stdin_len(),
            Elem::Cmd(cmd_node) => cmd_node.get_stdin_len(),
        }
    }

    fn get_stdout_len(&self) -> usize {
        match self {
            Elem::Write(write_node) => write_node.get_stdout_len(),
            Elem::Read(read_node) => read_node.get_stdout_len(),
            Elem::Cmd(cmd_node) => cmd_node.get_stdout_len(),
        }
    }
    fn get_stderr_len(&self) -> usize {
        match self {
            Elem::Write(write_node) => write_node.get_stderr_len(),
            Elem::Read(read_node) => read_node.get_stderr_len(),
            Elem::Cmd(cmd_node) => cmd_node.get_stderr_len(),
        }
    }

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

    fn set_loc(&mut self, loc: Location) {
        match self {
            Elem::Write(write_node) => write_node.set_loc(loc),
            Elem::Read(read_node) => read_node.set_loc(loc),
            Elem::Cmd(cmd_node) => cmd_node.set_loc(loc),
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
    /// Returns a copy of the pipestream where the left and the right are given node ids.
    /// One of left and right should be this node's id.
    /// TODO: make this function not use a copy.
    pub fn get_pipe(&self, left: NodeId, right: NodeId) -> Result<PipeStream> {
        if !(self.id == left) && !(self.id == right) {
            bail!(
                "Left or right is not this node, left: {:?}, right: {:?}, node: {:?}",
                left,
                right,
                self.id
            );
        }

        for stream in self.get_stdin() {
            match stream {
                DashStream::Pipe(pipestream) => {
                    if pipestream.get_left() == left && pipestream.get_right() == right {
                        return Ok(pipestream);
                    }
                }
                _ => {}
            }
        }

        for stream in self.get_stdout() {
            match stream {
                DashStream::Pipe(pipestream) => {
                    if pipestream.get_left() == left && pipestream.get_right() == right {
                        return Ok(pipestream);
                    }
                }
                _ => {}
            }
        }

        for stream in self.get_stderr() {
            match stream {
                DashStream::Pipe(pipestream) => {
                    if pipestream.get_left() == left && pipestream.get_right() == right {
                        return Ok(pipestream);
                    }
                }
                _ => {}
            }
        }

        bail!("Could not find stream for given link");
    }

    pub fn get_stdin_len(&self) -> usize {
        self.elem.get_stdin_len()
    }

    pub fn get_stdout_len(&self) -> usize {
        self.elem.get_stdout_len()
    }

    pub fn get_stderr_len(&self) -> usize {
        self.elem.get_stderr_len()
    }

    pub fn get_id(&self) -> NodeId {
        self.id
    }

    pub fn add_stdin(&mut self, stream: DashStream) -> Result<()> {
        self.elem.add_stdin(stream)
    }

    pub fn add_stdout(&mut self, stream: DashStream) -> Result<()> {
        self.elem.add_stdout(stream)
    }

    pub fn add_stderr(&mut self, stream: DashStream) -> Result<()> {
        self.elem.add_stderr(stream)
    }

    pub fn get_elem(&self) -> Elem {
        self.elem.clone()
    }

    pub fn get_mut_elem(&mut self) -> &mut Elem {
        &mut self.elem
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

    pub fn set_loc(&mut self, loc: Location) {
        self.elem.set_loc(loc)
    }

    pub fn resolve_args(&mut self, parent_dir: &str) -> Result<()> {
        self.elem.resolve_args(parent_dir)
    }

    pub fn replace_pipe_with_net(
        &mut self,
        pipe: PipeStream,
        net: NetStream,
        iotype: IOType,
    ) -> Result<()> {
        self.elem.replace_pipe_with_net(pipe, net, iotype)
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
#[derive(Serialize, Deserialize, PartialEq, Clone)]
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

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Copy)]
pub enum MergeDirection {
    /// Merge other into the front of this graph
    Input,
    /// Merge other into the back of this graph
    Output,
}

impl Program {
    pub fn get_id(&self) -> ProgId {
        self.id
    }

    pub fn get_mut_node(&mut self, id: NodeId) -> Option<&mut Node> {
        self.nodes.get_mut(&id)
    }

    pub fn get_mut_nodes_iter(&mut self) -> hash_map::IterMut<u32, Node> {
        self.nodes.iter_mut()
    }
    pub fn get_nodes_iter(&self) -> hash_map::Iter<u32, Node> {
        self.nodes.iter()
    }

    pub fn get_edges_iter(&self) -> slice::Iter<Link> {
        self.edges.iter()
    }

    pub fn contains(&self, id: NodeId) -> bool {
        self.nodes.contains_key(&id)
    }

    /// Creates a new program from different subgraphs.
    /// Merges them with the links provided by creating a new edge between a pair of points in
    /// different subgraphs (id0, p0), (id1, pt1) where the first tuple member is the subgraph
    /// Id, the second is the node within that subgraph.
    pub fn merge_subgraphs(
        subgraphs: HashMap<NodeId, Program>,
        existing_links: Vec<((NodeId, NodeId), (NodeId, NodeId))>,
        new_links: Vec<((NodeId, NodeId), (NodeId, NodeId))>,
    ) -> Result<Self> {
        println!("existing links: {:?}", existing_links);
        println!("new links: {:?}", new_links);
        let mut program = Program::default();
        let mut id_map: HashMap<(NodeId, NodeId), NodeId> = HashMap::default();

        for (graph_id, subgraph) in subgraphs.into_iter() {
            // add all the nodes and edges in this subgraph
            for (old_id, node) in subgraph.get_nodes_iter() {
                let new_id = program.add_elem(node.get_elem());
                id_map.insert((graph_id, *old_id), new_id);
            }
        }

        for existing_link in existing_links.iter() {
            // need to modify the pipes for these links and add edges
            let left = match id_map.get(&existing_link.0) {
                Some(id) => id,
                None => bail!("Links provided contain id not inside id_map: {:?}, -> current id_map: {:?}, links: {:?}", existing_link.0, id_map, existing_links),
            };
            let right = match id_map.get(&existing_link.1) {
                Some(id) => id,
                None => bail!(
                    "Links provided contain id not inside id_map: {:?}",
                    existing_link.1
                ),
            };

            // modify pipes
            let left_elem = program.get_mut_node(*left).unwrap().get_mut_elem();
            match left_elem {
                Elem::Read(ref mut readnode) => {
                    for stream in readnode.get_stdout_iter_mut() {
                        match stream {
                            DashStream::Pipe(ref mut pipestream) => {
                                // if the right side is the right side of the pipe -- change both
                                // pointers
                                if pipestream.get_right() == *right {
                                    pipestream.set_left(*left);
                                    pipestream.set_right(*right);
                                }
                            }
                            _ => {}
                        }
                    }
                }
                Elem::Write(ref mut _writenode) => {
                    bail!("Shouldn't be a link to another node out of a write node");
                    // NOOP (no pipes coming out of writenodes)
                }
                Elem::Cmd(ref mut cmdnode) => {
                    for stream in cmdnode.get_stdout_iter_mut() {
                        match stream {
                            DashStream::Pipe(ref mut pipestream) => {
                                // if the right side is the right side of the pipe -- change both
                                // pointers
                                if pipestream.get_right() == *right {
                                    pipestream.set_left(*left);
                                    pipestream.set_right(*right);
                                }
                            }
                            _ => {}
                        }
                    }

                    for stream in cmdnode.get_stderr_iter_mut() {
                        match stream {
                            DashStream::Pipe(ref mut pipestream) => {
                                // if the right side is the right side of the pipe -- change both
                                // pointers
                                if pipestream.get_right() == *right {
                                    pipestream.set_left(*left);
                                    pipestream.set_right(*right);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            let right_elem = program.get_mut_node(*right).unwrap().get_mut_elem();
            match right_elem {
                Elem::Read(ref mut _readnode) => {
                    bail!("Should not have pipe coming into readnode");
                }
                Elem::Write(ref mut writenode) => {
                    for stream in writenode.get_stdout_iter_mut() {
                        match stream {
                            DashStream::Pipe(ref mut pipestream) => {
                                // if the right side is the right side of the pipe -- change both
                                // pointers
                                if pipestream.get_right() == *right {
                                    pipestream.set_left(*left);
                                    pipestream.set_right(*right);
                                }
                            }
                            _ => {}
                        }
                    }
                }
                Elem::Cmd(ref mut cmdnode) => {
                    for stream in cmdnode.get_stdin_iter_mut() {
                        match stream {
                            DashStream::Pipe(ref mut pipestream) => {
                                // if the right side is the right side of the pipe -- change both
                                // pointers
                                if pipestream.get_right() == *right {
                                    pipestream.set_left(*left);
                                    pipestream.set_right(*right);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            program.add_unique_edge(*left, *right);
        }

        // add in the new connections (stdout connections)
        for new_link in new_links.iter() {
            let left = match id_map.get(&new_link.0) {
                Some(id) => id,
                None => bail!("Links provided contain id not inside id_map: {:?}, -> current id_map: {:?}, links: {:?}", new_link.0, id_map, new_links),
            };

            let right = match id_map.get(&new_link.1) {
                Some(id) => id,
                None => bail!(
                    "Links provided contain id not inside id_map: {:?}",
                    new_link.1
                ),
            };

            program.add_unique_edge(*left, *right);
            let new_pipe = PipeStream::new(*left, *right, IOType::Stdout)?;
            {
                let left_elem = program.get_mut_node(*left).unwrap();
                left_elem.add_stdout(DashStream::Pipe(new_pipe.clone()))?;
            }
            {
                let right_elem = program.get_mut_node(*right).unwrap();
                right_elem.add_stdin(DashStream::Pipe(new_pipe.clone()))?;
            }
        }
        Ok(program)
    }

    // TODO: does correctness require us to also rename streams?
    // Make sure to only call this when there are no streams.
    // But how to enforce that?
    pub fn merge(
        &mut self,
        other: Program,
        connections: Vec<(Link, MergeDirection)>,
    ) -> Result<()> {
        // keep track of the new Ids for each node
        let mut id_map: HashMap<NodeId, NodeId> = HashMap::default();
        // add in all the nodes from the other program
        for (old_id, node) in other.get_nodes_iter() {
            let new_id = self.add_elem(node.get_elem());
            id_map.insert(old_id.clone(), new_id);
        }

        // add in links from other program
        for link in other.get_edges_iter() {
            let left = id_map.get(&link.get_left()).unwrap();
            let right = id_map.get(&link.get_right()).unwrap();
            self.add_unique_edge(*left, *right);
        }

        // add in connection link
        for (link, merge_direction) in connections.iter() {
            match merge_direction {
                MergeDirection::Input => {
                    if !self.contains(link.get_right()) || !other.contains(link.get_left()) {
                        bail!(
                            "Link ends are not within either graph (InputDirection) : {:?}",
                            link
                        );
                    }
                    let new_left = id_map.get(&link.get_left()).unwrap();
                    self.add_unique_edge(*new_left, link.get_right());
                }
                MergeDirection::Output => {
                    if !self.contains(link.get_left()) || !other.contains(link.get_right()) {
                        bail!(
                            "Link ends are not within either graph (OutputDirection) : {:?}",
                            link
                        );
                    }
                    let new_right = id_map.get(&link.get_right()).unwrap();
                    self.add_unique_edge(link.get_left(), *new_right);
                }
            }
        }

        Ok(())
    }

    /// Iterates through all the edges in the program,
    /// and if any two nodes connected by an edge are not at the same location,
    /// makes the corresponding pipe a TCP stream.
    pub fn make_pipes_networked(&mut self) -> Result<()> {
        for link in self.edges.iter() {
            let left_loc = self
                .nodes
                .get(&link.get_left())
                .unwrap()
                .get_elem()
                .get_loc();
            let right_loc = self
                .nodes
                .get(&link.get_right())
                .unwrap()
                .get_elem()
                .get_loc();
            if left_loc == right_loc {
                continue;
            }

            let pipestream = self
                .nodes
                .get(&link.get_left())
                .unwrap()
                .get_pipe(link.get_left(), link.get_right())?;

            // find the corresponding pipestream
            let new_stream = NetStream::new(
                link.get_left(),
                link.get_right(),
                pipestream.get_output_type(),
                left_loc,
                right_loc,
            )?;

            // replace the pipes
            self.nodes
                .get_mut(&link.get_left())
                .unwrap()
                .replace_pipe_with_net(
                    pipestream.clone(),
                    new_stream.clone(),
                    pipestream.get_output_type(),
                )?;
            self.nodes
                .get_mut(&link.get_right())
                .unwrap()
                .replace_pipe_with_net(
                    pipestream.clone(),
                    new_stream.clone(),
                    pipestream.get_output_type(),
                )?;
        }
        Ok(())
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

    pub fn add_elem(&mut self, elem: Elem) -> NodeId {
        let node: Node = Node {
            elem: elem,
            id: self.counter + 1,
        };
        self.nodes.insert(self.counter + 1, node);
        self.counter += 1;
        self.sink_nodes.push(self.counter); // node with no dependencies is automatically a sink
        self.counter
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
        if !self.edges.contains(&Link {
            left: left,
            right: right,
        }) {
            self.edges.push(Link {
                left: left,
                right: right,
            });
            if self.sink_nodes.contains(&left) {
                self.sink_nodes.retain(|&x| x != left);
            }
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
    pub fn execution_order(&self) -> Vec<NodeId> {
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

        // First execute any commands, e.g. spawn the initial processes
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

impl fmt::Debug for Program {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // print ID
        f.pad(&format!("Program {:?}\n", self.id))?;
        // print all the nodes
        for (id, node) in self.nodes.iter() {
            f.pad(&format!("{:?}: {:?}\n", *id, node.clone()))?;
            f.pad(&format!("\n"))?;
        }
        // print all the edges:
        for link in self.edges.iter() {
            f.pad(&format!("edge: {:?}\n", link.clone()))?;
        }
        // print the sink nodes
        f.pad(&format!("sink nodes: {:?}\n", self.sink_nodes))
    }
}
