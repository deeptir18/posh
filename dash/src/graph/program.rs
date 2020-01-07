use super::rapper::Rapper;
use super::{cmd, read, stream, write, Location, Result};
use failure::bail;
use serde::{Deserialize, Serialize};
use std::collections::hash_map;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::slice;
use std::thread;
use stream::{DashStream, IOType, NetStream, PipeStream, SharedPipeMap, SharedStreamMap};
use thread::{spawn, JoinHandle};
pub type NodeId = u32;
pub type ProgId = u32;
use std::io::Write;

/// Elements can be read, write, or command nodes
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Elem {
    Write(write::WriteNode),
    Read(read::ReadNode),
    Cmd(cmd::CommandNode),
}

impl Into<Option<cmd::CommandNode>> for Elem {
    fn into(self) -> Option<cmd::CommandNode> {
        match self {
            Elem::Cmd(cmd) => Some(cmd),
            _ => None,
        }
    }
}

impl Rapper for Elem {
    fn get_stdout_id(&self) -> Option<NodeId> {
        match self {
            Elem::Write(write_node) => write_node.get_stdout_id(),
            Elem::Read(read_node) => read_node.get_stdout_id(),
            Elem::Cmd(cmd_node) => cmd_node.get_stdout_id(),
        }
    }

    fn replace_stream_edges(&mut self, edge: Link, new_edges: Vec<Link>) -> Result<()> {
        match self {
            Elem::Write(write_node) => write_node.replace_stream_edges(edge, new_edges),
            Elem::Read(read_node) => read_node.replace_stream_edges(edge, new_edges),
            Elem::Cmd(cmd_node) => cmd_node.replace_stream_edges(edge, new_edges),
        }
    }
    fn set_id(&mut self, id: NodeId) {
        match self {
            Elem::Write(write_node) => write_node.set_id(id),
            Elem::Read(read_node) => read_node.set_id(id),
            Elem::Cmd(cmd_node) => cmd_node.set_id(id),
        }
    }
    fn get_id(&self) -> NodeId {
        match self {
            Elem::Write(write_node) => write_node.get_id(),
            Elem::Read(read_node) => read_node.get_id(),
            Elem::Cmd(cmd_node) => cmd_node.get_id(),
        }
    }
    /// Turns an element (graph node) into dot format with the correct features.
    fn get_dot_label(&self) -> Result<String> {
        match self {
            Elem::Write(write_node) => {
                let label = write_node.get_dot_label()?;
                let node = format!("[shape=diamond label={:?}]", label);
                Ok(node)
            }
            Elem::Read(read_node) => {
                let label = read_node.get_dot_label()?;
                let node = format!("[shape=diamond label={:?}]", label);
                Ok(node)
            }
            Elem::Cmd(cmd_node) => {
                let label = cmd_node.get_dot_label()?;
                let node = format!("[shape=oval label={:?}]", label);
                Ok(node)
            }
        }
    }
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
            Elem::Write(write_node) => write_node.get_stdout(),
            Elem::Read(read_node) => read_node.get_stdout(),
            Elem::Cmd(cmd_node) => cmd_node.get_stdout(),
        }
    }

    fn get_stderr(&self) -> Vec<DashStream> {
        match self {
            Elem::Write(write_node) => write_node.get_stderr(),
            Elem::Read(read_node) => read_node.get_stderr(),
            Elem::Cmd(cmd_node) => cmd_node.get_stderr(),
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
        tmp_folder: String,
    ) -> Result<()> {
        match self {
            Elem::Write(write_node) => {
                write_node.run_redirection(pipes, network_connections, tmp_folder)
            }
            Elem::Read(read_node) => {
                read_node.run_redirection(pipes, network_connections, tmp_folder)
            }
            Elem::Cmd(cmd_node) => cmd_node.run_redirection(pipes, network_connections, tmp_folder),
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
    pub fn get_dot_label(&self) -> Result<String> {
        let label = self.elem.get_dot_label()?;
        Ok(format!("{:?} {}", self.id, label))
    }

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
        tmp_folder: String,
    ) -> Result<()> {
        self.elem
            .run_redirection(pipes, network_connections, tmp_folder)
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

    pub fn clear_stdin(&mut self) {
        match self.elem {
            Elem::Cmd(ref mut cmdnode) => {
                cmdnode.clear_stdin();
            }
            _ => unreachable!(),
        }
    }

    pub fn replace_stream_edges(&mut self, edge: Link, new_edges: Vec<Link>) -> Result<()> {
        self.elem.replace_stream_edges(edge, new_edges)
    }

    pub fn get_stdout_id(&self) -> Option<NodeId> {
        self.elem.get_stdout_id()
    }
}

/// One sided edges in the program graph
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Hash, Eq)]
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

    pub fn get_dot_label(&self) -> Result<String> {
        Ok(format!(
            "{:?} -> {:?} [style=dashed,color=grey]",
            self.left, self.right
        ))
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
    source_nodes: Vec<NodeId>,
    pub pwd: stream::FileStream,
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
            source_nodes: vec![],
            pwd: stream::FileStream::default(),
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
    pub fn set_pwd(&mut self, fs: stream::FileStream) {
        self.pwd = fs;
    }
    pub fn write_dot(&self, filename: &str) -> Result<()> {
        let mut file = File::create(filename)?;
        file.write_all(b"digraph {\n")?;
        // write each node and defining characteristics
        for (_, node) in self.nodes.iter() {
            let label = node.get_dot_label()?;
            file.write_fmt(format_args!("{}\n", label))?;
        }

        // write all the *graph* edges
        for edge in self.edges.iter() {
            let label = edge.get_dot_label()?;
            file.write_fmt(format_args!("{}\n", label))?;
        }

        // figure out the list of pipes and where they come from
        let mut pipe_map: HashMap<PipeStream, (NodeId, NodeId)> = HashMap::default();
        let mut net_map: HashMap<NetStream, (NodeId, NodeId)> = HashMap::default();

        // now write in all pipestreams and netstreams as separate nodes, and separate edges
        for (_, node) in self.nodes.iter() {
            // iterate through stdout, stderr
            for stream in node.get_stdout() {
                match stream {
                    DashStream::Pipe(ps) => match pipe_map.get_mut(&ps) {
                        Some(mut info) => {
                            info.0 = node.get_id();
                        }
                        None => {
                            pipe_map.insert(ps.clone(), (node.get_id(), 0));
                        }
                    },
                    DashStream::Tcp(ns) => match net_map.get_mut(&ns) {
                        Some(info) => {
                            info.0 = node.get_id();
                        }
                        None => {
                            net_map.insert(ns.clone(), (node.get_id(), 0));
                        }
                    },
                    _ => {}
                }
            }
            for stream in node.get_stderr() {
                match stream {
                    DashStream::Pipe(ps) => match pipe_map.get_mut(&ps) {
                        Some(mut info) => {
                            info.0 = node.get_id();
                        }
                        None => {
                            pipe_map.insert(ps.clone(), (node.get_id(), 0));
                        }
                    },
                    DashStream::Tcp(ns) => match net_map.get_mut(&ns) {
                        Some(mut info) => {
                            info.0 = node.get_id();
                        }
                        None => {
                            net_map.insert(ns.clone(), (node.get_id(), 0));
                        }
                    },
                    _ => {}
                }
            }
            for stream in node.get_stdin() {
                match stream {
                    DashStream::Pipe(ps) => match pipe_map.get_mut(&ps) {
                        Some(mut info) => {
                            info.1 = node.get_id();
                        }
                        None => {
                            pipe_map.insert(ps.clone(), (0, node.get_id()));
                        }
                    },
                    DashStream::Tcp(ns) => match net_map.get_mut(&ns) {
                        Some(mut info) => {
                            info.1 = node.get_id();
                        }
                        None => {
                            net_map.insert(ns.clone(), (0, node.get_id()));
                        }
                    },
                    _ => {}
                }
            }
        }
        let mut new_counter = self.counter + 1;
        // now iterate over all pipes, adding nodes
        for (ps, endpoints) in pipe_map.iter() {
            file.write_fmt(format_args!(
                "{:?} [shape=box,label={:?}]\n",
                new_counter,
                ps.get_dot_label()
            ))?;
            let left = endpoints.0;
            let right = endpoints.1;
            if left == 0 && right == 0 {
                bail!("Pipe with both left and right not found!");
            }
            if left == 0 {
                file.write_fmt(format_args!("{} -> {}\n", new_counter, right))?;
            } else if right == 0 {
                file.write_fmt(format_args!("{} -> {}\n", left, new_counter))?;
            } else {
                file.write_fmt(format_args!("{} -> {} -> {}\n", left, new_counter, right))?;
            }
            new_counter += 1;
        }

        for (ns, endpoints) in net_map.iter() {
            file.write_fmt(format_args!(
                "{:?} [shape=box,label={:?}]\n",
                new_counter,
                ns.get_dot_label()
            ))?;
            let left = endpoints.0;
            let right = endpoints.1;
            if left == 0 && right == 0 {
                bail!("Pipe with both left and right not found!");
            }
            if left == 0 {
                file.write_fmt(format_args!("{} -> {}\n", new_counter, right))?;
            } else if right == 0 {
                file.write_fmt(format_args!("{} -> {}\n", left, new_counter))?;
            } else {
                file.write_fmt(format_args!("{} -> {} -> {}\n", left, new_counter, right))?;
            }
            new_counter += 1;
        }

        file.write_all(b"}")?;
        Ok(())
    }

    pub fn get_id(&self) -> ProgId {
        self.id
    }

    pub fn get_node(&self, id: NodeId) -> Option<Node> {
        match self.nodes.get(&id) {
            None => None,
            Some(node) => Some(node.clone()),
        }
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

    /// Splits a node across its input streams.
    pub fn split_across_input(&mut self, id: NodeId) -> Result<()> {
        // find the node
        let node = match self.get_node(id) {
            Some(n) => n,
            None => bail!(
                "Could not find node id: {:?}, where we are calling split_across_input",
                id
            ),
        };
        let stdin = node.get_stdin();
        // if there is nothing to parallelize across, don't do anything
        if stdin.len() <= 1 {
            return Ok(());
        } else {
        }

        // create new nodes
        let mut new_node_ids: Vec<NodeId> = Vec::new();
        for _i in 0..stdin.len() {
            let mut new_elem = node.get_elem();
            match new_elem {
                Elem::Cmd(ref mut cmdnode) => {
                    cmdnode.clear_stdin();
                    cmdnode.clear_stdout();
                    cmdnode.clear_stderr();
                    new_node_ids.push(self.add_elem(new_elem));
                }
                _ => {
                    bail!("Shouldn't be trying to split a non-cmd node");
                }
            }
        }
        // fix the LEFT side
        // replace all stdin edges/streams to point to new individual nodes
        let mut streams = node.get_stdout();
        let mut stderr = node.get_stderr();
        let mut edges_to_remove: Vec<Link> = Vec::new();
        let mut edges_to_add: Vec<Link> = Vec::new();

        for (i, stream) in node.get_stdin().iter().enumerate() {
            // add a new node for this input stream
            match stream {
                DashStream::Pipe(pipestream) => {
                    assert!(id == pipestream.get_right());
                    let old_edge = Link {
                        left: pipestream.get_left(),
                        right: id,
                    };
                    let new_edge = Link {
                        left: pipestream.get_left(),
                        right: new_node_ids[i],
                    };
                    edges_to_remove.push(old_edge.clone());
                    edges_to_add.push(new_edge.clone());
                    let mut new_pipestream = pipestream.clone();
                    new_pipestream.set_right(new_node_ids[i]);
                    // find the left side node and replace the corresponding pipestream
                    {
                        let left_node = match self.nodes.get_mut(&pipestream.get_left()) {
                            Some(n) => n,
                            None => bail!("Pipestream does not have a left node that exists"),
                        };
                        left_node.replace_stream_edges(old_edge, vec![new_edge])?;
                    }
                    {
                        let new_node = self.nodes.get_mut(&new_node_ids[i]).unwrap();
                        new_node.add_stdin(DashStream::Pipe(new_pipestream))?;
                    }
                }
                DashStream::Tcp(netstream) => {
                    assert!(id == netstream.get_right());
                    let old_edge = Link {
                        left: netstream.get_left(),
                        right: id,
                    };
                    let new_edge = Link {
                        left: netstream.get_right(),
                        right: new_node_ids[i],
                    };
                    edges_to_remove.push(old_edge.clone());
                    edges_to_add.push(new_edge.clone());
                    let mut new_netstream = netstream.clone();
                    new_netstream.set_right(new_node_ids[i]);
                    {
                        let left_node = match self.nodes.get_mut(&netstream.get_left()) {
                            Some(n) => n,
                            None => bail!("Netstream has a left node that doesn't exist in map"),
                        };
                        left_node.replace_stream_edges(old_edge, vec![new_edge])?;
                    }

                    {
                        let right_node = self.nodes.get_mut(&new_node_ids[i]).unwrap();
                        right_node.add_stdin(DashStream::Tcp(new_netstream))?;
                    }
                }
                _ => unreachable!(),
            }
        }

        // fix the right side.
        // Iterate over the stdout and stderr streams, and
        // change the right side node as well as the new left nodes
        streams.append(&mut stderr);
        for stream in streams.iter() {
            match stream {
                DashStream::Pipe(pipestream) => {
                    assert!(pipestream.get_left() == id);
                    let old_edge = Link {
                        left: id,
                        right: pipestream.get_right(),
                    };
                    let new_edges: Vec<Link> = new_node_ids
                        .clone()
                        .iter()
                        .map(|x| Link {
                            left: *x,
                            right: pipestream.get_right(),
                        })
                        .collect();
                    edges_to_remove.push(old_edge.clone());
                    edges_to_add.append(&mut new_edges.clone());
                    {
                        let right_node = match self.nodes.get_mut(&pipestream.get_right()) {
                            Some(n) => n,
                            None => bail!("Netstream has right node that doesn't exist"),
                        };
                        right_node.replace_stream_edges(old_edge.clone(), new_edges.clone())?;
                    }
                    for node_id in new_node_ids.iter() {
                        let mut new_pipestream = pipestream.clone();
                        new_pipestream.set_left(*node_id);
                        let left_node = self.nodes.get_mut(node_id).unwrap();
                        match pipestream.get_output_type() {
                            IOType::Stdout => {
                                left_node.add_stdout(DashStream::Pipe(new_pipestream))?;
                            }
                            IOType::Stderr => {
                                left_node.add_stderr(DashStream::Pipe(new_pipestream))?;
                            }
                            _ => {
                                unreachable!();
                            }
                        }
                    }
                }
                DashStream::Tcp(netstream) => {
                    let old_edge = Link {
                        left: id,
                        right: netstream.get_right(),
                    };
                    let new_edges: Vec<Link> = new_node_ids
                        .clone()
                        .iter()
                        .map(|x| Link {
                            left: *x,
                            right: netstream.get_right(),
                        })
                        .collect();
                    edges_to_remove.push(old_edge.clone());
                    edges_to_add.append(&mut new_edges.clone());
                    {
                        let right_node = match self.nodes.get_mut(&netstream.get_right()) {
                            Some(n) => n,
                            None => bail!("Netstream has right node that doesn't exist"),
                        };
                        right_node.replace_stream_edges(old_edge.clone(), new_edges.clone())?;
                    }
                    for node_id in new_node_ids.iter() {
                        let mut new_netstream = netstream.clone();
                        new_netstream.set_left(*node_id);
                        let left_node = self.nodes.get_mut(node_id).unwrap();
                        match netstream.get_output_type() {
                            IOType::Stdout => {
                                left_node.add_stdout(DashStream::Tcp(new_netstream))?;
                            }
                            IOType::Stderr => {
                                left_node.add_stderr(DashStream::Tcp(new_netstream))?;
                            }
                            _ => {
                                unreachable!();
                            }
                        }
                    }
                }
                _ => {
                    unreachable!();
                }
            }
        }
        // add and remove all the edges mentioned
        self.edges.retain(|x| !edges_to_remove.contains(x));
        for edge in edges_to_add.into_iter() {
            self.add_unique_edge(edge.get_left(), edge.get_right());
        }

        // remove the original node
        self.remove_node(id)?;

        Ok(())
    }

    pub fn remove_node(&mut self, id: NodeId) -> Result<()> {
        match self.nodes.remove(&id) {
            Some(_) => {}
            None => {
                bail!("Node id: {:?} not found in graph to remove", id);
            }
        }
        self.edges
            .retain(|x| x.get_left() != id && x.get_right() != id);
        if self.sink_nodes.contains(&id) {
            self.sink_nodes.retain(|x| *x != id);
        }
        if self.source_nodes.contains(&id) {
            self.source_nodes.retain(|x| *x != id);
        }
        Ok(())
    }

    /// For parallelization, replaces nodes with other nodes
    /// Also need to replace the corresponding pipestreams or netstreams
    pub fn replace_node(&mut self, id: NodeId, nodes: Vec<Elem>) -> Result<()> {
        // need to remove this node and replace any to and from edges
        if !self.nodes.contains_key(&id) {
            bail!("Id not in map");
        }

        // if the new nodes to replace with have length 1 -> just the arguments need to change
        if nodes.len() == 1 {
            let new_node = &nodes[0];
            let node = self.get_mut_node(id).unwrap();
            match node.get_mut_elem() {
                Elem::Cmd(ref mut cmdnode) => {
                    cmdnode.clear_args();
                    let args = match new_node {
                        Elem::Cmd(new_cmd_elem) => new_cmd_elem.get_args(),
                        _ => unreachable!(),
                    };
                    cmdnode.clear_args();
                    cmdnode.set_args(args);
                }
                Elem::Write(_) => {}
                Elem::Read(_) => {}
            }
            return Ok(());
        }

        let mut new_ids: Vec<NodeId> = Vec::new();
        for elem in nodes.into_iter() {
            let id = self.add_elem(elem);
            new_ids.push(id);
        }

        let mut edges_to_add: Vec<Link> = Vec::new();

        for edge in self.edges.iter() {
            if id == edge.get_left() || id == edge.get_right() {
                // calculate all left and right side replacements
                let mut replacements: Vec<Link> = new_ids
                    .iter()
                    .map(|&new_id| {
                        if id == edge.get_left() {
                            return Link {
                                left: new_id,
                                right: edge.get_right(),
                            };
                        } else {
                            return Link {
                                left: edge.get_left(),
                                right: new_id,
                            };
                        }
                    })
                    .collect();
                // modify the pipestreams in the new nodes
                for new_edge in replacements.iter() {
                    let mut new_id = new_edge.get_left();
                    if id == new_edge.get_left() {
                        new_id = new_edge.get_right();
                    } else {
                    }

                    let node = self.nodes.get_mut(&new_id).unwrap();
                    match node.get_mut_elem() {
                        Elem::Cmd(ref mut command_node) => {
                            command_node.replace_stream(edge, new_edge)?;
                        }
                        _ => {
                            unreachable!();
                        }
                    }
                }
                // modify the pipestreams in the other side of the node
                if id == edge.get_left() {
                    let node = self.nodes.get_mut(&edge.get_right()).unwrap();
                    node.replace_stream_edges(edge.clone(), replacements.clone())?;
                } else {
                    let node = self.nodes.get_mut(&edge.get_left()).unwrap();
                    node.replace_stream_edges(edge.clone(), replacements.clone())?;
                }

                // add in new edges
                edges_to_add.append(&mut replacements);
            }
        }

        // remove the original node from the graph
        for new_edge in edges_to_add.iter() {
            self.add_unique_edge(new_edge.get_left(), new_edge.get_right());
        }
        self.remove_node(id)?;

        Ok(())
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
                                pipestream.set_left(*left);
                                pipestream.set_right(*right);
                            }
                            _ => {}
                        }
                    }
                }
                Elem::Write(ref mut _writenode) => {
                    // NOOP (no pipes coming out of writenodes)
                }
                Elem::Cmd(ref mut cmdnode) => {
                    for stream in cmdnode.get_stdout_iter_mut() {
                        match stream {
                            DashStream::Pipe(ref mut pipestream) => {
                                pipestream.set_left(*left);
                                pipestream.set_right(*right);
                            }
                            _ => {}
                        }
                    }

                    for stream in cmdnode.get_stderr_iter_mut() {
                        match stream {
                            DashStream::Pipe(ref mut pipestream) => {
                                pipestream.set_left(*left);
                                pipestream.set_right(*right);
                            }
                            _ => {}
                        }
                    }
                }
            }
            let right_elem = program.get_mut_node(*right).unwrap().get_mut_elem();
            match right_elem {
                Elem::Read(ref mut _readnode) => {}
                Elem::Write(ref mut writenode) => {
                    for stream in writenode.get_stdin_iter_mut() {
                        match stream {
                            DashStream::Pipe(ref mut pipestream) => {
                                pipestream.set_left(*left);
                                pipestream.set_right(*right);
                            }
                            _ => {}
                        }
                    }
                }
                Elem::Cmd(ref mut cmdnode) => {
                    for stream in cmdnode.get_stdin_iter_mut() {
                        match stream {
                            DashStream::Pipe(ref mut pipestream) => {
                                println!("modifying pipstream: {:?}", pipestream);
                                pipestream.set_left(*left);
                                pipestream.set_right(*right);
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
                .replace_pipe_with_net(pipestream.clone(), new_stream.clone(), IOType::Stdin)?;
        }
        Ok(())
    }

    // Finds source->sink paths, ignoring paths that involve output for stderr.
    // RIGHT NOW, this works because we have the guarantee that each node has at most 2 outward
    // edges, 1 for stdout and 1 for stderr.
    // Need to redo this if we ever have nodes that broadcast output to multiple nodes.
    pub fn get_stdout_forward_paths(&self) -> Vec<Vec<NodeId>> {
        let mut ret: Vec<Vec<NodeId>> = Vec::new();
        for node_id in self.source_nodes.iter() {
            let mut path: Vec<NodeId> = Vec::new();
            let mut found_sink = false;
            let mut current_node = *node_id;
            path.push(current_node);
            while !found_sink {
                let node = self.nodes.get(&current_node).unwrap();
                match node.get_stdout_id() {
                    Some(n) => {
                        current_node = n;
                    }
                    None => {
                        found_sink = true;
                    }
                }
                path.push(current_node);

                if self.sink_nodes.contains(&current_node) {
                    found_sink = true;
                }
            }
            ret.push(path);
        }
        ret
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

    pub fn add_elem(&mut self, mut elem: Elem) -> NodeId {
        elem.set_id(self.counter + 1);
        let node: Node = Node {
            elem: elem,
            id: self.counter + 1,
        };
        self.nodes.insert(self.counter + 1, node);
        self.counter += 1;
        self.sink_nodes.push(self.counter); // node with no dependencies is automatically a sink
        self.source_nodes.push(self.counter); // node with no dependences is automatically a source
        self.counter
    }

    pub fn add_unique_node(&mut self, node: Node) {
        let mut is_connected_left = false;
        let mut is_connected_right = false;
        for edge in self.edges.iter() {
            if edge.get_left() == node.get_id() {
                is_connected_left = true;
            }
            if edge.get_right() == node.get_id() {
                is_connected_right = true;
            }
        }
        if !is_connected_left {
            self.sink_nodes.push(node.get_id());
        }

        if !is_connected_right {
            self.source_nodes.push(node.get_id());
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
            if self.source_nodes.contains(&right) {
                self.source_nodes.retain(|&x| x != right);
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
        if self.source_nodes.contains(&right.get_id()) {
            self.source_nodes.retain(|&x| x != right.get_id());
        }
    }

    /// Adds an entire pipeline of commands (e.g., a single line of commands connected to each
    /// other)
    pub fn add_pipeline(&mut self, elems: Vec<Elem>) {
        let mut last_node_id: Option<u32> = None;
        let mut first_node_id: Option<u32> = None;
        for elem in elems {
            let node: Node = Node {
                elem: elem,
                id: self.counter + 1,
            };
            self.nodes.insert(self.counter + 1, node);
            self.counter += 1;
            match first_node_id {
                Some(_id) => {}
                None => {
                    first_node_id = Some(self.counter - 1);
                }
            }
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
        self.source_nodes.push(first_node_id.unwrap());
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
        let mut stack: Vec<NodeId> = Vec::new();
        for node_id in self.sink_nodes.iter() {
            stack.push(*node_id);
        }

        while stack.len() > 0 {
            let node = stack.pop().unwrap();
            if !path.contains(&node) {
                path.insert(0, node);
                for dependence in self.find_dependent_nodes(node) {
                    stack.push(dependence);
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
    pub fn execute(&mut self, stream_map: SharedStreamMap, tmp_folder: String) -> Result<()> {
        let pipe_map = SharedPipeMap::new();
        let execution_order = self.execution_order();
        let mut node_threads: Vec<JoinHandle<Result<()>>> = Vec::new();
        let mut node_thread_ids: Vec<NodeId> = Vec::new();
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
            println!("finished spawning: {:?}", node);
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
            let tmp = tmp_folder.clone();
            // This call is non-blocking
            println!("about to run redirection for: {:?},", node_id);
            node_threads.push(spawn(move || {
                node_clone.run_redirection(pipe_map_copy, stream_map_copy, tmp.to_string())
            }));
            node_thread_ids.push(*node_id);
        }

        // Join all the threads to make sure it worked
        let mut count: usize = 0;
        for thread in node_threads {
            match thread.join() {
                Ok(res) => match res {
                    Ok(_) => {}
                    Err(e) => {
                        bail!(
                            "Node failed to execute: {:?} id {:?}",
                            e,
                            node_thread_ids[count]
                        );
                    }
                },
                Err(e) => {
                    bail!("Thread failed to join!: {:?}", e);
                }
            }
            count += 1;
        }
        println!("joined all the threads");

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

        // print the source nodes
        f.pad(&format!("source nodes: {:?}\n", self.source_nodes))?;
        // print the sink nodes
        f.pad(&format!("sink nodes: {:?}\n", self.sink_nodes))
    }
}
