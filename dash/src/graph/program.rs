use super::command as cmd;
use super::execute::Execute;
use super::info::Info;
use super::pipe::SharedChannelMap;
use super::rapper::Rapper;
use super::read2 as read;
use super::write2 as write;
use super::{filestream, stream, Location, Result};
use failure::bail;
use filestream::{FifoMode, FifoStream, FileStream};
use serde::{Deserialize, Serialize};
use std::collections::hash_map;
use std::collections::HashMap;
use std::env;
use std::fmt;
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;
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

impl Execute for Elem {
    /// Spawns the node to do the necessary work.
    fn spawn(
        &mut self,
        pipes: SharedPipeMap,
        network_connections: SharedStreamMap,
        channels: SharedChannelMap,
        tmp_folder: PathBuf,
    ) -> Result<()> {
        match self {
            Elem::Write(write_node) => {
                write_node.spawn(pipes, network_connections, channels, tmp_folder)
            }
            Elem::Cmd(command_node) => {
                command_node.spawn(pipes, network_connections, channels, tmp_folder)
            }
            Elem::Read(read_node) => {
                read_node.spawn(pipes, network_connections, channels, tmp_folder)
            }
        }
    }

    /// Redirects input and output of node to the correct places based on where the stdin, stdout
    /// and stderr go to.
    fn redirect(
        &mut self,
        pipes: SharedPipeMap,
        network_connections: SharedStreamMap,
        channels: SharedChannelMap,
        tmp_folder: PathBuf,
    ) -> Result<()> {
        match self {
            Elem::Write(write_node) => {
                write_node.redirect(pipes, network_connections, channels, tmp_folder)
            }
            Elem::Cmd(command_node) => {
                command_node.redirect(pipes, network_connections, channels, tmp_folder)
            }
            Elem::Read(read_node) => {
                read_node.redirect(pipes, network_connections, channels, tmp_folder)
            }
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
            Elem::Write(write_node) => match write_node.get_stdout() {
                Some(stream) => vec![stream],
                None => vec![],
            },
            Elem::Read(read_node) => match read_node.get_stdout() {
                Some(stream) => vec![stream],
                None => vec![],
            },

            Elem::Cmd(cmd_node) => match cmd_node.get_stdout() {
                Some(stream) => vec![stream],
                None => vec![],
            },
        }
    }

    fn get_stderr(&self) -> Vec<DashStream> {
        match self {
            Elem::Write(write_node) => match write_node.get_stderr() {
                Some(stream) => vec![stream],
                None => vec![],
            },
            Elem::Read(read_node) => match read_node.get_stderr() {
                Some(stream) => vec![stream],
                None => vec![],
            },

            Elem::Cmd(cmd_node) => match cmd_node.get_stderr() {
                Some(stream) => vec![stream],
                None => vec![],
            },
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
            Elem::Write(write_node) => write_node.set_stdout(stream),
            Elem::Read(read_node) => read_node.set_stdout(stream),
            Elem::Cmd(cmd_node) => cmd_node.set_stdout(stream),
        }
    }

    fn add_stderr(&mut self, stream: DashStream) -> Result<()> {
        match self {
            Elem::Write(write_node) => write_node.set_stderr(stream),
            Elem::Read(read_node) => read_node.set_stderr(stream),
            Elem::Cmd(cmd_node) => cmd_node.set_stderr(stream),
        }
    }

    fn execute(
        &mut self,
        _pipes: SharedPipeMap,
        _network_connections: SharedStreamMap,
    ) -> Result<()> {
        unimplemented!()
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
        _pipes: SharedPipeMap,
        _network_connections: SharedStreamMap,
        _tmp_folder: String,
    ) -> Result<()> {
        unimplemented!()
    }

    fn resolve_args(&mut self, parent_dir: &str) -> Result<()> {
        match self {
            Elem::Write(write_node) => write_node.resolve_args(Path::new(parent_dir).to_path_buf()),
            Elem::Read(read_node) => read_node.resolve_args(Path::new(parent_dir).to_path_buf()),
            Elem::Cmd(cmd_node) => cmd_node.resolve_args(Path::new(parent_dir).to_path_buf()),
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
        channels: SharedChannelMap,
        tmp_folder: PathBuf,
    ) -> Result<()> {
        self.elem
            .redirect(pipes, network_connections, channels, tmp_folder)
    }

    pub fn execute(
        &mut self,
        pipes: SharedPipeMap,
        network_connections: SharedStreamMap,
        channels: SharedChannelMap,
        tmp_folder: PathBuf,
    ) -> Result<()> {
        self.elem
            .spawn(pipes, network_connections, channels, tmp_folder)
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
    pub fn new(left: NodeId, right: NodeId) -> Self {
        Link {
            left: left,
            right: right,
        }
    }
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

    pub fn set_id(&mut self, id: ProgId) {
        self.id = id;
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
    pub fn split_across_input(&mut self, id: NodeId) -> Result<Vec<NodeId>> {
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
            return Ok(vec![]);
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

        Ok(new_node_ids)
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

    pub fn replace_node(&mut self, id: NodeId, nodes: Vec<Elem>) -> Result<()> {
        let _ = self.replace_node_parallel(id, nodes, true)?;
        Ok(())
    }

    /// For parallelization, replaces nodes with other nodes
    /// Also need to replace the corresponding pipestreams or netstreams
    pub fn replace_node_parallel(
        &mut self,
        id: NodeId,
        nodes: Vec<Elem>,
        replace_args: bool,
    ) -> Result<Vec<NodeId>> {
        // need to remove this node and replace any to and from edges
        if !self.nodes.contains_key(&id) {
            bail!("Id not in map");
        }

        // if the new nodes to replace with have length 1 -> just the arguments need to change
        if nodes.len() == 1 && replace_args {
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
            return Ok(vec![]);
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

        Ok(new_ids)
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
                Elem::Read(ref mut readnode) => match readnode.get_stdout_mut() {
                    DashStream::Pipe(ref mut pipestream) => {
                        pipestream.set_left(*left);
                        pipestream.set_right(*right);
                    }
                    _ => {}
                },
                Elem::Write(ref mut _writenode) => {
                    // NOOP (no pipes coming out of writenodes)
                }
                Elem::Cmd(ref mut cmdnode) => {
                    match cmdnode.get_stdout_mut() {
                        Some(ref mut dashstream) => match dashstream {
                            DashStream::Pipe(ref mut pipestream) => {
                                pipestream.set_left(*left);
                                pipestream.set_right(*right);
                            }
                            _ => {}
                        },
                        None => {}
                    }

                    match cmdnode.get_stderr_mut() {
                        Some(ref mut dashstream) => match dashstream {
                            DashStream::Pipe(ref mut pipestream) => {
                                pipestream.set_left(*left);
                                pipestream.set_right(*right);
                            }
                            _ => {}
                        },
                        None => {}
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
                                tracing::debug!("modifying pipstream: {:?}", pipestream);
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

    /// Inserts a remote read operation into the program.
    pub fn add_remote_fifo_read(
        &mut self,
        origin_loc: &Location,
        access_loc: &Location,
        origin_filestream: &FileStream,
        fifo_location: &FifoStream,
    ) -> Result<()> {
        // add in a read node and write node
        let mut readnode = read::ReadNode::default();
        readnode.add_stdin(DashStream::File(origin_filestream.clone()))?;
        let readnode_id = self.add_elem(Elem::Read(readnode));
        let mut writenode = write::WriteNode::default();
        let mut new_fifo = fifo_location.clone();
        new_fifo.set_mode(FifoMode::WRITE);
        writenode.set_stdout(DashStream::Fifo(fifo_location.clone()))?;
        let writenode_id = self.add_elem(Elem::Write(writenode));

        // add edge in between them
        self.add_unique_edge(readnode_id, writenode_id);
        let netstream = NetStream::new(
            readnode_id,
            writenode_id,
            IOType::Stdout,
            origin_loc.clone(),
            access_loc.clone(),
        )?;

        // set the connection between two nodes
        let read = self.nodes.get_mut(&readnode_id).unwrap();
        {
            let read_elem = read.get_mut_elem();
            read_elem.add_stdout(DashStream::Tcp(netstream.clone()))?;
        }

        let write = self.nodes.get_mut(&writenode_id).unwrap();
        {
            let write_elem = write.get_mut_elem();
            write_elem.add_stdin(DashStream::Tcp(netstream.clone()))?;
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

    pub fn get_sinks(&self) -> Vec<NodeId> {
        self.sink_nodes.clone()
    }

    /// Finds nodes that are dependent on a certain node
    pub fn get_dependent_nodes(&self, node_id: NodeId) -> Vec<NodeId> {
        self.edges
            .iter()
            .filter(|&link| link.get_right() == node_id)
            .map(|link| link.get_left())
            .collect()
    }

    pub fn get_outgoing_nodes(&self, node_id: NodeId) -> Vec<NodeId> {
        self.edges
            .iter()
            .filter(|&link| link.get_left() == node_id)
            .map(|link| link.get_right())
            .collect()
    }

    // finds outgoing edges from a node
    pub fn get_outgoing_edges(&self, node_id: NodeId) -> Vec<(IOType, Link)> {
        let mut ret: Vec<(IOType, Link)> = Vec::new();
        let node = self.nodes.get(&node_id).unwrap();
        match node.get_elem() {
            Elem::Cmd(cmdnode) => {
                if let Some(stdout) = cmdnode.get_stdout() {
                    match stdout {
                        DashStream::Pipe(ps) => {
                            ret.push((IOType::Stdout, Link::new(ps.get_left(), ps.get_right())));
                        }
                        DashStream::Tcp(ns) => {
                            ret.push((IOType::Stdout, Link::new(ns.get_left(), ns.get_right())));
                        }
                        _ => {
                            unreachable!();
                        }
                    }
                }
                if let Some(stderr) = cmdnode.get_stderr() {
                    match stderr {
                        DashStream::Pipe(ps) => {
                            ret.push((IOType::Stderr, Link::new(ps.get_left(), ps.get_right())));
                        }
                        DashStream::Tcp(ns) => {
                            ret.push((IOType::Stderr, Link::new(ns.get_left(), ns.get_right())));
                        }
                        _ => {
                            unreachable!();
                        }
                    }
                }
            }
            Elem::Read(readnode) => {
                if let Some(stdout) = readnode.get_stdout() {
                    match stdout {
                        DashStream::Pipe(ps) => {
                            ret.push((IOType::Stdout, Link::new(ps.get_left(), ps.get_right())));
                        }
                        DashStream::Tcp(ns) => {
                            ret.push((IOType::Stdout, Link::new(ns.get_left(), ns.get_right())));
                        }
                        _ => {
                            // readnode can only have pipe or network pipe as output
                            unreachable!();
                        }
                    }
                }
            }
            Elem::Write(_writenode) => {
                // write node should have no stdout
            }
        }
        ret
    }

    pub fn get_dependent_edges(&self, node_id: NodeId) -> Vec<Link> {
        self.edges
            .iter()
            .filter(|&link| link.get_right() == node_id)
            .map(|link| (link.clone()))
            .collect()
    }

    fn partial_ordering_helper(
        &self,
        id: NodeId,
        visited: &mut HashMap<NodeId, bool>,
        stack: &mut Vec<NodeId>,
    ) {
        visited.insert(id, true);
        for outgoing in self.get_outgoing_nodes(id) {
            if !(visited.get(&outgoing).unwrap()) {
                self.partial_ordering_helper(outgoing, visited, stack);
            }
        }
        stack.insert(0, id);
    }

    /// Finds an execution order for the nodes
    /// by doing a topological sort via DFS
    pub fn execution_order(&self) -> Vec<NodeId> {
        let mut path: Vec<NodeId> = Vec::new();
        let nodes: Vec<NodeId> = self.nodes.keys().map(|k| *k).collect();
        let mut visited: HashMap<NodeId, bool> = nodes.iter().map(|k| (*k, false)).collect();
        for node_id in nodes.iter() {
            if !(visited.get(node_id).unwrap()) {
                self.partial_ordering_helper(*node_id, &mut visited, &mut path);
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

    /// If any of the nodes need a current dir, finds what the dir is
    /// to set for the entire program
    pub fn get_current_dir(&self) -> Option<PathBuf> {
        for (_id, node) in self.nodes.iter() {
            match node.get_elem() {
                Elem::Cmd(cmdnode) => {
                    if cmdnode.get_options().get_needs_current_dir() {
                        return Some(cmdnode.get_pwd());
                    }
                }
                _ => {}
            }
        }
        return None;
    }

    /// Executes a program on the current server.
    /// stream_map: SharedStreamMap that contains handles to any tcp streams needed by any nodes to
    /// execute.
    /// when executing the node. Note that if it's a client, folder should be none; no filepaths
    /// need to be resolved.
    pub fn execute(&mut self, stream_map: SharedStreamMap, tmp_folder: String) -> Result<()> {
        let pipe_map = SharedPipeMap::new();
        let channel_map = SharedChannelMap::new();
        let execution_order = self.execution_order();
        let mut node_threads: Vec<JoinHandle<Result<()>>> = Vec::new();
        let mut node_thread_ids: Vec<NodeId> = Vec::new();

        // First, set the current dir if this program requires it.
        // theoretically should not break anything else, as stuff is being executed with full paths
        match self.get_current_dir() {
            Some(pathbuf) => {
                tracing::debug!("Trying to set current dir on server: {:?}", pathbuf);
                env::set_current_dir(pathbuf.as_path())?;
            }
            None => {}
        }
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
            let tmp = Path::new(&tmp_folder).to_path_buf();
            // This call is non-blocking
            node_clone.execute(pipe_map_copy, stream_map_copy, channel_map.clone(), tmp)?;
            tracing::debug!("finished spawning: {:?}", node);
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
            let channels_clone = channel_map.clone();
            let mut node_clone = node.clone();
            let tmp = Path::new(&tmp_folder).to_path_buf();
            // This call is non-blocking
            tracing::debug!("about to run redirection for: {:?},", node_id);
            node_threads.push(spawn(move || {
                node_clone.run_redirection(pipe_map_copy, stream_map_copy, channels_clone, tmp)
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
        tracing::debug!("joined all the threads");

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
