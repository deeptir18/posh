use super::program::{Link, NodeId};
use super::stream;
use super::Location;
use super::Result;
use failure::bail;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::fs::{remove_file, File};
use std::io::ErrorKind;
use std::io::{copy, Read, Write};
use std::path::{Path, PathBuf};
use std::{thread, time};
use stream::{DashStream, IOType, NetStream, PipeStream, SharedPipeMap, SharedStreamMap};
use tracing::debug;

const READ_BUFFER_SIZE: usize = 4096;

fn get_filename(node_id: NodeId, stdin_idx: usize) -> String {
    format!("{}_{:?}.tmp", node_id, stdin_idx)
}

/// Run redirection from reader to writer, interfacing with tmp file if necessary.
/// reader: the input stream
/// writer: the output stream
/// metadata: information about which streams have finished
/// idx: the index of this input reader
/// tmp_handles: vector of temporary file handles
/// NOTE: important for this function to always call increment_current() on metadata whenever a
/// stream finishes
/// TODO: finish this function
/// Also need to handle the nonblocking error for the tcp stream
pub fn iterating_redirect<R: ?Sized, W: ?Sized>(
    reader: &mut R,
    writer: &mut W,
    metadata: &mut InputStreamMetadata,
    idx: usize,
    tmp_handles: &mut Vec<File>,
    node_id: NodeId,
) -> Result<u64>
where
    R: Read,
    W: Write,
{
    //debug!("in iterating redirect {:?}, idx {:?}", node_id, idx);
    // optimization: if there is one input stream,
    // directly copy from the reader to the writer
    // and increment the count
    if metadata.get_size() == 1 {
        debug!("In case where metadata size is 1 for node {:?}; about to run copy from input to output", node_id);
        let s = copy_wrapper(reader, writer)?;
        debug!("Node {:?} finished stdin copy", node_id);
        metadata.increment_bytes(0, s);
        metadata.set_finished(0);
        metadata.increment_current();
        return Ok(s);
    } else {
        // copy everything into tmpfiles
    }

    // else just copy ALL output into individual tmps
    if !metadata.get_finished_tmp(idx) {
        let mut tmpfile = &tmp_handles[idx];
        let mut buf = [0u8; READ_BUFFER_SIZE];
        match read_rapper(reader, &mut buf) {
            Ok(s) => {
                metadata.increment_bytes(idx, s as u64);
                // make sure to ONLY write what was read into the writer
                if s == 0 {
                    metadata.set_finished_tmp(idx);
                //debug!("Node {:?} Finished reading tmpfile for # {}", node_id, idx);
                } else {
                    /*debug!(
                        "about to write into tmpfile because read {:?} bytes, node {:?}, idx {:?}",
                        s, node_id, idx
                    );*/
                    tmpfile.write_all(&buf[..s])?;
                    tmpfile.flush()?;
                    /*debug!(
                        "wrote and flushed into tmpfile because read {:?} bytes, node {:?}, idx {:?}",
                        s, node_id, idx
                    );*/
                }
                return Ok(s as u64);
            }
            Err(e) => match e.kind() {
                ErrorKind::WouldBlock => {
                    let sleep_duration = time::Duration::from_millis(10);
                    thread::sleep(sleep_duration);
                    return Ok(0);
                }
                _ => {
                    bail!("Failed reading stdin on stream {:?}: {:?}", idx, e);
                }
            },
        }
    } else {
        // check if all finished, copy everything
        if metadata.all_finished() {
            debug!("All finished");
            let file_handles = metadata.open_read_only_files()?;
            for i in 0..metadata.get_size() {
                let mut tmpfile = &file_handles[i];
                debug!("Node {:?} Trying to copy {:?}", node_id, i);
                let size = copy(&mut tmpfile, writer)?;
                debug!("Node {:?} copied {:?}", node_id, size);
            }
            metadata.set_current_finished();
        }
        return Ok(0);
    }

    /*if idx == metadata.current() {
        /*debug!(
            "in iterating redirect {:?}, idx {:?}, current: {:?} is current!",
            node_id,
            idx,
            metadata.current()
        );*/
        // first, copy everything from the tmp file into the writer
        // if we haven't yet
        if !metadata.get_finished_tmp(idx) {
            let tmpfile = &tmp_handles[idx];
            /*debug!(
                "Trying to get metadata for {:?}; node id {:?}",
                idx, node_id
            );*/
            let file_metadata = tmpfile.metadata()?;
            //debug!("Got metadata for {:?}; node id {:?}", idx, node_id);
            if file_metadata.len() > 0 {
                /*debug!(
                    "Node id {:?}, trying to open {:?}, for idx {:?}",
                    node_id,
                    metadata.get_filename(idx),
                    idx
                );*/
                let mut new_tmpfile_handle = File::open(metadata.get_filename(idx).as_path())?;
                debug!(
                    "node {:?}, copying from tmpfile into writer for idx {:?}",
                    node_id, idx
                );
                let _ = copy_wrapper(&mut new_tmpfile_handle, writer)?;
                debug!(
                    "node {:?}, finished copying from tmpfile into writer",
                    node_id
                );
            }
            metadata.set_finished_tmp(idx);
        }
        if metadata.finished(idx) {
            metadata.increment_current();
        } else {
            //debug!("Node {:?} Continuing to read from {:?}", node_id, idx);
            let mut buf = [0u8; READ_BUFFER_SIZE];
            match read_rapper(reader, &mut buf) {
                Ok(s) => {
                    metadata.increment_bytes(idx, s as u64);
                    // if done reading.
                    if s == 0 {
                        metadata.set_finished(idx);
                        metadata.increment_current();
                        debug!(" {:?} Finished reading from {:?}", node_id, idx);
                    }

                    // make sure to ONLY write what was read into the writer
                    writer.write_all(&buf[..s])?;
                    writer.flush()?;
                    return Ok(s as u64);
                }
                Err(e) => {
                    match e.kind() {
                        // since TCP stream is not blocking -- need to check if nothing is
                        // available on this thread and check back
                        ErrorKind::WouldBlock => {
                            let sleep_duration = time::Duration::from_millis(10);
                            thread::sleep(sleep_duration);
                        }
                        _ => {
                            bail!("Failed reading stdin on stream {:?}: {:?}", idx, e);
                        }
                    }
                }
            }
        }
    } else {
        /*debug!(
            "idx that is greater than current: {:?}, current is {:?}, node id: {:?}",
            idx,
            metadata.current(),
            node_id
        );*/
        if metadata.get_finished_tmp(idx) {
            return Ok(0);
        }
        let mut tmpfile = &tmp_handles[idx];
        let mut buf = [0u8; READ_BUFFER_SIZE];
        match read_rapper(reader, &mut buf) {
            Ok(s) => {
                metadata.increment_bytes(idx, s as u64);
                // make sure to ONLY write what was read into the writer
                if s == 0 {
                    metadata.set_finished(idx);
                //debug!("Finished reading tmpfile for # {}", idx);
                } else {
                    /*debug!(
                        "about to write into tmpfile because read {:?} bytes, node {:?}, idx {:?}",
                        s, node_id, idx
                    );*/
                    tmpfile.write_all(&buf[..s])?;
                    tmpfile.flush()?;
                    /*debug!(
                        "wrote and flushed into tmpfile because read {:?} bytes, node {:?}, idx {:?}",
                        s, node_id, idx
                    );*/
                }
            }
            Err(e) => match e.kind() {
                ErrorKind::WouldBlock => {
                    let sleep_duration = time::Duration::from_millis(10);
                    thread::sleep(sleep_duration);
                }
                _ => {
                    bail!("Failed reading stdin on stream {:?}: {:?}", idx, e);
                }
            },
        }
    }
    /*debug!(
        "returning from this terrible function for NODE ID {:?} and idx {:?}",
        node_id, idx
    );*/

    Ok(0)*/
}
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct InputStreamMetadata {
    curr: usize,
    /// current input stream we are copying from
    size: usize,
    /// the length of the stdin stream list
    finished_map: HashMap<usize, bool>,
    /// is this index stream finished or not?
    filenames: Vec<PathBuf>,
    /// bytes copied: for extra tracking
    bytes_copied: HashMap<usize, u64>,
    /// finished_tmp
    finished_tmp: HashMap<usize, bool>,
}

impl InputStreamMetadata {
    /// Create a new metadata object about the current node.
    pub fn new(node_id: NodeId, tmp_folder: &str, len_stdin: usize) -> Self {
        let folder = Path::new(tmp_folder);
        let mut filenames: Vec<PathBuf> = Vec::new();
        let mut map: HashMap<usize, bool> = HashMap::default();
        let mut bytes_copied: HashMap<usize, u64> = HashMap::default();
        for i in 0..len_stdin {
            // the format for temporary files. could change later.
            let path = get_filename(node_id, i);
            let mut filename = folder.to_path_buf();
            filename.push(Path::new(&path));
            map.insert(i, false);
            bytes_copied.insert(i, 0);
            filenames.push(filename.to_path_buf());
        }

        InputStreamMetadata {
            curr: 0,
            size: len_stdin,
            finished_map: map.clone(),
            filenames: filenames,
            bytes_copied: bytes_copied,
            finished_tmp: map,
        }
    }

    pub fn get_size(&self) -> usize {
        self.size
    }

    pub fn set_current_finished(&mut self) {
        self.curr = self.size;
    }

    pub fn get_finished_tmp(&self, idx: usize) -> bool {
        *self.finished_tmp.get(&idx).unwrap()
    }

    pub fn set_finished_tmp(&mut self, idx: usize) {
        *self.finished_tmp.get_mut(&idx).unwrap() = true;
    }

    pub fn all_finished(&self) -> bool {
        for i in 0..self.size {
            if !self.get_finished_tmp(i) {
                return false;
            }
        }
        return true;
    }

    pub fn current(&self) -> usize {
        self.curr
    }

    pub fn current_mut(&mut self) -> usize {
        self.curr
    }

    pub fn increment_current(&mut self) {
        self.curr += 1;
    }

    pub fn increment_bytes(&mut self, idx: usize, bytes: u64) {
        let counter = self.bytes_copied.get_mut(&idx).unwrap();
        *counter += bytes;
    }

    pub fn finished(&self, id: usize) -> bool {
        *self.finished_map.get(&id).unwrap()
    }

    pub fn set_finished(&mut self, id: usize) {
        *self.finished_map.get_mut(&id).unwrap() = true;
    }

    pub fn get_filename(&self, id: usize) -> PathBuf {
        self.filenames[id].clone()
    }

    /// Returns a vector of filehandles for the temporary files.
    /// Note that IFF there is one input stream -> returns an empty vector,
    /// as the implementation will never actually use the temporary file.
    pub fn open_files(&self) -> Result<Vec<File>> {
        let mut ret: Vec<File> = Vec::new();
        if self.filenames.len() > 1 {
            for filename in self.filenames.iter() {
                let file = File::create(filename.as_path())?;
                ret.push(file);
            }
        }
        Ok(ret)
    }

    pub fn open_read_only_files(&self) -> Result<Vec<File>> {
        let mut ret: Vec<File> = Vec::new();
        if self.filenames.len() > 1 {
            for filename in self.filenames.iter() {
                let file = OpenOptions::new().read(true).open(filename)?;
                ret.push(file);
            }
        }
        Ok(ret)
    }

    /// Remove the temporary files.
    pub fn remove_files(&self) -> Result<()> {
        debug!("trying to remove: {:?}", self.filenames);
        if self.filenames.len() > 1 {
            for filename in self.filenames.iter() {
                remove_file(filename.as_path())?;
            }
        }
        Ok(())
    }
}
/// Dash wrapper for copy that catches pipe close errors.
pub fn copy_wrapper<R: ?Sized, W: ?Sized>(reader: &mut R, writer: &mut W) -> Result<u64>
where
    R: Read,
    W: Write,
{
    let finished: bool = false;
    while !finished {
        match copy(reader, writer) {
            Ok(s) => {
                return Ok(s);
            }
            Err(e) => match e.kind() {
                ErrorKind::BrokenPipe => {
                    return Ok(0);
                }
                ErrorKind::ConnectionAborted => {
                    return Ok(0);
                }
                ErrorKind::WouldBlock => {
                    // sleep and try again
                    // ideally, set the underlying writer to just be blocking
                    // this function is only called in settings where it's safe to block
                    let sleep_duration = time::Duration::from_millis(10);
                    thread::sleep(sleep_duration);
                }
                _ => {
                    bail!("{:?}", e);
                }
            },
        }
    }
    Ok(0)
}

/// Dash wrapper for copy that catches pipe close and connection aborted errors.
pub fn read_rapper<R: ?Sized>(reader: &mut R, buf: &mut [u8]) -> std::io::Result<usize>
where
    R: Read,
{
    match reader.read(buf) {
        Ok(s) => Ok(s),
        Err(e) => match e.kind() {
            ErrorKind::BrokenPipe => Ok(0),
            ErrorKind::ConnectionAborted => Ok(0),
            _ => Err(e),
        },
    }
}
/// Checks if this is a stream that represents a TCP connection that should be initiated by this
/// nodeid.
pub fn stream_initiate_filter(s: DashStream, node_id: NodeId, is_server: bool) -> bool {
    match s {
        DashStream::Tcp(netstream) => match is_server {
            // if not server (e.g. client), always initiate stream
            false => true,
            // if it is the server, check that the other connection is NOT the client, and this is
            // the left side of the connection
            true => {
                let loc = match netstream.get_connection(node_id) {
                    Some(l) => l,
                    None => {
                        // TODO: add some debugging here?
                        return false;
                    }
                };
                match loc {
                    Location::Client => {
                        return false;
                    }
                    _ => {}
                }
                if netstream.get_left() == node_id {
                    return false;
                } else {
                    return true;
                }
            }
        },
        _ => false,
    }
}

/// Resolves a file stream to point to the correct path on the given server
pub fn resolve_file_streams(streams: &mut Vec<DashStream>, parent_dir: &Path) {
    for s in streams.iter_mut() {
        match s {
            DashStream::File(filestream) => {
                // Mutates the underlying filestream object.
                filestream.prepend_directory(parent_dir);
            }
            _ => {}
        }
    }
}
/// Defines the set of functionality necessary to execute a node on any machine.
/// All types of nodes implement this trait.
pub trait Rapper {
    fn set_id(&mut self, id: NodeId);
    fn get_id(&self) -> NodeId;
    /// Generates the relevant dot label string for this node for display.
    fn get_dot_label(&self) -> Result<String>;
    /// Returns all streams this node would need to initiate.
    fn get_outward_streams(&self, iotype: IOType, is_server: bool) -> Vec<NetStream>;

    fn get_stdin(&self) -> Vec<DashStream>;

    fn get_stdout(&self) -> Vec<DashStream>;

    fn get_stderr(&self) -> Vec<DashStream>;

    fn get_stdin_len(&self) -> usize;

    fn get_stdout_len(&self) -> usize;

    fn get_stderr_len(&self) -> usize;

    fn add_stdin(&mut self, stream: DashStream) -> Result<()>;

    fn add_stdout(&mut self, stream: DashStream) -> Result<()>;

    fn add_stderr(&mut self, stream: DashStream) -> Result<()>;

    /// Starts processes that *execute* any commands.
    fn execute(&mut self, pipes: SharedPipeMap, network_connections: SharedStreamMap)
        -> Result<()>;

    /// Spawns threads that run redirection of I/O for any commands.
    fn run_redirection(
        &mut self,
        pipes: SharedPipeMap,
        network_connections: SharedStreamMap,
        tmp_folder: String,
    ) -> Result<()>;

    fn get_loc(&self) -> Location;

    fn set_loc(&mut self, loc: Location);

    fn resolve_args(&mut self, parent_dir: &str) -> Result<()>;

    fn replace_pipe_with_net(
        &mut self,
        pipe: PipeStream,
        net: NetStream,
        iotype: IOType,
    ) -> Result<()>;

    fn replace_stream_edges(&mut self, _edge: Link, _new_edges: Vec<Link>) -> Result<()> {
        bail!("Function replace_stream_edges shouldn't be called from write node");
    }

    // Gets the ID of where stdout of this node goes.
    // Assumes nodes don't broadcast to multiple nodes.
    fn get_stdout_id(&self) -> Option<NodeId>;
}
