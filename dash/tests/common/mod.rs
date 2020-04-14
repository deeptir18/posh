use dash::graph::command::CommandNode;
use dash::graph::filestream::{FifoMode, FifoStream, FileStream};
use dash::graph::info::Info;
use dash::graph::program::{Elem, Program};
use dash::graph::rapper::Rapper;
use dash::graph::read2::ReadNode;
use dash::graph::stream::{DashStream, IOType, NetStream, PipeStream};
use dash::graph::write2::WriteNode;
use dash::graph::Location;
use dash::util::Result;
use failure::bail;
use itertools::concat;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{create_dir, create_dir_all, remove_dir_all, File};
use std::io::Write;
use std::io::{BufRead, BufReader, BufWriter};
use std::path::{Path, PathBuf};
/// Temporary files used by Dash execution.
pub static EXECUTION: &str = "execution";
/// Temporary files with test output.
static TEST: &str = "test";

/// Test has a number of input files, along with a size for each input file.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Hash, Eq)]
pub struct TestInfo {
    folder_name: String,
    id: u32,
    num_input_files: u32,
    size: usize,
}

impl TestInfo {
    pub fn new(folder_name: String, id: u32, num_input_files: u32, size: usize) -> TestInfo {
        TestInfo {
            folder_name: folder_name,
            id: id,
            num_input_files: num_input_files,
            size: size,
        }
    }

    pub fn get_test_folder(&self) -> PathBuf {
        let path: PathBuf = [self.folder_name.as_str(), TEST].iter().collect();
        path
    }

    pub fn get_execution_folder(&self) -> PathBuf {
        let path: PathBuf = [self.folder_name.as_str(), EXECUTION].iter().collect();
        path
    }

    pub fn input_file_name(&self, file_id: u32) -> PathBuf {
        let filename = format!("test{}_{}_input.txt", self.id, file_id);
        let mut path = self.get_test_folder();
        path.push(filename.as_str());
        path
    }
    pub fn output_file_name(&self) -> PathBuf {
        let filename = format!("test{}_output.txt", self.id);
        let mut path = self.get_test_folder();
        path.push(filename.as_str());
        path
    }

    // Generate a file by repeating the alphabet
    pub fn content(&self) -> Vec<String> {
        let linesize = 500;
        let repeated_char = |letter: char| -> String {
            std::iter::repeat(letter).take(linesize).collect::<String>()
        };
        // chars that are alphabetic
        let alphabet: Vec<_> = (65..123)
            .map(|x| x.into())
            .filter(|x| char::is_alphanumeric(*x))
            .collect();
        let mut ret: Vec<String> = Vec::new();
        let mut ctr = 0;
        while ctr < self.size {
            ret.push(repeated_char(alphabet[ctr % alphabet.len()]));
            ctr += 1;
        }
        ret
    }

    pub fn grepped_content(&self, keyword: &str) -> Vec<String> {
        let file_content = self.content();
        let mut ret: Vec<String> = Vec::new();
        for line in file_content.into_iter() {
            if line.starts_with(keyword) {
                ret.push(line);
            }
        }
        println!("Grepped content len: {:?}", ret.len());
        ret
    }

    pub fn generate_input(&self) {
        for i in 0..self.num_input_files {
            let file = File::create(self.input_file_name(i).as_path()).unwrap();
            let mut bufwriter = BufWriter::new(&file);
            for line in self.content().into_iter() {
                bufwriter.write_fmt(format_args!("{}\n", line)).unwrap();
            }
        }
    }

    /// Returns a boolean checking whether the output file matches what is present in the vector.
    pub fn check_output(&self, correct: Vec<String>) -> bool {
        let file = File::open(self.output_file_name()).unwrap();
        let bufreader = BufReader::new(file);
        let lines: Vec<String> = bufreader
            .lines()
            .map(|line| format!("{}", line.unwrap()))
            .collect();
        println!(
            "Correct length: {:?}, file line length: {:?}",
            correct.len(),
            lines.len()
        );
        if lines == correct {
            true
        } else {
            false
        }
    }

    /// Check that the grepped content for this test is correct.
    pub fn check_grepped_output(&self, keyword: &Vec<&str>) -> bool {
        let mut input: Vec<Vec<String>> = Vec::new();
        for _i in 0..self.num_input_files {
            for key in keyword.iter() {
                input.push(self.grepped_content(key));
            }
        }
        self.check_output(concat(input))
    }

    /// Check that the original (non grepped) output for this test is correct.
    pub fn check_original_output(&self) -> bool {
        let mut input: Vec<Vec<String>> = Vec::new();
        for _i in 0..self.num_input_files {
            input.push(self.content());
        }
        self.check_output(concat(input))
    }
    pub fn setup_tmp_folder(&self) {
        create_dir_all(self.get_test_folder().as_path()).unwrap();
        create_dir(self.get_execution_folder().as_path()).unwrap();
    }

    pub fn delete_folder(&self) {
        remove_dir_all(Path::new(self.folder_name.as_str())).unwrap();
    }
}

impl Drop for TestInfo {
    fn drop(&mut self) {
        // delete all the files
        if Path::new(self.folder_name.as_str()).exists() {
            remove_dir_all(Path::new(self.folder_name.as_str())).unwrap();
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Hash, Eq)]
pub enum WriteType {
    File,
    Fifo,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Hash, Eq)]
pub struct NodeInfo {
    pub input_file: Option<PathBuf>,
    pub output_file: Option<(PathBuf, WriteType)>,
    pub input_keyword: Option<String>,
    pub location: Location,
}

impl Default for NodeInfo {
    fn default() -> Self {
        NodeInfo {
            input_file: None,
            output_file: None,
            input_keyword: None,
            location: Location::Client,
        }
    }
}
pub fn generate_program(
    nodes: &Vec<&str>,
    edges: &HashMap<(usize, usize), (String, bool)>,
    node_data: &HashMap<usize, NodeInfo>,
) -> Result<Program> {
    let mut prog = Program::default();
    for (idx, node) in nodes.iter().enumerate() {
        let i = idx + 1; // nodes are 1-indexed
        let nodeinfo = node_data.get(&i).unwrap();
        let mut elem = match node.as_ref() {
            "cat" => {
                let mut cat_node = CommandNode::new("cat", nodeinfo.location.clone())?;
                cat_node.set_id(i as u32);
                if let Some(path) = &nodeinfo.input_file {
                    cat_node.add_resolved_arg(String::from(path.as_path().to_str().unwrap()));
                } else {
                    bail!("Cat node without associated nodeinfo");
                }
                Elem::Cmd(cat_node)
            }
            "grep" => {
                let mut grep_node = CommandNode::new("grep", nodeinfo.location.clone())?;
                grep_node.set_id(i as u32);
                if let Some(key) = &nodeinfo.input_keyword {
                    grep_node.add_resolved_arg(key.clone());
                } else {
                    bail!("Grep node without associated nodeinfo");
                }
                if let Some(path) = &nodeinfo.input_file {
                    grep_node.add_stdin(DashStream::File(FileStream::new(
                        path.as_path(),
                        nodeinfo.location.clone(),
                    )))?;
                }
                Elem::Cmd(grep_node)
            }
            "read" => {
                let mut read_node = ReadNode::default();
                if let Some(path) = &nodeinfo.input_file {
                    read_node.add_stdin(DashStream::File(FileStream::new(
                        path.as_path(),
                        nodeinfo.location.clone(),
                    )))?;
                    Elem::Read(read_node)
                } else {
                    bail!("Read node without associated node info");
                }
            }
            "write" => {
                let mut write_node = WriteNode::default();
                if let Some((path, writetype)) = &nodeinfo.output_file {
                    match writetype {
                        WriteType::File => {
                            write_node.set_stdout(DashStream::File(FileStream::new(
                                path.as_path(),
                                nodeinfo.location.clone(),
                            )))?;
                        }
                        WriteType::Fifo => {
                            write_node.set_stdout(DashStream::Fifo(FifoStream::new(
                                path.as_path(),
                                nodeinfo.location.clone(),
                                FifoMode::WRITE,
                            )))?;
                        }
                    }
                } else {
                    bail!("Write node without associated node info");
                }
                Elem::Write(write_node)
            }
            _ => {
                bail!("Unknown node: {:?}", node);
            }
        };
        elem.set_loc(nodeinfo.location.clone());
        let id = prog.add_elem(elem);
        assert_eq!(id, i as u32);
    }

    // add all the edges
    for (edge, edge_info) in edges.iter() {
        let edge_type = &edge_info.0;
        let bufferable = &edge_info.1;
        let left = edge.0;
        let right = edge.1;
        match edge_type.as_ref() {
            "pipe" => {
                let mut pipe = PipeStream::new(left as u32, right as u32, IOType::Stdout)?;
                if *bufferable {
                    pipe.set_bufferable();
                }
                let left_node = prog.get_mut_node(left as u32).unwrap().get_mut_elem();
                {
                    left_node.add_stdout(DashStream::Pipe(pipe.clone()))?;
                }
                let right_node = prog.get_mut_node(right as u32).unwrap().get_mut_elem();
                {
                    right_node.add_stdin(DashStream::Pipe(pipe.clone()))?;
                }
            }
            "tcp" => {
                let left_location = node_data.get(&left).unwrap().location.clone();
                let right_location = node_data.get(&right).unwrap().location.clone();
                let mut pipe = NetStream::new(
                    left as u32,
                    right as u32,
                    IOType::Stdout,
                    left_location,
                    right_location,
                )?;
                if *bufferable {
                    pipe.set_bufferable();
                }
                let left_node = prog.get_mut_node(left as u32).unwrap().get_mut_elem();
                {
                    left_node.add_stdout(DashStream::Tcp(pipe.clone())).unwrap()
                }
                let right_node = prog.get_mut_node(right as u32).unwrap().get_mut_elem();
                {
                    right_node.add_stdin(DashStream::Tcp(pipe.clone())).unwrap()
                }
            }
            _ => {
                bail!(
                    "Given edge type {:?} not in the list of edge types",
                    edge_type
                );
            }
        }
        prog.add_unique_edge(left as u32, right as u32);
    }
    Ok(prog)
}
