extern crate dash;
extern crate shellwords;
use super::annotate::{parse_annotation_file, Annotation};
use super::fileinfo::{parse_mount_file, FileMap};
use dash::dag::{node, stream};
use dash::util::Result;
use failure::bail;
use nom::*;
pub struct Parser {
    pub annotations: Vec<Annotation>,
    pub folders: FileMap,
}

impl Parser {
    pub fn new(annotations_file: &str, folders_file: &str) -> Result<Self> {
        let anns = parse_annotation_file(annotations_file)?;
        let folders = parse_mount_file(folders_file)?;
        Ok(Parser {
            annotations: anns,
            folders: folders,
        })
    }

    pub fn parse_command(&self, command: &str) -> Result<node::Program> {
        // TODO: per command -- divide by pipes,
        // use annotations to figure out which things might be files and construct the graph
        // directing stdout/stderr to the right place based on the parsing
        // Execute the command
        unimplemented!();
    }

    pub fn get_client_folder(&self) -> String {
        // TODO: this function shouldn't exist
        assert!(self.folders.len() >= 1);
        for (file, _) in &self.folders {
            return file.clone();
        }
        "foo".to_string()
    }
}
