extern crate dash;
extern crate shellwords;
use super::annotate::{parse_annotation_file, Annotation};
use super::fileinfo::{parse_mount_file, FileMap};
use super::old_ast;
use dash::dag::node;
use dash::util::Result;
use failure::bail;
use nom::*;
use shellwords::split;
use std::collections::HashMap;
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
        // TODO: per command -- divide by pipes or & or ; (or could just support pipes for now)
        // use annotations to figure out which things might be files and construct the graph
        // directing stdout/stderr to the right place based on the parsing
        // Execute the command
        return old_ast::parse_input(String::from(command));
    }

    fn find_annotation(&self, name: String) -> Option<Annotation> {
        for ann in &self.annotations {
            if ann.is_same_cmd(&name) {
                return Some(ann.clone());
            }
        }
        None
    }

    pub fn parse_single_command(&self, command: &str) -> Result<node::Program> {
        // TODO: this crate is only compatible with the Unix shell
        let shell_split = match split(command) {
            Ok(s) => s,
            Err(e) => {
                bail!("Parse Error: {:?}", e);
            }
        };

        // TODO: if no annotation is present -- just parse the command as normal and return that;
        // but would need to make sure the execution engine is solid
        let ann = match self.find_annotation(shell_split[0].clone()) {
            Some(a) => a,
            None => {
                println!(
                    "Warning: could not find annotation for {:?}",
                    &shell_split[0]
                );
                return old_ast::parse_input(String::from(command));
            }
        };
        println!("ann: {:?}", ann);
        println!("shell splits: {:?}", shell_split);
        ann.parse_command(shell_split);
        return old_ast::parse_input(String::from(command));
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

#[cfg(test)]
mod tests {
    use super::*;

    fn get_tar_parser() -> Parser {
        let mut tar_options: Vec<Argument> = Vec::new();
        tar_options.push(Argument::Opt(OptArg {
            delimeter: "-".to_string(),
            info: SingleOption {
                name: "f".to_string(),
                num_args: 1,
                is_file: true,
                delim: " ".to_string(),
            },
        }));
        tar_options.push(Argument::Opt(OptArg {
            delimeter: "-".to_string(),
            info: SingleOption {
                name: "x".to_string(),
                num_args: 0,
                is_file: false,
                delim: " ".to_string(),
            },
        }));
        tar_options.push(Argument::Opt(OptArg {
            delimeter: "-".to_string(),
            info: SingleOption {
                name: "z".to_string(),
                num_args: 0,
                is_file: false,
                delim: " ".to_string(),
            },
        }));
        tar_options.push(Argument::Opt(OptArg {
            delimeter: "-".to_string(),
            info: SingleOption {
                name: "C".to_string(),
                num_args: 1,
                is_file: true,
                delim: " ".to_string(),
            },
        }));
        let tar_annotation = Annotation {
            name: "tar".to_string(),
            options: tar_options,
        };

        let mut folder_map: FileMap = HashMap::default();
        folder_map.insert("/mod/foo".to_string(), "127.0.0.1".to_string());

        let parser = Parser {
            folders: folder_map,
            annotations: vec![tar_annotation],
        };
        parser
    }
    #[test]
    fn test_tar_parse() {
        let parser = get_tar_parser();
        // try to parse a command to see what happens!
        parser.parse_single_command("tar -xzf /mod/foo/foo.tar -C /mod/foo");
        assert!(false);
    }
}

// what decisions do we want to do?
// is everything is remote?
// input remote:
// input is local
// knowing if things are inputs and outputs
// input_file and output_file
// write a BNF
// is the remote executable or not -- some sort of box we don't want to open
//
// workloads: how do we know if it
// it's good enough or bad enough (can make microbenchmarks)
// using nfs on ec2 (home directory to persist over restart)
// checked out code repo -- unzipping it took 8 minutes (things like cp are equally)
// contained 30000 small files
// each one had to do these small ops - OPEN, WRITE, CLOSE
// One of the tensorflow tutorials
// does the async one cover this?
// give some examples of things you can't cover
// we can't do tar - (stdin) e.g.
