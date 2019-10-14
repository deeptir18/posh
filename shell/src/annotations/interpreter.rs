extern crate dash;

use dash::util::Result;
use failure::bail;

use super::annotation_parser::parse_annotation_file;
use super::fileinfo::{parse_mount_file, FileMap};
use super::grammar::*;
use super::parser::Parser;
use super::shell_interpreter;
use dash::dag::node;
use std::collections::HashMap;
pub struct Interpreter {
    pub parsers: HashMap<String, Parser>,
    pub folders: FileMap,
}

impl Interpreter {
    pub fn new(annotations_file: &str, folders_file: &str) -> Result<Self> {
        let folders = parse_mount_file(folders_file)?;
        let mut parser_map: HashMap<String, Parser> = Default::default();
        let cmds = parse_annotation_file(annotations_file)?;

        for cmd in cmds {
            if parser_map.contains_key(&cmd.command_name) {
                let parser: &mut Parser = parser_map.get_mut(&cmd.command_name).unwrap(); // Should be safe to unwrap here
                parser.add_annotation(cmd)?;
            } else {
                let name = cmd.command_name.clone();
                let mut parser = Parser::new(&name);
                parser.add_annotation(cmd);
                parser_map.insert(name, parser);
            }
        }

        Ok(Interpreter {
            parsers: parser_map,
            folders: folders,
        })
    }

    pub fn parse_command(&self, command: &str) -> Result<node::Program> {
        unimplemented!();
        // TODO:
        // 1. Have some sort of quick shell-level parser that breaks the command into an
        //    intermediate representation with various piped portions and the stderr, stdout, and
        //    stdin of each portion.
        // 2. Within each command -- use the annotation parser to break each argument into TYPES
        //    (this part is almost done).
        // 3. Once you have the argument and types, along with the shell level information, turn
        //    this into a node::Program that can be executed on the client or on the server.
        //    Then you can think about modifying the execution engine. And have a real demo to show
        //    them today!
        //  4. The different annotations (today) that would be good to have are: tar, grep, cat,
        //     sort, wc, diff, ...

        // shell level parser
        let mut program: node::Program = Default::default();
        let mut shell_program: Vec<(node::Node, Vec<String>)> =
            shell_interpreter::shell_split(command)?;
        for (op, args) in shell_program {
            // if no args, just continue
            if args.len() == 0 {
                program.add_op(op);
                continue;
            }

            // if a parser exists, try to parse the command into a parsed command
            if self.parsers.contains_key(&op.name) {
                // TODO: might not need to be mutable here
                let parser: &mut Parser = self.parsers.get_mut(&op.name).unwrap();

                // use the parser to turn the String args into "types"
                let typed_args = parser.parse_command(args)?;

                // interpret typed arguments and add them to the op
                self.interpret_types(typed_args, &mut op);
            } else {
                for arg in args {
                    op.add_arg(node::OpArg::Arg(arg));
                }
            }

            // now add op to the program
            program.add_op(op);
        }
        self.assign_location(prog);
        Ok(program)
    }

    /// Turn the parsed command into a graph node
    /// Using the mount information and the file information -- turn the typed arguments into a
    /// graph node
    /// Need to decide:
    /// - What arguments are *FileStreams*
    /// - If it needs to be opened on the remote machine -- need to strip the prefix
    ///     - If it's a *list* of files, need to strip the prefix on each
    ///     - Need to fully RESOLVE the file first if it's a relative path to do the match
    ///     - Server program would need to resolve it there
    /// -  
    fn interpret_types(&mut self, typed_args: ParsedCommand, op: &mut node::Node) {
        unimplemented!();
        // iterate through the typed arg and add opargs, assigning resolved FileStream args
        // whenever necessary
        // This involves looking at the mount information and seeing if files match any of those
        // mounts
        // What would change if this was not the nfs backend but the nfs ec2 backend?
        // - Maybe ask phil if it's a good idea to be implementing this now with the NFS example in
        // mind
        // - Or thinking about other backends too
        // With the ec2-s3 backend ==> theoretically if it's mounted you'd do the same thing? but
        // resolve it differently
    }

    fn assign_location(&mut self, prog: &mut node::Program) {
        unimplemented!();
    }
}
