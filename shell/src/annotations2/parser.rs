use super::argument_matcher::ArgMatch;
use super::cmd_parser::CmdParser;
use super::grammar::parse_annotation_file;
use dash::util::Result;
use std::collections::HashMap;
/// Keeps track of all the annotations and matches command line syntax with a particular annotation
pub struct Parser {
    cmd_parsers: HashMap<String, CmdParser>,
}

impl Parser {
    /// Constructs a new parser from a file containing various annotations.
    pub fn new(annotations_file: &str) -> Result<Self> {
        let mut parser_map: HashMap<String, CmdParser> = Default::default();
        let cmds = parse_annotation_file(annotations_file)?;

        for cmd in cmds {
            if parser_map.contains_key(&cmd.command_name) {
                let parser: &mut CmdParser = parser_map.get_mut(&cmd.command_name).unwrap(); // Should be safe to unwrap here
                parser.add_annotation(cmd)?;
            } else {
                let name = cmd.command_name.clone();
                let mut parser = CmdParser::new(&name);
                parser.add_annotation(cmd)?;
                parser_map.insert(name, parser);
            }
        }

        Ok(Parser {
            cmd_parsers: parser_map,
        })
    }
    /// Constructs a new parser from a hashmap containing various CmdParsers.
    pub fn construct(map: HashMap<String, CmdParser>) -> Self {
        Parser { cmd_parsers: map }
    }

    /// Takes the specified invocation and returns a possible argument matcher.
    pub fn match_invocation(&self, cmd: &str, invocation: Vec<String>) -> Result<ArgMatch> {
        match self.find_parser_key(cmd, &invocation) {
            Some(cmd_parser_name) => {
                let parser: &CmdParser = self.cmd_parsers.get(&cmd_parser_name).unwrap();
                parser.parse_command(&invocation)
            }
            None => Ok(self.default_parse(cmd, &invocation)),
        }
    }

    /// Searches through available parsers and returns key for parser, if one exists.
    fn find_parser_key(&self, cmd: &str, invocation: &Vec<String>) -> Option<String> {
        for (parser_name, _) in self.cmd_parsers.iter() {
            let name_list: Vec<String> = parser_name
                .clone()
                .split(" ")
                .map(|x| x.to_string())
                .collect();
            if name_list.len() == 1 {
                if cmd.to_string() == name_list[0] {
                    return Some(name_list[0].clone());
                } else {
                    continue;
                }
            } else {
                let num_args = name_list.len() - 1;
                // now join a string with the command name and the first n string args
                let mut command_name = cmd.to_string();
                if invocation.len() < num_args {
                    // this parser isn't going to work
                    continue;
                }
                let args = invocation.clone();
                for i in 0..num_args {
                    command_name.push_str(" ");
                    command_name.push_str(args[i].as_str());
                }
                if command_name == *parser_name {
                    return Some(parser_name.clone());
                }
            }
        }

        None
    }

    /// Default parse when no other parser is available.
    fn default_parse(&self, cmd: &str, invocation: &Vec<String>) -> ArgMatch {
        ArgMatch::new_default(cmd, invocation)
    }
}
