extern crate dash;
extern crate nom;

use super::old_ast;
use dash::dag::{node, stream};
use dash::util::Result;
use failure::bail;
use nom::*;
use std::fs::File;
use std::io::{self, prelude::*, BufReader};
use std::*;

#[derive(Debug, Clone)]
pub struct Annotation {
    pub name: String, // command name
    pub options: Vec<Argument>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Argument {
    Opt(OptArg),
    File(FileType),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct OptArg {
    pub delimeter: String, // how this option is parsed
    pub info: SingleOption,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SingleOption {
    pub name: String,
    pub num_args: u8,  // default: 0 (just a flag)
    pub is_file: bool, // default: false
    pub delim: String, // default: " " what if there are more arguments?
                       // convention: single letter: for dashes: -z9 --longer_words
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum OptionInfo {
    Name(String),
    NumArgs(u8),
    IsFile,
    Delim(String),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum FileType {
    Single,   // Single File
    Multiple, // List of Files
    Pattern,  // File pattern (e.g. *.txt)
}
use nom::types::CompleteByteSlice;
named_complete!(
    parse_option_name<OptionInfo>,
    map!(
        do_parse!(tag: tag!("name:") >> name: next_alphabetic >> (name)),
        |name: &str| { OptionInfo::Name(name.to_string()) }
    )
);

use nom::is_alphanumeric;
named_complete!(
    pub next_alphabetic<&str>,
    map!(
        take_while1!(|u: u8| is_alphanumeric(u)),
        |n: CompleteByteSlice| str::from_utf8(n.0).unwrap()
        )
);

named_complete!(
    parse_option_num_args<OptionInfo>,
    map!(
        do_parse!(tag!("num_args:") >> dig: digit1 >> (dig)),
        |s: CompleteByteSlice| {
            let st = str::from_utf8(s.0).unwrap();
            OptionInfo::NumArgs(st.parse::<u8>().unwrap())
        }
    )
);

named_complete!(
    parse_option_file<OptionInfo>,
    do_parse!(tag!("is_file") >> (OptionInfo::IsFile))
);

named_complete!(
    parse_option_delim<OptionInfo>,
    do_parse!(
        tag!("delim:")
            >> delim:
                map!(
                    alt!(tag!("--") | tag!("-") | tag!("==") | tag!("=") | tag!(" ")),
                    |n: CompleteByteSlice| { str::from_utf8(n.0).unwrap() }
                )
            >> (OptionInfo::Delim(delim.to_string()))
    )
);

named_complete!(
    parse_option_info<OptionInfo>,
    alt!(parse_option_name | parse_option_num_args | parse_option_delim | parse_option_file)
);

named_complete!(
    parse_single_option<SingleOption>,
    map!(
        many1!(do_parse!(
            info: delimited!(opt!(tag!(",")), parse_option_info, opt!(tag!(","))) >> (info)
        )),
        |vec_options: Vec<OptionInfo>| {
            let mut opt = SingleOption {
                name: String::from(""),
                num_args: 0,
                is_file: false,
                delim: String::from(" "),
            };
            // TODO: return error if it's length 1 and name is not provided
            for info in vec_options {
                match info {
                    OptionInfo::Name(name) => opt.name = name,
                    OptionInfo::IsFile => opt.is_file = true,
                    OptionInfo::Delim(d) => opt.delim = String::from(d),
                    OptionInfo::NumArgs(n) => opt.num_args = n,
                }
            }
            opt
        }
    )
);

named_complete!(
    parse_option_set<Vec<SingleOption>>,
    many1!(do_parse!(
        opt: parse_single_option >> opt!(tag!("|")) >> (opt)
    ))
);

named_complete!(
    parse_options<Vec<Argument>>,
    map!(
        do_parse!(
            tag!("OPT:")
                >> delim: map!(take_until!("["), |n: CompleteByteSlice| {
                    str::from_utf8(n.0).unwrap()
                })
                >> option_set: delimited!(tag!("["), parse_option_set, tag!("]"))
                >> (delim, option_set)
        ),
        |(delim, option_set): (&str, Vec<SingleOption>)| {
            let mut ret: Vec<Argument> = vec![];
            for opt in option_set {
                let opt_arg = OptArg {
                    delimeter: delim.to_string(),
                    info: opt,
                };
                ret.push(Argument::Opt(opt_arg))
            }
            ret
        }
    )
);

// todo: but file could actually mean any one of these things
named_complete!(
    parse_file_type<FileType>,
    alt!(
        tag!("SINGLE") => {|_| FileType::Single} |
        tag!("MULTIPLE") => {|_| FileType::Multiple}  |
        tag!("PATTERN") => {|_| FileType::Pattern}
    )
);

named_complete!(
    parse_file<Vec<Argument>>,
    map!(
        do_parse!(tag!("FILE:") >> file_type: parse_file_type >> (file_type)),
        |(file_type): (FileType)| {
            let mut ret: Vec<Argument> = vec![];
            ret.push(Argument::File(file_type));
            ret
        }
    )
);

named_complete!(
    parse_arguments<Vec<Argument>>,
    map!(
        many1!(do_parse!(
            opt!(tag!(" ")) >> ret: alt!(parse_file | parse_options) >> (ret)
        )),
        |vec: Vec<Vec<Argument>>| {
            let mut ret: Vec<Argument> = Vec::new();
            for v in vec {
                for a in v {
                    ret.push(a);
                }
            }
            ret
        }
    )
);

named_complete!(
    parse_annotation<Annotation>,
    map!(
        do_parse!(
            name: next_alphabetic >> // can only have alphabetic names
            tag!(":") >>
            arg_list: parse_arguments >>
            (name, arg_list)
        ),
        |(name, arg_list): (&str, Vec<Argument>)| {
            Annotation {
                name: String::from(name),
                options: arg_list,
            }
        }
    )
);
/* Parses the annotation for a certain command that generates a custom parser
 *
 *
 */
fn test_option_in_word(option: &str, word: &str) -> bool {
    word.contains(option)
}

fn is_option(word: &str) -> bool {
    word.contains("-")
}
impl Annotation {
    // example annotation:
    // by default: delimeter is a space and is_file is false
    // TODO: write a grammar first of the things that you can express (as ebnf as bnf)
    // this is just specific syntax (does name, delimeter, is_file, delim)
    // is delim btwn argname and 1st one or between more
    // what is the coverage of the structs, what is the grammar of options you can parse
    // [commandname]: OPT:-[name:a|name:b,num:1,delim:" "|name:argname,num:2,delim:"delim",is_file|...] OPT:--[a|b:1," "|argname:num,"delim"] FILE:[]
    // [commandname]: delimeter[options] files[SINGLE|MULTIPLE|PATTERN]
    pub fn new(ann: &str) -> Result<Annotation> {
        match parse_annotation(CompleteByteSlice(ann.as_bytes())) {
            Ok(a) => Ok(a.1),
            Err(e) => bail!("{:?}", e),
        }
    }

    /*
     * Parses the split of shell words to work with this annotation
     */
    pub fn parse_command(&self, cmd: Vec<String>) -> Result<node::Program> {
        // TODO: figure out a way to make sure that you can handle these cases separately
        // break into further options
        let combine_options = true;
        let mut actual_options: Vec<String> = Vec::new();
        for word in &cmd {
            if word.clone() == self.name {
                continue;
            }
            if is_option(&word) {
                for (i, c) in word.chars().enumerate() {
                    let mut delimeter = "-".to_string();
                    // do something with character `c` and index `i`
                    if i == 0 {
                        delimeter = c.to_string();
                    } else {
                        let mut next = delimeter.clone();
                        next.push(c);
                        actual_options.push(next);
                    }
                }
            } else {
                actual_options.push(word.clone());
            }
        }

        let mut cmd_args: Vec<node::OpArg> = Vec::new();
        // iterate over the list --> if an option that has 1 argument is provided
        let mut i: usize = 0;
        while i != actual_options.len() {
            let opt = &actual_options[i].clone();
            let matching_option: OptArg = self.check_option(&opt).unwrap();

            if matching_option.info.num_args == 0 {
                cmd_args.push(node::OpArg::Arg(opt.clone()));
                i += 1;
                continue;
            } else {
                if matching_option.info.is_file {
                    for j in i + 1..i + matching_option.info.num_args as usize {
                        let arg = &actual_options[j].clone();
                        let datastream: stream::DataStream = stream::DataStream {
                            stream_type: stream::StreamType::RemoteFile,
                            name: arg.clone(),
                        };
                        cmd_args.push(node::OpArg::Stream(datastream));
                    }
                } else {
                    for j in i + 1..i + matching_option.info.num_args as usize {
                        let arg = &actual_options[j].clone();
                        cmd_args.push(node::OpArg::Arg(arg.clone()));
                    }
                }
            }
            i += matching_option.info.num_args as usize;
            continue;
        }

        let shell_command = node::Op::ShellCommand {
            name: cmd[0].clone(),
            arguments: cmd_args,
            stdin: None,
            stderr: stream::DataStream {
                stream_type: stream::StreamType::LocalStdout,
                name: "".to_string(),
            },
            stdout: stream::DataStream {
                stream_type: stream::StreamType::LocalStdout,
                name: "".to_string(),
            },
            action: node::OpAction::Run,
        };

        let mut ops: Vec<node::Op> = Vec::new();
        ops.push(shell_command);
        Ok(node::Program::new(ops))
    }

    pub fn check_option(&self, opt: &str) -> Option<OptArg> {
        // TODO: temp
        let delim = String::from("-");
        let name = (&opt[1..opt.len()]);
        for arg in &self.options {
            if let (Argument::Opt(ref field)) = arg.clone() {
                if delim == field.delimeter && name == field.info.name {
                    return Some(field.clone());
                }
            } else {
                unreachable!();
            }
            // if is option: check if it's an option that is followed by a file
            // check if the file is local or remote
            // and then append a local or remote file
        }
        return None;
    }
    // checks if the annotation refers to the same command
    pub fn is_same_cmd(&self, cmd_name: &str) -> bool {
        (String::from(cmd_name) == self.name)
    }
}

pub fn parse_annotation_file(file: &str) -> Result<Vec<Annotation>> {
    let mut ret: Vec<Annotation> = Vec::new();
    let file = File::open(file)?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line_src = line?;
        let ann = Annotation::new(line_src.as_ref())?;
        ret.push(ann)
    }
    Ok(ret)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_option_types() {
        let (_, n): (CompleteByteSlice, OptionInfo) =
            parse_option_name(CompleteByteSlice(b"name:foo")).unwrap();
        assert_eq!(n, OptionInfo::Name(String::from("foo")));

        let (_, n2): (CompleteByteSlice, OptionInfo) =
            parse_option_file(CompleteByteSlice(b"is_file")).unwrap();
        assert_eq!(n2, OptionInfo::IsFile);

        let (_, n3): (CompleteByteSlice, OptionInfo) =
            parse_option_delim(CompleteByteSlice(b"delim:--")).unwrap();
        assert_eq!(n3, OptionInfo::Delim(String::from("--")));

        let (_, n4): (CompleteByteSlice, OptionInfo) =
            parse_option_num_args(CompleteByteSlice(b"num_args:12")).unwrap();
        assert_eq!(n4, OptionInfo::NumArgs(12));
    }

    #[test]
    fn test_option_types_alt() {
        let (_, n): (CompleteByteSlice, OptionInfo) =
            parse_option_info(CompleteByteSlice(b"name:foo")).unwrap();
        assert_eq!(n, OptionInfo::Name(String::from("foo")));

        let (_, n2): (CompleteByteSlice, OptionInfo) =
            parse_option_info(CompleteByteSlice(b"is_file")).unwrap();
        assert_eq!(n2, OptionInfo::IsFile);

        let (_, n3): (CompleteByteSlice, OptionInfo) =
            parse_option_info(CompleteByteSlice(b"delim:--")).unwrap();
        assert_eq!(n3, OptionInfo::Delim(String::from("--")));

        let (_, n4): (CompleteByteSlice, OptionInfo) =
            parse_option_info(CompleteByteSlice(b"num_args:12")).unwrap();
        assert_eq!(n4, OptionInfo::NumArgs(12));
    }

    #[test]
    fn test_full_single_option() {
        let (_, n): (CompleteByteSlice, SingleOption) =
            parse_single_option(CompleteByteSlice(b"name:foo,is_file,delim:--,num_args:12,"))
                .unwrap();
        assert_eq!(
            n,
            SingleOption {
                name: "foo".to_string(),
                is_file: true,
                num_args: 12,
                delim: "--".to_string()
            }
        );
    }

    #[test]
    fn test_option_set() {
        let (_, n): (CompleteByteSlice, Vec<SingleOption>) = parse_option_set(CompleteByteSlice(
            b"name:foo,is_file,delim:--,num_args:12|name:boo,delim:=,num_args:2",
        ))
        .unwrap();
        assert_eq!(
            n[0],
            SingleOption {
                name: "foo".to_string(),
                is_file: true,
                num_args: 12,
                delim: "--".to_string()
            }
        );
        assert_eq!(
            n[1],
            SingleOption {
                name: "boo".to_string(),
                is_file: false,
                num_args: 2,
                delim: "=".to_string()
            }
        );
    }

    #[test]
    fn test_options() {
        let (_, n): (CompleteByteSlice, Vec<Argument>) = parse_options(CompleteByteSlice(
            b"OPT:-[name:foo,is_file,delim:=,num_args:12|name:boo,delim:=,num_args:2]",
        ))
        .unwrap();
        let first_info = SingleOption {
            name: "foo".to_string(),
            is_file: true,
            num_args: 12,
            delim: "=".to_string(),
        };
        let second_info = SingleOption {
            name: "boo".to_string(),
            is_file: false,
            num_args: 2,
            delim: "=".to_string(),
        };
        assert_eq!(
            n[0],
            Argument::Opt(OptArg {
                delimeter: "-".to_string(),
                info: first_info
            }),
        );
        assert_eq!(
            n[1],
            Argument::Opt(OptArg {
                delimeter: "-".to_string(),
                info: second_info
            }),
        );
    }

    #[test]
    fn test_file_type() {
        let (_, n): (CompleteByteSlice, Vec<Argument>) =
            parse_file(CompleteByteSlice(b"FILE:PATTERN")).unwrap();
        assert_eq!(n[0], Argument::File(FileType::Pattern));
    }

    #[test]
    fn test_argument_list() {
        let (_, n): (CompleteByteSlice, Vec<Argument>) = parse_arguments(CompleteByteSlice(
            b"OPT:-[name:foo,is_file,delim:=,num_args:12|name:boo,delim:=,num_args:2] FILE:SINGLE",
        ))
        .unwrap();

        let first_info = SingleOption {
            name: "foo".to_string(),
            is_file: true,
            num_args: 12,
            delim: "=".to_string(),
        };
        let second_info = SingleOption {
            name: "boo".to_string(),
            is_file: false,
            num_args: 2,
            delim: "=".to_string(),
        };
        assert_eq!(
            n[0],
            Argument::Opt(OptArg {
                delimeter: "-".to_string(),
                info: first_info
            }),
        );
        assert_eq!(
            n[1],
            Argument::Opt(OptArg {
                delimeter: "-".to_string(),
                info: second_info
            }),
        );
        assert_eq!(n[2], Argument::File(FileType::Single));
        assert!(n.len() == 3);
    }

    #[test]
    fn test_annotation() {
        let (_, ann): (CompleteByteSlice, Annotation) = parse_annotation(CompleteByteSlice(
            b"cat: OPT:-[name:foo,is_file,delim:=,num_args:12|name:boo,delim:=,num_args:2] FILE:SINGLE",
        ))
        .unwrap();
        let n: Vec<Argument> = ann.options;
        assert_eq!(ann.name, String::from("cat"));
        let first_info = SingleOption {
            name: "foo".to_string(),
            is_file: true,
            num_args: 12,
            delim: "=".to_string(),
        };
        let second_info = SingleOption {
            name: "boo".to_string(),
            is_file: false,
            num_args: 2,
            delim: "=".to_string(),
        };
        assert_eq!(
            n[0],
            Argument::Opt(OptArg {
                delimeter: "-".to_string(),
                info: first_info
            }),
        );
        assert_eq!(
            n[1],
            Argument::Opt(OptArg {
                delimeter: "-".to_string(),
                info: second_info
            }),
        );
        assert_eq!(n[2], Argument::File(FileType::Single));
        assert!(n.len() == 3);
    }
}
