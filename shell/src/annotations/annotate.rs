extern crate dash;
extern crate nom;

use dash::util::Result;
use failure::bail;
use nom::*;
use std::*;

macro_rules! named_complete {
    ($name:ident<$t:ty>, $submac:ident!( $($args:tt)* )) => (
        fn $name( i: nom::types::CompleteByteSlice ) -> nom::IResult<nom::types::CompleteByteSlice, $t, u32> {
            $submac!(i, $($args)*)
        }
    );
    (pub $name:ident<$t:ty>, $submac:ident!( $($args:tt)* )) => (
        pub fn $name( i: nom::types::CompleteByteSlice ) -> nom::IResult<nom::types::CompleteByteSlice, $t, u32> {
            $submac!(i, $($args)*)
        }
    )
}

#[derive(Debug)]
struct Annotation {
    name: String, // command name
    options: Vec<Argument>,
}

#[derive(Debug, PartialEq, Eq)]
enum Argument {
    Opt(OptArg),
    File(FileType),
}

#[derive(Debug, PartialEq, Eq)]
struct OptArg {
    delimeter: String, // how this option is parsed
    info: SingleOption,
}

#[derive(Debug, PartialEq, Eq)]
struct SingleOption {
    name: String,
    num_args: u8,  // default: 0 (just a flag)
    is_file: bool, // default: false
    delim: String, // default: " "
}

#[derive(Debug, PartialEq, Eq)]
enum OptionInfo {
    Name(String),
    NumArgs(u8),
    IsFile,
    Delim(String),
}

#[derive(Debug, PartialEq, Eq)]
enum FileType {
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
impl Annotation {
    // example annotation:
    // by default: delimeter is a space and is_file is false
    // [commandname]: OPT:-[name:a|name:b,num:1,delim:" "|name:argname,num:num,delim:"delim",is_file|...] OPT:--[a|b:1," "|argname:num,"delim"] FILE:[]
    // [commandname]: delimeter[options] files[SINGLE|MULTIPLE|PATTERN]
    pub fn new(ann: &str) -> Result<Annotation> {
        match parse_annotation(CompleteByteSlice(ann.as_bytes())) {
            Ok(a) => Ok(a.1),
            Err(e) => bail!("{:?}", e),
        }
    }
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
