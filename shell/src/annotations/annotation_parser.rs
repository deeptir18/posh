extern crate dash;
extern crate nom;

use super::grammar::*;
use dash::util::Result;
use failure::bail;
use nom::types::CompleteByteSlice;
use nom::*;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::*;
named_complete!(
    parse_specific_size_amt<SizeInfo>,
    map!(
        do_parse!(tag!("size:") >> digit: digit1 >> (digit)),
        |n: CompleteByteSlice| {
            let st = str::from_utf8(n.0).unwrap();
            SizeInfo::Num(st.parse::<u64>().unwrap())
        }
    )
);

// eventually, list delimiter should only be specific things that are allowed
// i.e. space or comma or semicolon or something
named_complete!(
    parse_list_delimiter<SizeInfo>,
    map!(
        do_parse!(
            tag!("list_separator:") >> tag!("(") >> sep: take_until!(")") >> tag!(")") >> (sep)
        ),
        |n: CompleteByteSlice| {
            let st = str::from_utf8(n.0).unwrap();
            match st {
                " " => SizeInfo::Delimiter(ListSeparator::Space),
                "," => SizeInfo::Delimiter(ListSeparator::Comma),
                _ => {
                    panic!("Provided list separator that is not space or comma");
                }
            }
        }
    )
);

named_complete!(
    parse_specific_size<ParamSize>,
    map!(
        do_parse!(
            tag!("specific_size(")
                >> size_info:
                    count!(
                        do_parse!(
                            ans: alt!(parse_specific_size_amt | parse_list_delimiter)
                                >> opt!(tag!(","))
                                >> (ans)
                        ),
                        2
                    )
                >> tag!(")")
                >> (size_info)
        ),
        |size_info: Vec<SizeInfo>| {
            let mut delimiter: ListSeparator = Default::default();
            let mut size: u64 = 1;
            for info in size_info {
                match info {
                    SizeInfo::Num(num) => {
                        size = num;
                    }
                    SizeInfo::Delimiter(delim) => {
                        delimiter = delim;
                    }
                }
            }
            ParamSize::SpecificSize(size, delimiter)
        }
    )
);

named_complete!(
    parse_list<ParamSize>,
    map!(
        do_parse!(tag!("list(") >> sep: parse_list_delimiter >> tag!(")") >> (sep)),
        |sep: SizeInfo| {
            match sep {
                SizeInfo::Delimiter(delim) => ParamSize::List(delim),
                SizeInfo::Num(_) => {
                    unreachable!(); // parse list delimiter cannot return a number
                }
            }
        }
    )
);

named_complete!(
    parse_type<Info>,
    map!(
        do_parse!(
            tag!("type:")
                >> argtype: alt!(tag!("input_file") | tag!("output_file") | tag!("str"))
                >> (argtype)
        ),
        |n: CompleteByteSlice| {
            match str::from_utf8(n.0).unwrap() {
                "input_file" => Info::ParamType(ArgType::InputFile),
                "output_file" => Info::ParamType(ArgType::OutputFile),
                "str" => Info::ParamType(ArgType::Str),
                _ => {
                    panic!("Non allowed shell type allowed");
                }
            }
        }
    )
);

named_complete!(
    parse_size<Info>,
    map!(
        do_parse!(
            tag!("size:")
                >> size: alt!(
                    map!(tag!("0"), |_| { ParamSize::Zero })
                        | map!(tag!("1"), |_| { ParamSize::One })
                        | parse_specific_size
                        | parse_list
                )
                >> (size)
        ),
        |param_size: ParamSize| { Info::Size(param_size) }
    )
);

named_complete!(
    parse_default<Info>,
    map!(
        do_parse!(
            tag!("default_value:")
                >> word: delimited!(tag!("\""), take_until!("\""), tag!("\""))
                >> (word)
        ),
        |s: CompleteByteSlice| {
            let st = str::from_utf8(s.0).unwrap();
            Info::DefaultValue(String::from(st))
        }
    )
);

named_complete!(
    parse_short<Info>,
    map!(
        do_parse!(tag!("short:") >> word: alpha1 >> (word)),
        |s: CompleteByteSlice| {
            let st = str::from_utf8(s.0).unwrap();
            Info::Short(String::from(st))
        }
    )
);

named_complete!(
    parse_long_name<String>,
    map!(many1!(alt!(alpha1 | tag!("-") | tag!("_"))), |elts: Vec<
        CompleteByteSlice,
    >| {
        let mut name = "".to_string();
        for elt in elts.iter() {
            let str_repr = str::from_utf8(elt.0).unwrap();
            name.push_str(str_repr);
        }
        name
    })
);

named_complete!(
    parse_command_name<String>,
    map!(many1!(alt!(alpha1 | tag!(" ") | tag!("-"))), |elts: Vec<
        CompleteByteSlice,
    >| {
        let mut name = "".to_string();
        for elt in elts.iter() {
            let str_repr = str::from_utf8(elt.0).unwrap();
            name.push_str(str_repr);
        }
        name
    })
);

// TODO: need to include dashes here somehow
named_complete!(
    parse_long<Info>,
    map!(
        do_parse!(tag!("long:") >> word: parse_long_name >> (word)),
        |s: String| { Info::Long(s) }
    )
);
named_complete!(
    parse_desc<Info>,
    map!(
        do_parse!(tag!("desc:") >> tag!("(") >> word: take_until!(")") >> tag!(")") >> (word)),
        |s: CompleteByteSlice| {
            let st = str::from_utf8(s.0).unwrap();
            Info::Desc(String::from(st))
        }
    )
);

named_complete!(
    parse_multiple<Info>,
    map!(tag!("multiple"), |_| { Info::Multiple })
);

named_complete!(
    parse_splittable<Info>,
    map!(tag!("splittable"), |_| { Info::Splittable })
);
named_complete!(
    parse_individual_info<Info>,
    alt!(
        parse_type
            | parse_size
            | parse_default
            | parse_short
            | parse_long
            | parse_desc
            | parse_multiple
            | parse_splittable
    )
);
named_complete!(
    parse_param<Result<Param>>,
    map!(
        many1!(do_parse!(
            info: parse_individual_info >> opt!(tag!(",")) >> (info)
        )),
        |vec_options: Vec<Info>| {
            let mut param: Param = Default::default();
            for info in vec_options {
                match info {
                    Info::ParamType(t) => {
                        param.param_type = t;
                    }
                    Info::Size(s) => {
                        param.size = s;
                    }
                    Info::DefaultValue(s) => {
                        param.default_value = s;
                    }
                    Info::Multiple => {
                        param.multiple = true;
                    }
                    Info::Splittable => {
                        param.splittable = true;
                    }
                    _ => {
                        bail!("Could not parse individual param: provide only type, size, default value");
                    }
                }
            }
            Ok(param)
            // TODO: make sure they provide information to parse a param
        }
    )
);
named_complete!(
    parse_opt_with_param<Result<(Opt, Param)>>,
    map!(
        many1!(do_parse!(
            info: delimited!(opt!(tag!(",")), parse_individual_info, opt!(tag!(","))) >> (info)
        )),
        |vec_options: Vec<Info>| {
            let mut param: Param = Default::default();
            let mut opt: Opt = Default::default();

            for info in vec_options {
                match info {
                    Info::ParamType(t) => {
                        param.param_type = t;
                    }
                    Info::Size(s) => {
                        param.size = s;
                    }
                    Info::DefaultValue(s) => {
                        param.default_value = s;
                    }
                    Info::Short(s) => {
                        opt.short = s;
                    }
                    Info::Long(l) => {
                        opt.long = l;
                    }
                    Info::Desc(d) => {
                        opt.desc = d;
                    }
                    Info::Multiple => {
                        param.multiple = true;
                    }
                    Info::Splittable => {
                        param.splittable = true;
                    }
                    _ => {
                        bail!("Could not parse individual param: provide only type, size, default value");
                    }
                }
            }
            Ok((opt, param))
            // TODO: make sure they provide information to parse a param
        }
    )
);
named_complete!(
    parse_flag<Result<Opt>>,
    map!(
        many1!(do_parse!(
            info: delimited!(opt!(tag!(",")), parse_individual_info, opt!(tag!(","))) >> (info)
        )),
        |vec_options: Vec<Info>| {
            let mut opt: Opt = Default::default();
            for info in vec_options {
                match info {
                    Info::Short(s) => {
                        opt.short = s;
                    }
                    Info::Long(l) => {
                        opt.long = l;
                    }
                    Info::Desc(d) => {
                        opt.desc = d;
                    }
                    Info::Multiple => {
                        opt.multiple = true;
                    }
                    _ => {
                        bail!("Could not parse individual opt: provide only short, long, desc.");
                    }
                }
            }
            Ok(opt)
        }
    )
);

named_complete!(
    parse_flag_set<Vec<Result<Opt>>>,
    many1!(do_parse!(
        tag!("(") >> flag: parse_flag >> tag!(")") >> opt!(tag!(",")) >> (flag)
    ))
);
named_complete!(
    parse_opt_with_param_set<Vec<Result<(Opt, Param)>>>,
    many1!(do_parse!(
        tag!("(") >> opt_param: parse_opt_with_param >> tag!(")") >> opt!(tag!(",")) >> (opt_param)
    ))
);
named_complete!(
    parse_param_set<Vec<Result<Param>>>,
    many1!(do_parse!(
        tag!("(") >> param: parse_param >> tag!(")") >> opt!(tag!(",")) >> (param)
    ))
);

named_complete!(
    parse_params<Result<Vec<Argument>>>,
    map!(
        do_parse!(
            tag!("PARAMS:") >> tag!("[") >> param_set: parse_param_set >> tag!("]") >> (param_set)
        ),
        |option_set: Vec<Result<Param>>| {
            let mut ret: Vec<Argument> = Vec::new();
            for opt in option_set {
                match opt {
                    Ok(o) => {
                        ret.push(Argument::LoneParam(o));
                    }
                    Err(e) => {
                        bail!("Error in LoneParam: {:?}", e);
                    }
                }
            }
            Ok(ret)
        }
    )
);

named_complete!(
    parse_opt_with_params<Result<Vec<Argument>>>,
    map!(
        do_parse!(
            tag!("OPTPARAMS:")
                >> tag!("[")
                >> param_set: parse_opt_with_param_set
                >> tag!("]")
                >> (param_set)
        ),
        |option_set: Vec<Result<(Opt, Param)>>| {
            let mut ret: Vec<Argument> = vec![];
            for opt in option_set {
                match opt {
                    Ok(o) => {
                        ret.push(Argument::OptWithParam(o.0, o.1));
                    }
                    Err(e) => {
                        bail!("Error in LoneParam: {:?}", e);
                    }
                }
            }
            Ok(ret)
        }
    )
);

named_complete!(
    parse_flags<Result<Vec<Argument>>>,
    map!(
        do_parse!(
            tag!("FLAGS:") >> tag!("[") >> param_set: parse_flag_set >> tag!("]") >> (param_set)
        ),
        |option_set: Vec<Result<Opt>>| {
            let mut ret: Vec<Argument> = vec![];
            for opt in option_set {
                match opt {
                    Ok(o) => {
                        ret.push(Argument::LoneOption(o));
                    }
                    Err(e) => {
                        bail!("Error in LoneParam: {:?}", e);
                    }
                }
            }
            Ok(ret)
        }
    )
);
named_complete!(
    parse_arguments<Result<Vec<Argument>>>,
    map!(
        many1!(do_parse!(
            opt!(tag!(" "))
                >> ret: alt!(parse_flags | parse_opt_with_params | parse_params)
                >> (ret)
        )),
        |vec: Vec<Result<Vec<Argument>>>| {
            let mut ret: Vec<Argument> = Vec::new();

            for v in vec {
                match v {
                    Ok(args) => {
                        for arg in args {
                            ret.push(arg);
                        }
                    }
                    Err(e) => {
                        bail!("Error in parsing argsa: {:?}", e);
                    }
                }
            }

            Ok(ret)
        }
    )
);

named_complete!(
    parse_long_arg_single_dash<IndividualParseOption>,
    map!(tag!("long_arg_single_dash"), {
        |_| IndividualParseOption::LongArgSingleDash
    })
);

named_complete!(
    parse_splittable_across_input<IndividualParseOption>,
    map!(tag!("splittable_across_input"), {
        |_| IndividualParseOption::SplittableAcrossInput
    })
);

named_complete!(
    parse_reduces_input<IndividualParseOption>,
    map!(tag!("reduces_input"), {
        |_| IndividualParseOption::ReducesInput
    })
);

named_complete!(
    parse_needs_current_dir<IndividualParseOption>,
    map!(tag!("needs_current_dir"), {
        |_| IndividualParseOption::NeedsCurrentDir
    })
);

named_complete!(
    parse_individual_parsing_option<IndividualParseOption>,
    alt!(
        parse_long_arg_single_dash
            | parse_splittable_across_input
            | parse_reduces_input
            | parse_needs_current_dir
    )
);
named_complete!(
    parse_parsing_options<Result<ParsingOptions>>,
    map!(
        many0!(do_parse!(
            info: delimited!(
                opt!(tag!(",")),
                parse_individual_parsing_option,
                opt!(tag!(","))
            ) >> (info)
        )),
        |vec_options: Vec<IndividualParseOption>| {
            let mut parsing_opt = ParsingOptions::default();
            for opt in vec_options.iter() {
                match opt {
                    IndividualParseOption::LongArgSingleDash => {
                        parsing_opt.long_arg_single_dash = true
                    }
                    IndividualParseOption::SplittableAcrossInput => {
                        parsing_opt.splittable_across_input = true
                    }
                    IndividualParseOption::ReducesInput => {
                        parsing_opt.reduces_input = true;
                    }
                    IndividualParseOption::NeedsCurrentDir => {
                        parsing_opt.needs_current_dir = true;
                    }
                }
            }
            Ok(parsing_opt)
        }
    )
);

// TODO: how do we add in more general options here -- how to fit in this "long_arg_single_dash"
// syntax so it works?
// Then, need to define the syntax for splitting commands across inputs
named_complete!(
    parse_annotation<Result<Command>>,
    map!(
        do_parse!(
            name: parse_command_name
                >> opt!(tag!("["))
                >> parsing_options: parse_parsing_options
                >> opt!(tag!("]"))
                >> tag!(":")
                >> arg_list: parse_arguments
                >> (name, parsing_options, arg_list)
        ),
        |(name, parsing_options, arg_list): (
            String,
            Result<ParsingOptions>,
            Result<Vec<Argument>>
        )| {
            let opts = match parsing_options {
                Ok(o) => o,
                Err(e) => {
                    bail!("Could not parse parsing options: {:?}", e);
                }
            };
            let args = match arg_list {
                Ok(a) => a,
                Err(e) => {
                    bail!("Could not parse args: {:?}", e);
                }
            };
            Ok(Command {
                command_name: name,
                args: args,
                parsing_options: opts,
            })
        }
    )
);

/// Constructor that parses the annotation string into a struct.
/// Assumes annotations are of the form:
/// command_name[PARSING_OPTIONS:long_arg_single_dash]: FLAGS:[(short:o,long:option,desc:desc),()...
///                 OPTPARAMS:(short:o,long:option,desc:desc,num:[zero|one|many(separator:",")|specific_size(size:size,separator:",")], type:[input_file|output_file|str]),()...]
///                 PARAMS:(num:[one|many|specific_size...]])
///
/// The annotation format corresponds to the grammar in grammar.rs
impl Command {
    pub fn new(ann: &str) -> Result<Self> {
        match parse_annotation(CompleteByteSlice(ann.as_bytes())) {
            Ok(a) => match a.1 {
                Ok(annotation) => Ok(annotation),
                Err(e) => bail!("Failed parsing annotaton: {:?}", e),
            },
            Err(e) => bail!("Failed parsing annotaton: {:?}", e),
        }
    }

    pub fn long_arg_single_dash(&self) -> bool {
        self.parsing_options.long_arg_single_dash
    }

    /// Used for checking if an option passed into the program matches a long option.
    /// For options that are preceeded by a single dash instead of multiple dashes.
    // TODO: this is sort of hacky -> e.g. what is a better way to do this check?
    pub fn check_matches_long_option(&self, word: &str) -> Option<Argument> {
        // first check if this argument starts with a -
        match word.chars().next() {
            Some(ch) => {
                if ch != "-".chars().next().unwrap() {
                    return None;
                }
            }
            None => {
                return None;
            }
        }

        for arg in self.args.iter() {
            match arg {
                Argument::LoneOption(opt) => {
                    if opt.long == "" {
                        continue;
                    }
                    let name = format!("-{}", opt.long);
                    if word.to_owned().starts_with(name.as_str()) {
                        return Some(arg.clone());
                    }
                }
                Argument::OptWithParam(opt, _param) => {
                    if opt.long == "" {
                        continue;
                    }
                    let name = format!("-{}", opt.long);
                    if word.to_owned().starts_with(name.as_str()) {
                        return Some(arg.clone());
                    }
                }
                _ => {}
            }
        }
        return None;
    }
}

pub fn parse_annotation_file(file: &str) -> Result<Vec<Command>> {
    let mut ret: Vec<Command> = Vec::new();
    let file = File::open(file)?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line_src = line?;
        let cmd = Command::new(line_src.as_ref())?;
        ret.push(cmd);
    }
    Ok(ret)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_types() {
        let (_, n): (CompleteByteSlice, Info) =
            parse_type(CompleteByteSlice(b"type:input_file")).unwrap();
        assert_eq!(n, Info::ParamType(ArgType::InputFile));

        let (_, n2): (CompleteByteSlice, Info) =
            parse_type(CompleteByteSlice(b"type:output_file")).unwrap();
        assert_eq!(n2, Info::ParamType(ArgType::OutputFile));

        let (_, n3): (CompleteByteSlice, Info) =
            parse_type(CompleteByteSlice(b"type:str")).unwrap();
        assert_eq!(n3, Info::ParamType(ArgType::Str));
    }

    #[test]
    fn test_parse_size() {
        let (_, n): (CompleteByteSlice, Info) = parse_size(CompleteByteSlice(b"size:0")).unwrap();
        assert_eq!(n, Info::Size(ParamSize::Zero));

        let (_, n2): (CompleteByteSlice, Info) = parse_size(CompleteByteSlice(b"size:1")).unwrap();
        assert_eq!(n2, Info::Size(ParamSize::One));

        let (_, n3): (CompleteByteSlice, Info) =
            parse_size(CompleteByteSlice(b"size:list(list_separator:( ))")).unwrap();
        assert_eq!(n3, Info::Size(ParamSize::List(ListSeparator::Space)));

        let (_, n3): (CompleteByteSlice, Info) =
            parse_size(CompleteByteSlice(b"size:list(list_separator:(,))")).unwrap();
        assert_eq!(n3, Info::Size(ParamSize::List(ListSeparator::Comma)));

        let (_, n4): (CompleteByteSlice, Info) = parse_size(CompleteByteSlice(
            b"size:specific_size(size:3,list_separator:(,))",
        ))
        .unwrap();
        assert_eq!(
            n4,
            Info::Size(ParamSize::SpecificSize(3 as u64, ListSeparator::Comma))
        );
    }

    #[test]
    fn test_parse_short() {
        let (_, n): (CompleteByteSlice, Info) = parse_short(CompleteByteSlice(b"short:o")).unwrap();
        assert_eq!(n, Info::Short("o".to_string()));
    }

    #[test]
    fn test_parse_long() {
        let (_, n): (CompleteByteSlice, Info) =
            parse_long(CompleteByteSlice(b"long:option")).unwrap();
        assert_eq!(n, Info::Long("option".to_string()));
    }

    #[test]
    fn test_parse_desc() {
        let (_, n): (CompleteByteSlice, Info) =
            parse_desc(CompleteByteSlice(b"desc:(fake description)")).unwrap();
        assert_eq!(n, Info::Desc("fake description".to_string()));
    }
    #[test]
    fn test_parse_param() {
        let (_, n): (CompleteByteSlice, Result<Param>) = parse_param(CompleteByteSlice(
            b"type:input_file,size:1,default_value:\".\"",
        ))
        .unwrap();

        let param = Param {
            param_type: ArgType::InputFile,
            size: ParamSize::One,
            default_value: ".".to_string(),
            multiple: false,
        };

        let parsed_param = match n {
            Ok(v) => v,
            Err(e) => {
                panic!("Error parsing param: {:?}", e);
            }
        };
        assert_eq!(param, parsed_param);
    }

    #[test]
    fn test_parse_param_set() {
        let (_, n): (CompleteByteSlice, Vec<Result<Param>>) = parse_param_set(CompleteByteSlice(
            b"(type:input_file,size:1,default_value:\".\"),(type:str,size:1,default_value:\"..\"),",
        ))
        .unwrap();
        let first_param = Param {
            param_type: ArgType::InputFile,
            size: ParamSize::One,
            default_value: ".".to_string(),
            multiple: false,
        };

        let second_param = Param {
            param_type: ArgType::Str,
            size: ParamSize::One,
            default_value: "..".to_string(),
            multiple: false,
        };

        assert_eq!(first_param, *n[0].as_ref().unwrap());
        assert_eq!(second_param, *n[1].as_ref().unwrap());
    }

    #[test]
    fn test_parse_params() {
        let (_, n): (CompleteByteSlice, Result<Vec<Argument>>) = parse_params(CompleteByteSlice(
            b"PARAMS:[(type:input_file,size:1,default_value:\".\"),(type:str,size:1),]",
        ))
        .unwrap();
        let second_param: Param = Param {
            param_type: ArgType::Str,
            size: ParamSize::One,
            default_value: Default::default(),
            multiple: false,
        };
        let first_param: Param = Param {
            param_type: ArgType::InputFile,
            size: ParamSize::One,
            default_value: ".".to_string(),
            multiple: false,
        };
        let arg_list = match n {
            Ok(v) => v,
            Err(e) => {
                panic!("Error parsing: {:?}", e);
            }
        };
        assert_eq!(arg_list.len(), 2);
        assert_eq!(arg_list[0], Argument::LoneParam(first_param));
        assert_eq!(arg_list[1], Argument::LoneParam(second_param));
    }

    #[test]
    fn test_flag() {
        let (_, n): (CompleteByteSlice, Result<Opt>) = parse_flag(CompleteByteSlice(
            b"short:o,long:option,multiple,desc:(foo foo)",
        ))
        .unwrap();

        let option: Opt = Opt {
            short: "o".to_string(),
            long: "option".to_string(),
            desc: "foo foo".to_string(),
            multiple: true,
        };

        assert_eq!(option, n.unwrap());
    }
    #[test]
    fn test_parse_flag_set() {
        let (_, n): (CompleteByteSlice, Vec<Result<Opt>>) = parse_flag_set(CompleteByteSlice(
            b"(short:o,long:option,desc:(foo foo)),(short:d,long:debug,desc:(debug mode)))",
        ))
        .unwrap();

        let option1: Opt = Opt {
            short: "o".to_string(),
            long: "option".to_string(),
            desc: "foo foo".to_string(),
            multiple: false,
        };

        let option2: Opt = Opt {
            short: "d".to_string(),
            long: "debug".to_string(),
            desc: "debug mode".to_string(),
            multiple: false,
        };

        assert_eq!(option1, *n[0].as_ref().unwrap());
        assert_eq!(option2, *n[1].as_ref().unwrap());
    }

    #[test]
    fn test_parse_flags() {
        let (_, n): (CompleteByteSlice, Result<Vec<Argument>>) = parse_flags(CompleteByteSlice(
            b"FLAGS:[(short:o,long:option,desc:(foo foo)),(short:d,long:debug,desc:(debug mode))]",
        ))
        .unwrap();

        let option1: Opt = Opt {
            short: "o".to_string(),
            long: "option".to_string(),
            desc: "foo foo".to_string(),
            multiple: false,
        };

        let option2: Opt = Opt {
            short: "d".to_string(),
            long: "debug".to_string(),
            desc: "debug mode".to_string(),
            multiple: false,
        };

        let flags = n.unwrap();
        assert_eq!(Argument::LoneOption(option1), flags[0]);
        assert_eq!(Argument::LoneOption(option2), flags[1]);
    }

    #[test]
    fn test_parse_opt_param() {
        let (_, n): (CompleteByteSlice, Result<(Opt,Param)>) = parse_opt_with_param(CompleteByteSlice(
            b"type:input_file,short:d,long:directory,desc:(directory to unzip),size:1,default_value:\".\"",
        ))
        .unwrap();

        let opt = Opt {
            short: "d".to_string(),
            long: "directory".to_string(),
            desc: "directory to unzip".to_string(),
            multiple: false,
        };

        let param = Param {
            param_type: ArgType::InputFile,
            size: ParamSize::One,
            default_value: ".".to_string(),
            multiple: false,
        };

        let parsed_param = match n {
            Ok(v) => v,
            Err(e) => {
                panic!("Error parsing param: {:?}", e);
            }
        };
        assert_eq!(param, parsed_param.1);
        assert_eq!(opt, parsed_param.0);
    }

    #[test]
    fn test_parse_opt_param_set() {
        let (_, n): (CompleteByteSlice, Vec<Result<(Opt,Param)>>) = parse_opt_with_param_set(CompleteByteSlice(
            b"(short:d,long:directory,type:input_file,size:1,default_value:\".\"),(short:p,long:parent,desc:(parent dir),type:str,size:1,default_value:\"..\"),",
        ))
        .unwrap();
        let first_param = Param {
            param_type: ArgType::InputFile,
            size: ParamSize::One,
            default_value: ".".to_string(),
            multiple: false,
        };

        let first_opt = Opt {
            short: "d".to_string(),
            long: "directory".to_string(),
            desc: "".to_string(),
            multiple: false,
        };

        let second_opt = Opt {
            short: "p".to_string(),
            long: "parent".to_string(),
            desc: "parent dir".to_string(),
            multiple: false,
        };

        let second_param = Param {
            param_type: ArgType::Str,
            size: ParamSize::One,
            default_value: "..".to_string(),
            multiple: false,
        };

        assert_eq!(first_param, (*n[0].as_ref().unwrap()).1);
        assert_eq!(second_param, (*n[1].as_ref().unwrap()).1);
        assert_eq!(first_opt, (*n[0].as_ref().unwrap()).0);
        assert_eq!(second_opt, (*n[1].as_ref().unwrap()).0);
    }

    #[test]
    fn test_parse_opt_params() {
        let (_, n): (CompleteByteSlice, Result<Vec<Argument>>) = parse_opt_with_params(CompleteByteSlice(
            b"OPTPARAMS:[(short:d,long:directory,type:input_file,size:1,default_value:\".\"),(short:p,long:parent,desc:(parent dir),type:str,size:1,default_value:\"..\"),]",
        )).unwrap();

        let second_param: Param = Param {
            param_type: ArgType::Str,
            size: ParamSize::One,
            default_value: "..".to_string(),
            multiple: false,
        };
        let first_param: Param = Param {
            param_type: ArgType::InputFile,
            size: ParamSize::One,
            default_value: ".".to_string(),
            multiple: false,
        };
        let first_opt = Opt {
            short: "d".to_string(),
            long: "directory".to_string(),
            desc: "".to_string(),
            multiple: false,
        };

        let second_opt = Opt {
            short: "p".to_string(),
            long: "parent".to_string(),
            desc: "parent dir".to_string(),
            multiple: false,
        };
        let arg_list = match n {
            Ok(v) => v,
            Err(e) => {
                panic!("Error parsing: {:?}", e);
            }
        };
        assert_eq!(arg_list.len(), 2);
        assert_eq!(arg_list[0], Argument::OptWithParam(first_opt, first_param));
        assert_eq!(
            arg_list[1],
            Argument::OptWithParam(second_opt, second_param)
        );
    }

    #[test]
    fn test_parse_command() {
        let (_, command): (CompleteByteSlice, Result<Command>) = parse_annotation(CompleteByteSlice(
            b"tar: FLAGS:[(short:o,long:option,desc:(foo foo)),(short:d,long:debug,desc:(debug mode))] OPTPARAMS:[(short:d,long:directory,type:input_file,size:1,default_value:\".\"),(short:p,long:parent,desc:(parent dir),type:str,size:1,default_value:\"..\"),]",
        )).unwrap();
        let cmd = command.unwrap();
        let arg_list = cmd.args;
        assert_eq!(cmd.command_name, "tar".to_string());
        let option1: Opt = Opt {
            short: "o".to_string(),
            long: "option".to_string(),
            desc: "foo foo".to_string(),
            multiple: false,
        };

        let option2: Opt = Opt {
            short: "d".to_string(),
            long: "debug".to_string(),
            desc: "debug mode".to_string(),
            multiple: false,
        };

        let second_param: Param = Param {
            param_type: ArgType::Str,
            size: ParamSize::One,
            default_value: "..".to_string(),
            multiple: false,
        };
        let first_param: Param = Param {
            param_type: ArgType::InputFile,
            size: ParamSize::One,
            default_value: ".".to_string(),
            multiple: false,
        };
        let first_opt = Opt {
            short: "d".to_string(),
            long: "directory".to_string(),
            desc: "".to_string(),
            multiple: false,
        };

        let second_opt = Opt {
            short: "p".to_string(),
            long: "parent".to_string(),
            desc: "parent dir".to_string(),
            multiple: false,
        };

        assert_eq!(arg_list.len(), 4);
        assert_eq!(arg_list[2], Argument::OptWithParam(first_opt, first_param));
        assert_eq!(
            arg_list[3],
            Argument::OptWithParam(second_opt, second_param)
        );

        assert_eq!(Argument::LoneOption(option1), arg_list[0]);
        assert_eq!(Argument::LoneOption(option2), arg_list[1]);
    }
}
