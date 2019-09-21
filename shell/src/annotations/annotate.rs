extern crate dash;
extern crate nom;

use dash::util::Result;
use failure::bail;
use nom::character::complete::*;
use nom::character::*;
use nom::*;
use std::*;

#[derive(Debug)]
struct Annotation {
    name: String, // command name
    options: Vec<Argument>,
}

#[derive(Debug)]
enum Argument {
    Opt(OptArg),
    File(FileArg),
}

#[derive(Debug)]
struct OptArg {
    delimeter: String, // how this option is parsed
    info: SingleOption,
}

#[derive(Debug)]
struct SingleOption {
    name: String,
    num_args: u8,  // default: 0 (just a flag)
    is_file: bool, // default: false
    delim: String, // default: " "
}

#[derive(Debug)]
enum OptionInfo {
    Name(String),
    NumArgs(u8),
    IsFile,
    Delim(String),
}

#[derive(Debug)]
struct FileArg {
    name: String,
    file_type: FileType,
}

#[derive(Debug)]
enum FileType {
    Single,   // Single File
    Multiple, // List of Files
    Pattern,  // File pattern (e.g. *.txt)
}

struct Temp {
    inside: String,
}

named!(
    parse_option_name<OptionInfo>,
    map!(
        do_parse!(
            //tag: tag!("name:") >>
            name: next_alphabetic >> (name)
        ),
        |name: &str| {
            println!("name: {:?}", name);
            OptionInfo::Name(String::from(name))
        }
    )
);
named!(
        next_alphabetic<&[u8], &str>,
        map_res!(take_while1!(is_alphabetic), str::from_utf8)
    );
named!(wrapper<&[u8], &str>,
    do_parse!(name: next_alphabetic >> (name))
);
named!(
    parse_option_num_args<OptionInfo>,
    map!(
        do_parse!(
            tag!("num_args:") >> dig: map_res!(take_while!(is_digit), str::from_utf8) >> (dig)
        ),
        |s: &str| OptionInfo::NumArgs(s.parse::<u8>().unwrap())
    )
);

named!(
    parse_option_file<OptionInfo>,
    do_parse!(tag!("is_file") >> (OptionInfo::IsFile))
);

named!(
    parse_option_delim<OptionInfo>,
    do_parse!(
        tag!("delim:")
            >> delim:
                map_res!(
                    alt!(tag!("-") | tag!("--") | tag!("=") | tag!("==") | tag!(" ")),
                    str::from_utf8
                )
            >> (OptionInfo::Delim(String::from(delim)))
    )
);

named!(
    parse_option_info<OptionInfo>,
    alt!(parse_option_name | parse_option_num_args | parse_option_delim | parse_option_file)
);

named!(
    parse_single_option<SingleOption>,
    map!(
        many1!(do_parse!(
            opt: parse_option_info >> opt!(tag!(",")) >> (opt)
        )),
        |(vec_options): (Vec<OptionInfo>)| {
            let mut opt = SingleOption {
                name: String::from(""),
                num_args: 0,
                is_file: false,
                delim: String::from(" "),
            };
            println!("length of ret: {:?}", vec_options.len());
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

/* Parses the annotation for a certain command that generates a custom parser
 *
 *
 */
/*fn parse_annotation(ann: &str) -> Result<Annotation> {
    // example annotation:
    // by default: delimeter is a space and is_file is false
    // [commandname]: OPT:-[name:a|name:b,num:1,delim:" "|name:argname,num:num,delim:"delim",is_file|...] OPT:--[a|b:1," "|argname:num,"delim"] FILE:[]
    // [commandname]: delimeter[options] files[SINGLE|MULTIPLE|PATTERN]
    match get_annotation(ann.as_bytes()) {
        Ok(a) => Ok(a.1),
        Err(e) => bail!("{:?}", e),
    }
}*/

#[cfg(test)]
mod tests {
    use super::*;

    /*#[test]
    fn test_basic() {
        println!(
            "{:?}",
            parse_annotation(
                "cat: OPT:-[ahhh] OPT:--[aaaaaahhhahahahahaah] FILE:[SINGLE|MULTIPLE|PATTERN]"
            )
            .unwrap()
        );
        assert!(false);
    }*/
    #[test]
    fn test_opt_name() {
        println!("{:?}", wrapper(b"name"));
        assert!(false);
    }

}
