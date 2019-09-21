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
    num_args: u8,
    is_file: bool,
}

#[derive(Debug)]
struct FileArg {
    name: String,
    file_type: FileType,
}

#[derive(Debug)]
enum FileType {
    Single, // Single File
    Multiple, // List of Files
    Pattern, // File pattern (e.g. *.txt)
}

struct Temp {
    inside: String,
}

named!(
    options<Temp>,
    do_parse!(
        word: map_res!(alpha1, str::from_utf8)
            >> (Temp {
                inside: String::from(word)
            })
    )
);
named!(
    parse_option_set<&[u8], Vec<SingleOption>>,
    map!(
        do_parse!(
            delimeter: map_res!(take_until!("["), str::from_utf8)
                >> option_set: delimited!(tag!("["), options, tag!("]"))
                >> (delimeter, option_set)
        ),
        |(delim, option_set): (&str, Temp)| {
            let mut vec: Vec<OptArg> = Vec::new();
            vec.push(Argument::Opt {
                delimeter: String::from(delim),
                info: SingleOption{ 
                    name: option_set.inside,
                    num_args: 1,
                    is_file: false,
                }});
            vec
        }
    )
);

named!(
    parse_single_option<SingleOption>,
    
)

named!(
    parse_options<Vec<Argument>>,
    map_res!(do_parse!(
        _: tag!("OPT:") >>
        option_list: delimited!(tag!("["), parse_option_list, tag!("]")) >>
        (option_list)
    ))
)

named!(
    parse_files<Vec<Argument>>,
    do_parse!(
        tag!("FILE:") >>
        
        

    )
)
named!(
    parse_one_arg<Vec<Argument>>,
    alt!(
        parse_options => { |res|  res } |
        parse_files  => { |res| res }
    )
);

named!(
    parse_args<Vec<Argument>>,
    map!(many0!(parse_one_arg),
        |vec_args|: Vec<Argument> {
            let merged: Vec<Argument> = Vec::new();
            for vec in vec_args {
                for arg in vec {
                    merged.push(arg);
                }
            }
            merged
        }
    )
);
named!(
        next_alphabetic<&[u8], &str>,
        map_res!(take_while1!(is_alphabetic), str::from_utf8)
    );

named!(
        get_annotation<&[u8], Annotation>,
            do_parse!(
                name: map_res!(take_until!(":"), str::from_utf8) >>
            tag!(": ") >>
            options: parse_args >>
            (Annotation{ name: String::from(name), options: options}) // put in the unwrap for now
    )
);
/* Parses the annotation for a certain command that generates a custom parser
 *
 * */
fn parse_annotation(ann: &str) -> Result<Annotation> {
    // example annotation:
    // by default: delimeter is a space and is_file is false
    // [commandname]: OPT:-[a|b:1," "|argname:num,"delim",is_file|...] OPT:--[a|b:1," "|argname:num,"delim"] FILE:[]
    // [commandname]: delimeter[options] files[SINGLE|MULTIPLE|PATTERN]
    match get_annotation(ann.as_bytes()) {
        Ok(a) => Ok(a.1),
        Err(e) => bail!("{:?}", e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        println!(
            "{:?}",
            parse_annotation(
                "cat: OPT:-[ahhh] OPT:--[aaaaaahhhahahahahaah] FILE:[SINGLE|MULTIPLE|PATTERN]"
            )
            .unwrap()
        );
        assert!(false);
    }
}
