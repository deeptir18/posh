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
    options: Vec<OptArg>,
}
#[derive(Debug)]
struct OptArg {
    delimeter: String, // how this option is parsed
    name: String,
    num_args: u8,
    is_file: bool,
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
    parse_option_set<&[u8], Vec<OptArg>>,
    map_res!(
        do_parse!(
            delimeter: map_res!(take_until!("["), str::from_utf8)
                >> option_set: delimited!(tag!("["), options, tag!("]"))
                >> (delimeter, option_set)
        ),
        |(delim, option_set): (&str, Temp)| {
            let vec: Vec<OptArg> = Vec::new();
            vec.push(OptArg {
                delimeter: String::from(delim),
                name: option_set.inside,
                num_args: 1,
                is_file: false,
            });
            Ok(vec)
        }
    )
);

//named!(parse_options<Vec<OptArg>>, ws!(many1!(parse_option_set)));
named!(
    next_alphabetic<&[u8], &str>,
    map_res!(take_while1!(is_alphabetic), str::from_utf8)
);

named!(
    get_annotation<&[u8], Annotation>,
        do_parse!(
            name: map_res!(take_until!(":"), str::from_utf8) >>
            tag!(": ") >>
            options: parse_option_set >>
            (Annotation{ name: String::from(name), options: options}) // put in the unwrap for now
    )
);
/* Parses the annotation for a certain command that generates a custom parser
 *
 * */
fn parse_annotation(ann: &str) -> Result<Annotation> {
    // example annotation:
    // by default: delimeter is a space and is_file is false
    // [commandname]: -[a|b:1," "|argname:num,"delim",is_file|...] --[a|b:1," "|argname:num,"delim"]
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
        println!("{:?}", parse_annotation("cat: -[ahhh]").unwrap());
        assert!(false);
    }
}
