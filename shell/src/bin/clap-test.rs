extern crate clap;
use clap::{App, Arg, ArgMatches};
use std::panic;
pub fn main() {
    test_normal_invocation();
    test_normal_invocation_grouped();
    test_unknown_arg();
    test_unnamed_arg_single();
    test_unnamed_arg_multiple();
    test_multiple_unnamed_arg();
    test_options_multiple_args();
}

fn test_normal_invocation() {
    let mut app = App::new("test_program")
        .version("1.0")
        .author("doesn't matter");
    app = app.arg(Arg::with_name("debug").short("d").long("debug"));
    app = app.arg(Arg::with_name("outfile").short("o").long("outfile"));
    let normal_invocation = vec![
        "test_program".to_string(),
        "-d".to_string(),
        "--outfile".to_string(),
    ];
    match app.get_matches_from_safe(normal_invocation) {
        Ok(p) => println!("normal: {:?}", p),
        Err(e) => println!("normal: {:?}", e),
    }
}

fn test_normal_invocation_grouped() {
    let mut app = App::new("test_program")
        .version("1.0")
        .author("doesn't matter");
    app = app.arg(Arg::with_name("debug").short("d").long("debug"));
    app = app.arg(Arg::with_name("outfile").short("o").long("outfile"));

    let normal_invocation2 = vec!["test_program".to_string(), "-do".to_string()];
    match app.get_matches_from_safe(normal_invocation2) {
        Ok(p) => println!("normal2: {:?}", p),
        Err(e) => println!("normal2: {:?}", e),
    }
}

fn test_unknown_arg() {
    let mut app = App::new("test_program")
        .version("1.0")
        .author("doesn't matter");
    app = app.arg(Arg::with_name("debug").short("d").long("debug"));
    app = app.arg(Arg::with_name("outfile").short("o").long("outfile"));
    let invocation_unknown_arg = vec!["test_program".to_string(), "--foo".to_string()];
    match app.get_matches_from_safe(invocation_unknown_arg) {
        Ok(p) => println!("unknown arg: {:?}", p),
        Err(e) => println!("unknown arg: {:?}", e),
    }
}

fn test_unnamed_arg_single() {
    let mut app = App::new("test_program").version("1.0").author("foo");
    app = app.arg(Arg::with_name("debug").short("d").long("debug"));
    app = app.arg(Arg::with_name("input_file").takes_value(true));
    let invocation = vec![
        "test_program".to_string(),
        "file1".to_string(),
        "file2".to_string(),
    ];

    match app.get_matches_from_safe(invocation) {
        Ok(p) => println!("unnamed arg: {:?}", p),
        Err(e) => println!("unnamed arg: {:?}", e),
    }
}

fn test_unnamed_arg_multiple() {
    let mut app = App::new("test_program").version("1.0").author("foo");
    app = app.arg(Arg::with_name("debug").short("d").long("debug"));
    app = app.arg(Arg::with_name("foo").short("f").long("foo"));
    app = app.arg(
        Arg::with_name("input_file")
            .takes_value(true)
            .multiple(true),
    );
    let invocation = vec![
        "test_program".to_string(),
        "-d".to_string(),
        "file1".to_string(),
        "file2".to_string(),
        "-f".to_string(),
        "file3".to_string(),
    ];

    match app.get_matches_from_safe(invocation) {
        Ok(p) => println!("unnamed arg: {:?}", p),
        Err(e) => println!("unnamed arg: {:?}", e),
    }
}

fn test_multiple_unnamed_arg() {
    // so if you have MULTIPLE unnamed args - clap fills them in first
    // by order -- so the first 1 will go to the first unnnamed arg (wherever it appea  rs),
    // and the second one will go to the next unnamed argument (wherever it appears)
    // This DOESN'T extend to args with more than 1 value UNLESS they are comma delimited
    let mut app = App::new("test_program").version("1.0").author("foo");
    app = app.arg(Arg::with_name("debug").short("d").long("debug"));
    app = app.arg(Arg::with_name("foo").short("f").long("foo"));
    app = app.arg(
        Arg::with_name("input_file")
            .takes_value(true)
            .use_delimiter(true)
            .value_terminator(","),
    );
    app = app.arg(
        Arg::with_name("output_file")
            .takes_value(true)
            .multiple(true),
    );
    let invocation = vec![
        "test_program".to_string(),
        "-d".to_string(),
        "file1,filex,file3".to_string(),
        "file2".to_string(),
        "--foo".to_string(),
        "file3".to_string(),
    ];
    match app.get_matches_from_safe(invocation) {
        Ok(p) => println!("multiple unnamed arg: {:?}", p),
        Err(e) => println!("multiple unnamed arg: {:?}", e),
    }
}

fn test_options_multiple_args() {
    // So if you have a NAMED option before another option that takes values --
    // it can have a specific number of multiple values before the other stuff
    // You can have multiple values on a NAMED option as long as it ends with another option or --
    // to signify the unnamed options come after
    let mut app = App::new("test_program").version("1.0").author("foo");
    app = app.arg(Arg::with_name("debug").short("d").long("debug"));
    app = app.arg(
        Arg::with_name("fake")
            .short("f")
            .long("fake")
            .multiple(true),
    );
    app = app.arg(
        Arg::with_name("input_file")
            .short("i")
            .long("input_file")
            .takes_value(true)
            .multiple(true),
    );
    app = app.arg(Arg::with_name("output_file").takes_value(true));

    let invocation = vec![
        "test_program".to_string(),
        "-if".to_string(),
        "file2".to_string(),
        "file3".to_string(),
        "-f".to_string(),
        "--".to_string(),
        "file3".to_string(),
    ];
    let matches: Option<ArgMatches> = match app.get_matches_from_safe(invocation) {
        Ok(p) => {
            println!("options multiple args: {:?}", p);
            Some(p)
        }
        Err(e) => {
            println!("options multiple args: {:?}", e);
            None
        }
    };

    for arg in matches.unwrap().args.iter() {
        println!("arg: 1: {:?}, 2: {:?}", arg.0, arg.1);
    }
}
