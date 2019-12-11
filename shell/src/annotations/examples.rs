use super::fileinfo::FileMap;
use super::grammar::*;
use super::interpreter::Interpreter;
use super::parser::Parser;
use std::collections::HashMap;
use std::path::PathBuf;
fn get_test_filemap() -> FileMap {
    let mut map: HashMap<String, String> = HashMap::default();
    map.insert("/d/c/".to_string(), "127.0.0.1".to_string());
    FileMap::construct(map)
}

// "tar: FLAGS:[(short:o,long:option,desc:(foo foo)),(short:d,long:debug,desc:(debug mode))] OPTPARAMS:[(short:d,long:directory,type:input_file,size:1,default_value:\".\"),(short:p,long:parent,desc:(parent dir),type:str,size:1,default_value:\"..\"),]"
fn get_cat_parser() -> Parser {
    let mut parser = Parser::new("cat");
    let annotation = "cat: PARAMS:[(type:input_file,size:list(list_separator:( ))),]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

fn get_jq_parser() -> Parser {
    let mut parser = Parser::new("jq");
    let annotation = "jq: FLAGS:[(short:c)] PARAMS:[(type:str,size:1),(type:input_file,size:1)]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

fn get_grep_parser() -> Parser {
    let mut parser = Parser::new("grep");
    // note: the grep parser would parse *both* types of invocations -- unless the first one is
    // supplied first
    // TODO: is this a big deal?
    let annotation =
        "grep: FLAGS:[(short:v,long:invert-match)] PARAMS:[(type:str,size:1),(type:input_file,size:list(list_separator:( )))]";
    let annotation1 = "grep: FLAGS:[(short:v,long:invert-match)] OPTPARAMS:[(short:e,long:regexp,type:str,size:1),(short:f,long:file,type:input_file,size:1)] PARAMS:[(type:input_file,size:list(list_separator:( )))]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
        .add_annotation(Command::new(annotation1).unwrap())
        .unwrap();
    parser
}

fn get_wc_parser() -> Parser {
    let mut parser = Parser::new("wc");
    let annotation =
        "wc: FLAGS:[(short:l,long:lines)] PARAMS:[(type:input_file,size:list(list_separator:( )))]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

fn get_mogrify_parser() -> Parser {
    let mut parser = Parser::new("mogrify");
    let annotation = "mogrify[long_arg_single_dash]: OPTPARAMS:[(long:format,type:str,size:1),(long:path,type:input_file,size:1),(long:thumbnail,type:str,size:1)] PARAMS:[(type:output_file,size:list(list_separator:( )))]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

fn get_awk_parser() -> Parser {
    let mut parser = Parser::new("awk");
    let annotation = "awk: OPTPARAMS:[(short:W,type:str,size:1),(short:F,type:str,size:1),(short:v,type:str,size:1)] PARAMS:[(type:str,size:1),(type:input_file,size:list(list_separator:( )))]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    let annotation2 = "awk: OPTPARAMS:[(short:W,type:str,size:1),(short:F,type:str,size:1),(short:v,type:str,size:1),(short:f,type:input_file,size:1)] PARAMS:[(type:input_file,size:list(list_separator:( )))]";
    parser
        .add_annotation(Command::new(annotation2).unwrap())
        .unwrap();
    parser
}

fn get_tr_parser() -> Parser {
    let mut parser = Parser::new("tr");
    let annotation = "tr: FLAGS:[(short:d)] PARAMS:[(type:str,size:1)]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

fn get_cut_parser() -> Parser {
    let mut parser = Parser::new("cut");
    let annotation = "cut: OPTPARAMS:[(long:characters,short:c,size:1,type:str)]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

fn get_pr_parser() -> Parser {
    let mut parser = Parser::new("pr");
    let annotation =
        "pr: FLAGS:[(short:m,long:merge),(short:t)] OPTPARAMS:[(short:s,type:str,size:1)]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

fn get_head_parser() -> Parser {
    let mut parser = Parser::new("head");
    let annotation = "head: OPTPARAMS:[(long:lines,short:n,size:1)]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

fn get_q_parser() -> Parser {
    let mut parser = Parser::new("q");
    let annotation =
        "q: FLAGS:[(short:H)] OPTPARAMS:[(short:d,size:1,type:str)] PARAMS:[(type:str,size:1)]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

fn get_sort_parser() -> Parser {
    let mut parser = Parser::new("sort");
    let annotation = "sort: OPTPARAMS:[(short:k,size:1,long:key,type:str),(short:t,size:1,type:str)] FLAGS:[(short:n),(short:r)]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

fn get_zannotate_parser() -> Parser {
    let mut parser = Parser::new("zannotate");
    let annotation = "zannotate[long_arg_single_dash]: FLAGS:[(long:routing)] OPTPARAMS:[(long:routing-mrt-file,type:input_file,size:1),(long:input-file-type,type:str,size:1)]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

fn get_test_parser() -> HashMap<String, Parser> {
    let mut parsers: HashMap<String, Parser> = HashMap::default();
    parsers.insert("cat".to_string(), get_cat_parser());
    parsers.insert("grep".to_string(), get_grep_parser());
    parsers.insert("mogrify".to_string(), get_mogrify_parser());
    parsers.insert("cut".to_string(), get_cut_parser());
    parsers.insert("head".to_string(), get_head_parser());
    parsers.insert("q".to_string(), get_q_parser());
    parsers.insert("jq".to_string(), get_jq_parser());
    parsers.insert("awk".to_string(), get_awk_parser());
    parsers.insert("tr".to_string(), get_tr_parser());
    parsers.insert("pr".to_string(), get_pr_parser());
    parsers.insert("sort".to_string(), get_sort_parser());
    parsers.insert("zannotate".to_string(), get_zannotate_parser());
    parsers.insert("wc".to_string(), get_wc_parser());
    parsers
}

pub fn get_test_interpreter() -> Interpreter {
    Interpreter {
        parsers: get_test_parser(),
        filemap: get_test_filemap(),
        pwd: PathBuf::new(),
    }
}
