use super::{annotations2, config, interpreter, scheduler};
use annotations2::cmd_parser::CmdParser;
use annotations2::grammar::Command;
use annotations2::parser::Parser;
use config::network::{FileNetwork, ServerInfo, ServerKey};
use dash::graph::Location;
use interpreter::Interpreter;
use scheduler::DPScheduler;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
fn get_git_clone_parser() -> CmdParser {
    let mut parser = CmdParser::new("git clone");
    let annotation = "git clone: PARAMS:[(type:str,size:1),(type:output_file,size:1)]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

fn get_git_commit_parser() -> CmdParser {
    let mut parser = CmdParser::new("git commit");
    let annotation = "git commit[needs_current_dir]: OPTPARAMS:[(type:str,short:m,size:1)]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

// "tar: FLAGS:[(short:o,long:option,desc:(foo foo)),(short:d,long:debug,desc:(debug mode))] OPTPARAMS:[(short:d,long:directory,type:input_file,size:1,default_value:\".\"),(short:p,long:parent,desc:(parent dir),type:str,size:1,default_value:\"..\"),]"
fn get_cat_parser() -> CmdParser {
    let mut parser = CmdParser::new("cat");
    let annotation = "cat: PARAMS:[(type:input_file,splittable,size:list(list_separator:( ))),]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

fn get_jq_parser() -> CmdParser {
    let mut parser = CmdParser::new("jq");
    let annotation = "jq: FLAGS:[(short:c)] PARAMS:[(type:str,size:1),(type:input_file,size:1)]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

fn get_grep_parser() -> CmdParser {
    let mut parser = CmdParser::new("grep");
    // note: the grep parser would parse *both* types of invocations -- unless the first one is
    // supplied first
    // TODO: is this a big deal?
    let annotation =
        "grep[splittable_across_input,reduces_input]: FLAGS:[(short:v,long:invert-match)] PARAMS:[(type:str,size:1),(type:input_file,size:list(list_separator:( )))]";
    let annotation1 = "grep[splittable_across_input,reduces_input]: FLAGS:[(short:v,long:invert-match)] OPTPARAMS:[(short:e,long:regexp,type:str,size:1),(short:f,long:file,type:input_file,size:1)] PARAMS:[(type:input_file,size:list(list_separator:( )))]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
        .add_annotation(Command::new(annotation1).unwrap())
        .unwrap();
    parser
}

fn get_wc_parser() -> CmdParser {
    let mut parser = CmdParser::new("wc");
    let annotation =
        "wc: FLAGS:[(short:l,long:lines)] PARAMS:[(type:input_file,size:list(list_separator:( )))]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

fn get_mogrify_parser() -> CmdParser {
    let mut parser = CmdParser::new("mogrify");
    let annotation = "mogrify[long_arg_single_dash]: OPTPARAMS:[(long:format,type:str,size:1),(long:path,type:input_file,size:1),(long:thumbnail,type:str,size:1)] PARAMS:[(type:output_file,size:list(list_separator:( )))]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

fn get_awk_parser() -> CmdParser {
    let mut parser = CmdParser::new("awk");
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

fn get_tr_parser() -> CmdParser {
    let mut parser = CmdParser::new("tr");
    let annotation = "tr: FLAGS:[(short:d)] PARAMS:[(type:str,size:1)]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

fn get_cut_parser() -> CmdParser {
    let mut parser = CmdParser::new("cut");
    let annotation = "cut: OPTPARAMS:[(long:characters,short:c,size:1,type:str)]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

fn get_pr_parser() -> CmdParser {
    let mut parser = CmdParser::new("pr");
    let annotation =
        "pr: FLAGS:[(short:m,long:merge),(short:t)] OPTPARAMS:[(short:s,type:str,size:1)]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

fn get_head_parser() -> CmdParser {
    let mut parser = CmdParser::new("head");
    let annotation = "head: OPTPARAMS:[(long:lines,short:n,size:1)]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

fn get_q_parser() -> CmdParser {
    let mut parser = CmdParser::new("q");
    let annotation =
        "q: FLAGS:[(short:H)] OPTPARAMS:[(short:d,size:1,type:str)] PARAMS:[(type:str,size:1)]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

fn get_sort_parser() -> CmdParser {
    let mut parser = CmdParser::new("sort");
    let annotation = "sort: OPTPARAMS:[(short:k,size:1,long:key,type:str),(short:t,size:1,type:str)] FLAGS:[(short:n),(short:r)]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

fn get_zannotate_parser() -> CmdParser {
    let mut parser = CmdParser::new("zannotate");
    let annotation = "zannotate[long_arg_single_dash]: FLAGS:[(long:routing)] OPTPARAMS:[(long:routing-mrt-file,type:input_file,size:1),(long:input-file-type,type:str,size:1)]";
    parser
        .add_annotation(Command::new(annotation).unwrap())
        .unwrap();
    parser
}

fn get_test_parser() -> Parser {
    let mut parsers: HashMap<String, CmdParser> = HashMap::default();
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
    parsers.insert("git clone".to_string(), get_git_clone_parser());
    parsers.insert("git commit".to_string(), get_git_commit_parser());
    Parser::construct(parsers)
}
fn get_test_filemap() -> HashMap<PathBuf, ServerKey> {
    let mut map: HashMap<PathBuf, ServerKey> = HashMap::default();
    map.insert(
        Path::new("/b/a").to_path_buf(),
        ServerKey {
            ip: "125.0.0.1".to_string(),
        },
    );
    map.insert(
        Path::new("/c/b").to_path_buf(),
        ServerKey {
            ip: "126.0.0.1".to_string(),
        },
    );
    map.insert(
        Path::new("/d/c").to_path_buf(),
        ServerKey {
            ip: "127.0.0.1".to_string(),
        },
    );
    map.insert(
        Path::new("/e/d").to_path_buf(),
        ServerKey {
            ip: "128.0.0.1".to_string(),
        },
    );
    map.insert(
        Path::new("/f/e").to_path_buf(),
        ServerKey {
            ip: "129.0.0.1".to_string(),
        },
    );
    map
}

fn get_test_links() -> HashMap<(Location, Location), u32> {
    let mut ret: HashMap<(Location, Location), u32> = HashMap::default();
    let ips = vec![
        "client",
        "125.0.0.1",
        "126.0.0.1",
        "127.0.0.1",
        "128.0.0.1",
        "129.0.0.1",
    ];
    for i in 0..ips.len() {
        for j in 0..ips.len() {
            if i == j {
                continue;
            }
            let first_location = ips[i].to_string();
            let second_location = ips[i].to_string();
            // 20 Mbps for wan links
            if first_location == "client" {
                ret.insert((Location::Client, Location::Server(second_location)), 20);
            } else if second_location == "client" {
                ret.insert((Location::Server(first_location), Location::Client), 20);
            } else {
                ret.insert(
                    (
                        Location::Server(first_location),
                        Location::Server(second_location),
                    ),
                    10000, // 10 Gbps as Mbps
                );
            }
        }
    }
    ret
}

fn get_test_server_info() -> HashMap<ServerKey, ServerInfo> {
    let mut ret: HashMap<ServerKey, ServerInfo> = HashMap::new();
    let ips = vec![
        "125.0.0.1",
        "126.0.0.1",
        "127.0.0.1",
        "128.0.0.1",
        "129.0.0.1",
    ];
    for ip in ips.iter() {
        let info = ServerInfo {
            other_mounted_directories: Vec::new(),
            tmp_directory: Path::new("/dash/tmp").to_path_buf(),
        };
        ret.insert(ServerKey { ip: ip.to_string() }, info);
    }
    ret
}
fn get_test_network_config() -> FileNetwork {
    FileNetwork::construct(get_test_filemap(), get_test_links(), get_test_server_info())
}

pub fn get_test_interpreter() -> Interpreter {
    // TODO: actually choose with scheduler to use
    let scheduler = Box::new(DPScheduler {});
    Interpreter::construct(
        get_test_network_config(),
        get_test_parser(),
        scheduler,
        Path::new("/d/c/folder").to_path_buf(),
    )
}
