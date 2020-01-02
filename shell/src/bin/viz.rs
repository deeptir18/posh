extern crate dash;
extern crate shell;
extern crate structopt_derive;

use dash::util::Result;
use failure::bail;
use shell::annotations::examples;
use shell::annotations::shell_parse;
use std::path::Path;
use std::process::Command;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "visualization_binary",
    help = "Binary to help visualize commands"
)]

struct Opt {
    #[structopt(
        short = "of",
        long = "output_folder",
        help = "Place to write dot files and binaries to."
    )]
    output_folder: String,
    #[structopt(short = "dot", long = "dot_binary", help = "Location of dot binary")]
    dot_binary: String,
}
enum VizType {
    Shell,
    Dash,
}
fn main() {
    let opt = Opt::from_args();
    let output_folder = opt.output_folder;
    let dot_binary = opt.dot_binary;
    /*run_viz(
        &dot_binary,
        "cat /d/c/b/1.INFO | grep '[RAY]' | head -n1 | cut -c 7- > /d/c/b/rays.csv",
        "rt_cmd1",
        &output_folder,
        VizType::Shell,
    );
    run_viz(
        &dot_binary,
        "cat /d/c/b/1.INFO | grep '[RAY]' | head -n1 | cut -c 7- > /d/c/b/rays.csv",
        "rt_cmd1",
        &output_folder,
        VizType::Dash,
    );
    run_viz(
        &dot_binary,
        "cat /d/c/b/2.INFO /d/c/b/3.INFO /d/c/b/4.INFO | grep -v pathID | cut -c 7- >> rays.csv",
        "rt_cmd2",
        &output_folder,
        VizType::Dash,
    );
    run_viz(
        &dot_binary,
        "cat /d/c/b/FILENAME |  zannotate -routing -routing-mrt-file=/d/c/b/mrt_file -input-file-type=json > /d/c/b/annotated",
        "portscan_preprocess",
        &output_folder,
        VizType::Dash,
        );
    run_viz(
        &dot_binary,
    "pr -mts, <( cat /d/c/b/annotated | jq \".ip\" | tr -d '\"' ) <( cat /d/c/b/annotated | jq -c \".zannotate.routing.asn\" ) | awk -F',' '{ a[$2]++; } END { for (n in a) print n \",\" a[n] } ' | sort -k2 -n -t',' -r > b/as_popularity",
        "port_scan_cmd",
        &output_folder,
        VizType::Dash,
    );*/
    
    run_viz(
        &dot_binary,
        "cat /d/c/foo /b/a/foo /e/d/foo /f/e/foo | grep 'bar' > local.txt",
        "distributed_cat",
        &output_folder,
        VizType::Shell,
    );
    run_viz(
        &dot_binary,
        "cat /d/c/foo /b/a/foo /e/d/foo /f/e/foo | grep 'bar' > local.txt",
        "distributed_cat",
        &output_folder,
        VizType::Dash,
    );
}

fn run_viz(dot_binary: &str, cmd: &str, name: &str, output_folder: &str, viztype: VizType) {
    match viztype {
        VizType::Shell => match visualize_shell_graph(dot_binary, cmd, name, output_folder) {
            Ok(_) => {}
            Err(e) => {
                println!("Failed to visualize shell graph: {:?}", e);
            }
        },
        VizType::Dash => match visualize_dash_graph(dot_binary, cmd, name, output_folder) {
            Ok(_) => {}
            Err(e) => {
                println!("Failed to visualize dash graph: {:?}", e);
            }
        },
    }
}

/// Generates shell graph for the command,
/// Writes dot file to the given folder,
/// And generates graph.
/// Assumes graphviz is installed.
fn visualize_shell_graph(dot_binary: &str, command: &str, name: &str, folder: &str) -> Result<()> {
    let file = Path::new(folder);
    let dot_path = file.join(format!("{}_shell_viz.dot", name));
    let graph_path = file.join(format!("{}_shell_viz.pdf", name));
    let dot_path_str = match dot_path.to_str() {
        Some(s) => s,
        None => bail!("Could not turn path: {:?}, shell_viz.dot", file),
    };
    let graph_path_str = match graph_path.to_str() {
        Some(s) => s,
        None => bail!("Could not turn path: {:?}, shell_viz.pdf", file),
    };

    // generate shell graph
    let shellsplit = shell_parse::ShellSplit::new(command)?;
    let shellgraph = shellsplit.convert_into_shell_graph()?;
    shellgraph.write_dot(dot_path_str)?;
    // invoke graphviz
    invoke_graph_viz(dot_binary, dot_path_str, graph_path_str)?;
    Ok(())
}

fn visualize_dash_graph(dot_binary: &str, command: &str, name: &str, folder: &str) -> Result<()> {
    let mut interpreter = examples::get_test_interpreter();
    let file = Path::new(folder);
    let dot_path = file.join(format!("{}_dash_viz.dot", name));
    let graph_path = file.join(format!("{}_dash_viz.pdf", name));
    let dot_path_str = match dot_path.to_str() {
        Some(s) => s,
        None => bail!("Could not turn path: {:?}, dash_viz.dot", file),
    };
    let graph_path_str = match graph_path.to_str() {
        Some(s) => s,
        None => bail!("Could not turn path: {:?}, dash_viz.pdf", file),
    };

    let shellsplit = shell_parse::ShellSplit::new(command)?;
    let shellgraph = shellsplit.convert_into_shell_graph()?;
    let mut program = shellgraph.convert_into_program()?;
    interpreter.apply_parser(&mut program)?;
    interpreter.resolve_env_vars(&mut program)?;
    interpreter.parallelize_cmd_nodes(&mut program)?;
    interpreter.resolve_filestreams(&mut program)?;
    //interpreter.assign_program_location(&mut program)?;
    // invoke graphviz
    program.write_dot(dot_path_str)?;
    invoke_graph_viz(dot_binary, dot_path_str, graph_path_str)?;
    Ok(())
}

fn invoke_graph_viz(binary_path: &str, dot_path: &str, graph_path: &str) -> Result<()> {
    // dot basic.dot -Tpdf -o basic.pdf
    let _output = Command::new(binary_path)
        .arg(dot_path)
        .arg("-Tpdf")
        .arg("-o")
        .arg(graph_path)
        .output()
        .expect("Failed to run dot command");
    Ok(())
}
