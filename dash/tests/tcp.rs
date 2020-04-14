mod common;
mod tcp_helper;
use crate::common::{generate_program, NodeInfo, TestInfo, WriteType};
use crate::tcp_helper::*;
use dash::graph::program::Elem;
use std::collections::HashMap;

#[test]
fn cmd_cmd_tcp_write() {
    let test_info = TestInfo::new(String::from("simple_tcp"), 1, 1, 1000);
    test_info.setup_tmp_folder();
    test_info.generate_input();
    let nodes = vec!["cat", "grep", "write"];
    let mut edges: HashMap<(usize, usize), (String, bool)> = HashMap::default();
    edges.insert((1, 2), (String::from("pipe"), false));
    edges.insert((2, 3), (String::from("tcp"), true));
    let mut node_data: HashMap<usize, NodeInfo> = HashMap::default();
    node_data.insert(
        1,
        NodeInfo {
            input_file: Some(test_info.input_file_name(0)),
            location: server(),
            ..Default::default()
        },
    );
    node_data.insert(
        2,
        NodeInfo {
            input_keyword: Some(String::from("d")),
            location: server(),
            ..Default::default()
        },
    );
    node_data.insert(
        3,
        NodeInfo {
            output_file: Some((test_info.output_file_name(), WriteType::File)),
            ..Default::default()
        },
    );
    let mut prog = match generate_program(&nodes, &edges, &node_data) {
        Ok(prog) => prog,
        Err(e) => {
            println!(
                "Failed to generate prog with nodes {:?}, edges {:?}, node data {:?}",
                nodes, edges, node_data
            );
            panic!("Error: {:?}", e);
        }
    };

    let execution_tmp = test_info
        .get_execution_folder()
        .as_path()
        .to_str()
        .unwrap()
        .to_string();

    match execute_test_program(&execution_tmp.as_str(), &mut prog) {
        Ok(_) => {}
        Err(e) => {
            panic!("Issue executing program: {:?}", e);
        }
    }

    assert!(test_info.check_grepped_output(&vec!["d"]));
    test_info.delete_folder();
}

#[test]
// two arguments that come from FIFOs
fn test_double_fifo() {
    let test_info = TestInfo::new(String::from("double_fifo"), 2, 1, 1000);
    test_info.setup_tmp_folder();
    test_info.generate_input();

    let nodes = vec!["read", "write", "read", "write", "cat", "write"];
    let mut edges: HashMap<(usize, usize), (String, bool)> = HashMap::default();
    edges.insert((1, 2), (String::from("tcp"), false));
    edges.insert((3, 4), (String::from("tcp"), false));
    edges.insert((5, 6), (String::from("pipe"), false));
    let mut node_data: HashMap<usize, NodeInfo> = HashMap::default();
    node_data.insert(
        1,
        NodeInfo {
            input_file: Some(test_info.input_file_name(0)),
            location: server(),
            ..Default::default()
        },
    );
    // need to get a temporary location for the FIFO file
    let mut tmp_path = test_info.get_execution_folder();
    tmp_path.push("2_fifo.txt");
    node_data.insert(
        2,
        NodeInfo {
            output_file: Some((tmp_path.clone(), WriteType::Fifo)),
            ..Default::default()
        },
    );
    node_data.insert(
        3,
        NodeInfo {
            input_file: Some(test_info.input_file_name(1)),
            location: server(),
            ..Default::default()
        },
    );
    // need to get a temporary location for the FIFO file
    let mut tmp_path2 = test_info.get_execution_folder();
    tmp_path2.push("4_fifo.txt");
    node_data.insert(
        4,
        NodeInfo {
            output_file: Some((tmp_path2.clone(), WriteType::Fifo)),
            ..Default::default()
        },
    );
    node_data.insert(
        5,
        NodeInfo {
            input_file: Some(tmp_path),
            ..Default::default()
        },
    );
    node_data.insert(
        6,
        NodeInfo {
            output_file: Some((test_info.output_file_name(), WriteType::File)),
            ..Default::default()
        },
    );
    let execution_tmp = test_info
        .get_execution_folder()
        .as_path()
        .to_str()
        .unwrap()
        .to_string();

    let mut prog = match generate_program(&nodes, &edges, &node_data) {
        Ok(prog) => prog,
        Err(e) => {
            println!(
                "Failed to generate prog with nodes {:?}, edges {:?}, node data {:?}",
                nodes, edges, node_data
            );
            panic!("Error: {:?}", e);
        }
    };
    let cat_node = prog.get_mut_node(5).unwrap().get_mut_elem();
    match cat_node {
        Elem::Cmd(ref mut cmdnode) => {
            cmdnode.add_resolved_arg(tmp_path2.as_path().to_str().unwrap().to_string());
        }
        _ => {
            panic!("Node 5 should be cat cmdnode");
        }
    }
    match execute_test_program(&execution_tmp.as_str(), &mut prog) {
        Ok(_) => {}
        Err(e) => {
            panic!("Issue executing program: {:?}", e);
        }
    }
    assert!(test_info.check_original_output());
    test_info.delete_folder();
}

// read node writing to a tcp stream
// write reading from a tcp
#[test]
fn read_tcp_write() {
    let test_info = TestInfo::new(String::from("read_tcp"), 1, 1, 1000);
    test_info.setup_tmp_folder();
    test_info.generate_input();
    let nodes = vec!["read", "write"];
    let mut edges: HashMap<(usize, usize), (String, bool)> = HashMap::default();
    edges.insert((1, 2), (String::from("tcp"), false));
    let mut node_data: HashMap<usize, NodeInfo> = HashMap::default();
    node_data.insert(
        1,
        NodeInfo {
            input_file: Some(test_info.input_file_name(0)),
            location: server(),
            ..Default::default()
        },
    );
    node_data.insert(
        2,
        NodeInfo {
            output_file: Some((test_info.output_file_name(), WriteType::File)),
            ..Default::default()
        },
    );
    let mut prog = match generate_program(&nodes, &edges, &node_data) {
        Ok(prog) => prog,
        Err(e) => {
            println!(
                "Failed to generate prog with nodes {:?}, edges {:?}, node data {:?}",
                nodes, edges, node_data
            );
            panic!("Error: {:?}", e);
        }
    };

    let execution_tmp = test_info
        .get_execution_folder()
        .as_path()
        .to_str()
        .unwrap()
        .to_string();

    match execute_test_program(&execution_tmp.as_str(), &mut prog) {
        Ok(_) => {}
        Err(e) => {
            panic!("Issue executing program: {:?}", e);
        }
    }

    assert!(test_info.check_original_output());
    test_info.delete_folder();
}

#[test]
fn cmd_tcp_cmd_write() {
    let test_info = TestInfo::new(String::from("cmd_tcp"), 1, 1, 1000);
    test_info.setup_tmp_folder();
    test_info.generate_input();
    let nodes = vec!["cat", "grep", "write"];
    let mut edges: HashMap<(usize, usize), (String, bool)> = HashMap::default();
    edges.insert((1, 2), (String::from("tcp"), true));
    edges.insert((2, 3), (String::from("pipe"), false));
    let mut node_data: HashMap<usize, NodeInfo> = HashMap::default();
    node_data.insert(
        1,
        NodeInfo {
            input_file: Some(test_info.input_file_name(0)),
            location: server(),
            ..Default::default()
        },
    );
    node_data.insert(
        2,
        NodeInfo {
            input_keyword: Some(String::from("f")),
            ..Default::default()
        },
    );
    node_data.insert(
        3,
        NodeInfo {
            output_file: Some((test_info.output_file_name(), WriteType::File)),
            ..Default::default()
        },
    );
    let mut prog = match generate_program(&nodes, &edges, &node_data) {
        Ok(prog) => prog,
        Err(e) => {
            println!(
                "Failed to generate prog with nodes {:?}, edges {:?}, node data {:?}",
                nodes, edges, node_data
            );
            panic!("Error: {:?}", e);
        }
    };

    let execution_tmp = test_info
        .get_execution_folder()
        .as_path()
        .to_str()
        .unwrap()
        .to_string();

    match execute_test_program(&execution_tmp.as_str(), &mut prog) {
        Ok(_) => {}
        Err(e) => {
            panic!("Issue executing program: {:?}", e);
        }
    }

    assert!(test_info.check_grepped_output(&vec!["f"]));
    test_info.delete_folder();
}

#[test]
fn double_tcp_buffering() {
    // two input files
    let test_info = TestInfo::new(String::from("double_buffering_tcp"), 1, 2, 1000);
    test_info.setup_tmp_folder();
    test_info.generate_input();

    let nodes = vec!["cat", "cat", "grep", "write"];
    let mut edges: HashMap<(usize, usize), (String, bool)> = HashMap::default();
    edges.insert((1, 3), (String::from("tcp"), true));
    edges.insert((2, 3), (String::from("tcp"), true)); // second pipe must buffer its output
    edges.insert((3, 4), (String::from("pipe"), false));
    let mut node_data: HashMap<usize, NodeInfo> = HashMap::default();
    node_data.insert(
        1,
        NodeInfo {
            input_file: Some(test_info.input_file_name(0)),
            location: server(),
            ..Default::default()
        },
    );
    node_data.insert(
        2,
        NodeInfo {
            input_file: Some(test_info.input_file_name(1)),
            location: server(),
            ..Default::default()
        },
    );
    node_data.insert(
        3,
        NodeInfo {
            input_keyword: Some(String::from("d")),
            ..Default::default()
        },
    );
    node_data.insert(
        4,
        NodeInfo {
            output_file: Some((test_info.output_file_name(), WriteType::File)),
            ..Default::default()
        },
    );
    let mut prog = match generate_program(&nodes, &edges, &node_data) {
        Ok(prog) => prog,
        Err(e) => {
            println!(
                "Failed to generate prog with nodes {:?}, edges {:?}, node data {:?}",
                nodes, edges, node_data
            );
            panic!("Error: {:?}", e);
        }
    };

    let execution_tmp = test_info
        .get_execution_folder()
        .as_path()
        .to_str()
        .unwrap()
        .to_string();

    match execute_test_program(&execution_tmp.as_str(), &mut prog) {
        Ok(_) => {}
        Err(e) => {
            panic!("Issue executing program: {:?}", e);
        }
    }

    assert!(test_info.check_grepped_output(&vec!["d"]));
    // if ok, can delete the folder
    test_info.delete_folder();
}

// read to Tcp stream where writer writes to  FIFO
// used as an argument to a command
#[test]
fn tcp_fifo() {
    // one input file
    let test_info = TestInfo::new(String::from("tcp_fifo"), 1, 1, 1000);
    test_info.setup_tmp_folder();
    test_info.generate_input();

    let nodes = vec!["read", "write", "cat", "write"];
    let mut edges: HashMap<(usize, usize), (String, bool)> = HashMap::default();
    edges.insert((1, 2), (String::from("tcp"), false));
    edges.insert((3, 4), (String::from("pipe"), false));
    let mut node_data: HashMap<usize, NodeInfo> = HashMap::default();
    node_data.insert(
        1,
        NodeInfo {
            input_file: Some(test_info.input_file_name(0)),
            location: server(),
            ..Default::default()
        },
    );
    // need to get a temporary location for the FIFO file
    let mut tmp_path = test_info.get_execution_folder();
    tmp_path.push("2_fifo.txt");
    node_data.insert(
        2,
        NodeInfo {
            output_file: Some((tmp_path.clone(), WriteType::Fifo)),
            ..Default::default()
        },
    );
    node_data.insert(
        3,
        NodeInfo {
            input_file: Some(tmp_path),
            ..Default::default()
        },
    );
    node_data.insert(
        4,
        NodeInfo {
            output_file: Some((test_info.output_file_name(), WriteType::File)),
            ..Default::default()
        },
    );
    let execution_tmp = test_info
        .get_execution_folder()
        .as_path()
        .to_str()
        .unwrap()
        .to_string();

    let mut prog = match generate_program(&nodes, &edges, &node_data) {
        Ok(prog) => prog,
        Err(e) => {
            println!(
                "Failed to generate prog with nodes {:?}, edges {:?}, node data {:?}",
                nodes, edges, node_data
            );
            panic!("Error: {:?}", e);
        }
    };
    match execute_test_program(&execution_tmp.as_str(), &mut prog) {
        Ok(_) => {}
        Err(e) => {
            panic!("Issue executing program: {:?}", e);
        }
    }
    assert!(test_info.check_original_output());
    test_info.delete_folder();
}

// read to Tcp stream where writer writes to  FIFO
// used as an argument to a command
// node that takes in file as argument runs grep
#[test]
fn grep_fifo() {
    // one input file
    let test_info = TestInfo::new(String::from("grep_fifo"), 1, 1, 1000);
    test_info.setup_tmp_folder();
    test_info.generate_input();

    let nodes = vec!["read", "write", "grep", "write"];
    let mut edges: HashMap<(usize, usize), (String, bool)> = HashMap::default();
    edges.insert((1, 2), (String::from("tcp"), false));
    edges.insert((3, 4), (String::from("pipe"), false));
    let mut node_data: HashMap<usize, NodeInfo> = HashMap::default();
    node_data.insert(
        1,
        NodeInfo {
            input_file: Some(test_info.input_file_name(0)),
            location: server(),
            ..Default::default()
        },
    );
    // need to get a temporary location for the FIFO file
    let mut tmp_path = test_info.get_execution_folder();
    tmp_path.push("2_fifo.txt");
    node_data.insert(
        2,
        NodeInfo {
            output_file: Some((tmp_path.clone(), WriteType::Fifo)),
            ..Default::default()
        },
    );
    node_data.insert(
        3,
        NodeInfo {
            input_keyword: Some(String::from("d")),
            ..Default::default()
        },
    );
    node_data.insert(
        4,
        NodeInfo {
            output_file: Some((test_info.output_file_name(), WriteType::File)),
            ..Default::default()
        },
    );
    let execution_tmp = test_info
        .get_execution_folder()
        .as_path()
        .to_str()
        .unwrap()
        .to_string();

    let mut prog = match generate_program(&nodes, &edges, &node_data) {
        Ok(prog) => prog,
        Err(e) => {
            println!(
                "Failed to generate prog with nodes {:?}, edges {:?}, node data {:?}",
                nodes, edges, node_data
            );
            panic!("Error: {:?}", e);
        }
    };
    // modify grep node to also have file as an argument
    let grep_node = prog.get_mut_node(3).unwrap().get_mut_elem();
    match grep_node {
        Elem::Cmd(ref mut cmdnode) => {
            cmdnode.add_resolved_arg(tmp_path.as_path().to_str().unwrap().to_string());
        }
        _ => {
            panic!("Node 3 in prog should be grep");
        }
    }
    match execute_test_program(&execution_tmp.as_str(), &mut prog) {
        Ok(_) => {}
        Err(e) => {
            panic!("Issue executing program: {:?}", e);
        }
    }
    assert!(test_info.check_grepped_output(&vec!["d"]));
    test_info.delete_folder();
}
// write node accepting multiple connections over TCP
#[test]
fn triple_tcp_bufferable() {
    // two input files
    let test_info = TestInfo::new(String::from("triple_buffering_pipe"), 1, 3, 1000);
    test_info.setup_tmp_folder();
    test_info.generate_input();

    let nodes = vec!["cat", "cat", "cat", "write"];
    let mut edges: HashMap<(usize, usize), (String, bool)> = HashMap::default();
    edges.insert((1, 4), (String::from("tcp"), true));
    edges.insert((2, 4), (String::from("tcp"), true));
    edges.insert((3, 4), (String::from("tcp"), true));
    let mut node_data: HashMap<usize, NodeInfo> = HashMap::default();
    node_data.insert(
        1,
        NodeInfo {
            input_file: Some(test_info.input_file_name(0)),
            location: server(),
            ..Default::default()
        },
    );
    node_data.insert(
        2,
        NodeInfo {
            input_file: Some(test_info.input_file_name(1)),
            location: server(),
            ..Default::default()
        },
    );
    node_data.insert(
        3,
        NodeInfo {
            input_file: Some(test_info.input_file_name(2)),
            location: server(),
            ..Default::default()
        },
    );
    node_data.insert(
        4,
        NodeInfo {
            output_file: Some((test_info.output_file_name(), WriteType::File)),
            ..Default::default()
        },
    );
    let mut prog = match generate_program(&nodes, &edges, &node_data) {
        Ok(prog) => prog,
        Err(e) => {
            println!(
                "Failed to generate prog with nodes {:?}, edges {:?}, node data {:?}",
                nodes, edges, node_data
            );
            panic!("Error: {:?}", e);
        }
    };

    let execution_tmp = test_info
        .get_execution_folder()
        .as_path()
        .to_str()
        .unwrap()
        .to_string();

    match execute_test_program(&execution_tmp.as_str(), &mut prog) {
        Ok(_) => {}
        Err(e) => {
            panic!("Issue executing program: {:?}", e);
        }
    }
    assert!(test_info.check_original_output());
    test_info.delete_folder();
}
