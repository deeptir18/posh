use std::collections::HashMap;
mod common;
use crate::common::{generate_program, NodeInfo, TestInfo, WriteType};
use dash::graph::stream::SharedStreamMap;

#[test]
fn cmd_cmd_write_pipe() {
    let test_info = TestInfo::new(String::from("simple_pipe"), 1, 1, 1000);
    test_info.setup_tmp_folder();
    test_info.generate_input();
    let nodes = vec!["cat", "grep", "write"];
    let mut edges: HashMap<(usize, usize), (String, bool)> = HashMap::default();
    edges.insert((1, 2), (String::from("pipe"), false));
    edges.insert((2, 3), (String::from("pipe"), false));
    let mut node_data: HashMap<usize, NodeInfo> = HashMap::default();
    node_data.insert(
        1,
        NodeInfo {
            input_file: Some(test_info.input_file_name(0)),
            ..Default::default()
        },
    );
    node_data.insert(
        2,
        NodeInfo {
            input_keyword: Some(String::from("d")),
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
    let mut test_prog = match generate_program(&nodes, &edges, &node_data) {
        Ok(prog) => prog,
        Err(e) => {
            println!(
                "Failed to generate prog with nodes {:?}, edges {:?}, node data {:?}",
                nodes, edges, node_data
            );
            panic!("Error: {:?}", e);
        }
    };

    // let the program execute
    match test_prog.execute(
        SharedStreamMap::new(),
        test_info
            .get_execution_folder()
            .as_path()
            .to_str()
            .unwrap()
            .to_string(),
    ) {
        Ok(_) => {}
        Err(e) => {
            tracing::error!("Program execution failed: {:?}", e);
        }
    }

    let keywords = vec!["d"];
    assert!(test_info.check_grepped_output(&keywords));
    test_info.delete_folder();
}

#[test]
fn cmd_read_write_pipe() {
    let test_info = TestInfo::new(String::from("read_pipe"), 1, 1, 1000);
    test_info.setup_tmp_folder();
    test_info.generate_input();
    let nodes = vec!["grep", "write"];
    let mut edges: HashMap<(usize, usize), (String, bool)> = HashMap::default();
    edges.insert((1, 2), (String::from("pipe"), false));
    let mut node_data: HashMap<usize, NodeInfo> = HashMap::default();
    node_data.insert(
        1,
        NodeInfo {
            input_file: Some(test_info.input_file_name(0)),
            input_keyword: Some(String::from("d")),
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
    let mut test_prog = match generate_program(&nodes, &edges, &node_data) {
        Ok(prog) => prog,
        Err(e) => {
            println!(
                "Failed to generate prog with nodes {:?}, edges {:?}, node data {:?}",
                nodes, edges, node_data
            );
            panic!("Error: {:?}", e);
        }
    };
    // let the program execute
    match test_prog.execute(
        SharedStreamMap::new(),
        test_info
            .get_execution_folder()
            .as_path()
            .to_str()
            .unwrap()
            .to_string(),
    ) {
        Ok(_) => {}
        Err(e) => {
            tracing::error!("Program execution failed: {:?}", e);
        }
    }

    let keywords = vec!["d"];
    assert!(test_info.check_grepped_output(&keywords));
    test_info.delete_folder();
}
#[test]
fn single_pipe_bufferable() {
    let test_info = TestInfo::new(String::from("single_pipe_bufferable"), 1, 1, 1000);
    test_info.setup_tmp_folder();
    test_info.generate_input();
    let nodes = vec!["cat", "grep", "write"];
    let mut edges: HashMap<(usize, usize), (String, bool)> = HashMap::default();
    edges.insert((1, 2), (String::from("pipe"), true));
    edges.insert((2, 3), (String::from("pipe"), false));
    let mut node_data: HashMap<usize, NodeInfo> = HashMap::default();
    node_data.insert(
        1,
        NodeInfo {
            input_file: Some(test_info.input_file_name(0)),
            ..Default::default()
        },
    );
    node_data.insert(
        2,
        NodeInfo {
            input_keyword: Some(String::from("d")),
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
    let mut test_prog = match generate_program(&nodes, &edges, &node_data) {
        Ok(prog) => prog,
        Err(e) => {
            println!(
                "Failed to generate prog with nodes {:?}, edges {:?}, node data {:?}",
                nodes, edges, node_data
            );
            panic!("Error: {:?}", e);
        }
    };

    // let the program execute
    match test_prog.execute(
        SharedStreamMap::new(),
        test_info
            .get_execution_folder()
            .as_path()
            .to_str()
            .unwrap()
            .to_string(),
    ) {
        Ok(_) => {}
        Err(e) => {
            tracing::error!("Program execution failed: {:?}", e);
        }
    }

    let keywords = vec!["d"];
    assert!(test_info.check_grepped_output(&keywords));
    test_info.delete_folder();
}

#[test]
fn double_pipe_bufferable() {
    // two input files
    let test_info = TestInfo::new(String::from("double_buffering_pipe"), 1, 2, 1000);
    test_info.setup_tmp_folder();
    test_info.generate_input();

    let nodes = vec!["cat", "cat", "grep", "write"];
    let mut edges: HashMap<(usize, usize), (String, bool)> = HashMap::default();
    edges.insert((1, 3), (String::from("pipe"), false));
    edges.insert((2, 3), (String::from("pipe"), true)); // second pipe must buffer its output
    edges.insert((3, 4), (String::from("pipe"), false));
    let mut node_data: HashMap<usize, NodeInfo> = HashMap::default();
    node_data.insert(
        1,
        NodeInfo {
            input_file: Some(test_info.input_file_name(0)),
            ..Default::default()
        },
    );
    node_data.insert(
        2,
        NodeInfo {
            input_file: Some(test_info.input_file_name(1)),
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
    let mut test_prog = match generate_program(&nodes, &edges, &node_data) {
        Ok(prog) => prog,
        Err(e) => {
            println!(
                "Failed to generate prog with nodes {:?}, edges {:?}, node data {:?}",
                nodes, edges, node_data
            );
            panic!("Error: {:?}", e);
        }
    };

    // let the program execute
    match test_prog.execute(
        SharedStreamMap::new(),
        test_info
            .get_execution_folder()
            .as_path()
            .to_str()
            .unwrap()
            .to_string(),
    ) {
        Ok(_) => {}
        Err(e) => {
            tracing::error!("Program execution failed: {:?}", e);
        }
    }

    let keywords = vec!["d"];
    assert!(test_info.check_grepped_output(&keywords));

    // if ok, can delete the folder
    test_info.delete_folder();
}

#[test]
pub fn triple_pipe_bufferable() {
    // two input files
    let test_info = TestInfo::new(String::from("triple_buffering_pipe"), 1, 3, 1000);
    test_info.setup_tmp_folder();
    test_info.generate_input();

    let nodes = vec!["cat", "cat", "cat", "write"];
    let mut edges: HashMap<(usize, usize), (String, bool)> = HashMap::default();
    edges.insert((1, 4), (String::from("pipe"), false));
    edges.insert((2, 4), (String::from("pipe"), true)); // second pipe must buffer its output
    edges.insert((3, 4), (String::from("pipe"), true)); // third pipe must buffer its output
    let mut node_data: HashMap<usize, NodeInfo> = HashMap::default();
    node_data.insert(
        1,
        NodeInfo {
            input_file: Some(test_info.input_file_name(0)),
            ..Default::default()
        },
    );
    node_data.insert(
        2,
        NodeInfo {
            input_file: Some(test_info.input_file_name(1)),
            ..Default::default()
        },
    );
    node_data.insert(
        3,
        NodeInfo {
            input_file: Some(test_info.input_file_name(2)),
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
    let mut test_prog = match generate_program(&nodes, &edges, &node_data) {
        Ok(prog) => prog,
        Err(e) => {
            println!(
                "Failed to generate prog with nodes {:?}, edges {:?}, node data {:?}",
                nodes, edges, node_data
            );
            panic!("Error: {:?}", e);
        }
    };

    // let the program execute
    match test_prog.execute(
        SharedStreamMap::new(),
        test_info
            .get_execution_folder()
            .as_path()
            .to_str()
            .unwrap()
            .to_string(),
    ) {
        Ok(_) => {}
        Err(e) => {
            tracing::error!("Program execution failed: {:?}", e);
        }
    }
    assert!(test_info.check_original_output());
    test_info.delete_folder();
}
