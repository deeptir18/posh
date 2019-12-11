extern crate dash;
extern crate shellwords;
use super::fileinfo::FileMap;
use dash::dag::{node, stream};
use dash::util::Result;
use failure::bail;
use shellwords::split;
use std::path::PathBuf;

/// Splits command into different stdin, stdout.
/// Creates node::Program where args are just string args.
/// Next steps will take the args and modify them.
pub fn shell_split(command: &str, filemap: &FileMap) -> Result<Vec<(node::Node, Vec<String>)>> {
    let shell_split = match split(&command) {
        Ok(s) => s,
        Err(e) => bail!("Mismatched quotes error: {:?}", e),
    };

    let piped_commands = split_by_pipe(shell_split);
    let mut ops: Vec<(node::Node, Vec<String>)> = Vec::new();
    for (i, x) in piped_commands.iter().enumerate() {
        let is_last: bool = i == piped_commands.len() - 1;
        match shell_parse(x.clone(), is_last, i, filemap) {
            Ok(op) => {
                ops.push(op);
            }
            Err(e) => {
                bail!(
                    "Error shell parsing {:?} into operation: {:?}",
                    x.clone(),
                    e
                );
            }
        }
    }

    Ok(ops)
}

/// Splits input by pipe.
fn split_by_pipe(inp: Vec<String>) -> Vec<Vec<String>> {
    let mut res: Vec<Vec<String>> = Vec::new();
    let mut piped_indices: Vec<usize> = Vec::new();
    for (i, x) in inp.iter().enumerate() {
        if x == "|" {
            piped_indices.push(i);
        }
    }
    piped_indices.push(inp.len());
    let mut last_index = 0;
    for x in &piped_indices {
        let mut insert: Vec<String> = Vec::new();
        // get slice of the input between this index and the last one
        if last_index != 0 {
            last_index += 1;
        }
        for i in last_index..*x {
            insert.push(inp[i].clone());
        }
        res.push(insert);
        last_index = *x;
    }
    if piped_indices.len() == 0 {
        res.push(inp);
    }
    res
}

/// Parses the command into shell directives.
/// TODO: this is super janky -- it would be better to integrate this into a real shell at some
/// point.
fn shell_parse(
    mut command: Vec<String>,
    is_last: bool,
    idx: usize,
    filemap: &FileMap,
) -> Result<(node::Node, Vec<String>)> {
    let mut args: Vec<String> = Vec::new();
    if command.len() == 0 {
        bail!("{:?} did not have initial base_command", command);
    }
    let base_command: String = command.remove(0);
    //args.push(base_command.clone());
    let mut op: node::Node = Default::default();
    op.set_name(&base_command);

    let stdout_default = match is_last {
        false => {
            stream::DataStream::new(stream::StreamType::Pipe, format!("pipe_{:?}", idx).as_ref())
        }
        true => stream::DataStream::new(stream::StreamType::LocalStdout, ""),
    };

    let stderr_default = stream::DataStream::new(stream::StreamType::LocalStdout, "");
    let stdin_default: stream::DataStream = match idx != 0 {
        true => stream::DataStream::new(
            stream::StreamType::Pipe,
            format!("pipe_{:?}", idx - 1).as_ref(),
        ),
        false => Default::default(),
    };

    // look for stdout, stderr and stdin redirection
    // TODO: can add in support for more complicated things like >>
    let stdout_arg = get_arg(&command, ">", stdout_default, filemap)?;
    op.set_stdout(stdout_arg);
    let stderr_arg = get_arg(&command, "2>", stderr_default, filemap)?;
    op.set_stderr(stderr_arg);
    let stdin_arg = get_arg(&command, "<", stdin_default, filemap)?;
    op.set_stdin(stdin_arg);

    // set the spawn or the run directives
    if !is_last {
        op.set_action(node::OpAction::Spawn);
    } else {
        op.set_action(node::OpAction::Run);
    }
    let mut skip_next = false;
    for arg in command {
        if skip_next {
            skip_next = false;
            continue;
        }
        // TODO: this doesn't seem like a correct way to do the shell parse
        if arg.contains(">") || arg.contains("2>") || arg.contains("<") {
            skip_next = true;
            continue;
        }
        args.push(arg);
    }

    Ok((op, args))
}

fn get_arg(
    args: &Vec<String>,
    pattern: &str,
    default_val: stream::DataStream,
    filemap: &FileMap,
) -> Result<stream::DataStream> {
    match get_arg_following(args, pattern) {
        Ok(s) => match s {
            Some(a) => {
                let pwd = PathBuf::new();
                match filemap.find_match(&a, &pwd) {
                    Some(fileinfo) => {
                        let stream = stream::DataStream::strip_prefix(
                            stream::StreamType::RemoteFile,
                            &a,
                            &fileinfo.0,
                        )?;
                        Ok(stream)
                    }
                    None => Ok(stream::DataStream::new(stream::StreamType::LocalFile, &a)),
                }
            }
            None => Ok(default_val),
        },
        Err(e) => bail!(
            "Provided shell redirection without following argument: {:?}",
            e
        ),
    }
}

fn get_arg_following(args: &Vec<String>, pattern: &str) -> Result<Option<String>> {
    match args
        .iter()
        .position(|s| s.to_string() == pattern.to_string())
    {
        Some(p) => {
            if p + 1 >= args.len() {
                bail!("No arg following {:?}", pattern);
            }
            Ok(Some(args[p + 1].clone()))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_get_arg_following() {
        let vec = vec!["foo".to_string(), "2>".to_string(), "foobar".to_string()];
        assert_eq!(
            get_arg_following(&vec, "2>").unwrap().unwrap(),
            "foobar".to_string()
        );
        let vec = vec!["foo".to_string(), ">".to_string(), "foobar".to_string()];
        match get_arg_following(&vec, "2>").unwrap() {
            Some(_) => {
                assert!(false, "Option should not be valid");
            }
            None => {}
        }

        let vec = vec!["foo".to_string(), ">".to_string()];
        match get_arg_following(&vec, ">") {
            Ok(_) => {
                assert!(false, "get_arg_following should have failed.");
            }
            Err(_) => {}
        }
    }

    fn get_test_filemap() -> FileMap {
        let mut map: HashMap<String, String> = HashMap::default();
        map.insert("/d/c/".to_string(), "127.0.0.1".to_string());
        FileMap::construct(map)
    }
    #[test]
    fn test_get_arg() {
        let filemap = get_test_filemap();
        let default_stream = stream::DataStream::default();
        let args = vec![
            "cat".to_string(),
            "foo".to_string(),
            ">".to_string(),
            "/d/c/b/a".to_string(),
        ];
        assert_eq!(
            get_arg(&args, ">", default_stream, &filemap).unwrap(),
            stream::DataStream::new(stream::StreamType::RemoteFile, "b/a")
        );
    }

    #[test]
    fn test_split_by_pipe() {
        let input = vec![
            "cat".to_string(),
            "foo".to_string(),
            "|".to_string(),
            "wc".to_string(),
            "|".to_string(),
            "sort".to_string(),
        ];
        let expected_output: Vec<Vec<String>> = vec![
            vec!["cat".to_string(), "foo".to_string()],
            vec!["wc".to_string()],
            vec!["sort".to_string()],
        ];
        assert_eq!(split_by_pipe(input), expected_output);
    }

    #[test]
    fn test_shell_parse() {
        let args = vec![
            "cat".to_string(),
            "foo".to_string(),
            ">".to_string(),
            "/d/c/b/a".to_string(),
        ];
        let filemap = get_test_filemap();
        let expected_op = node::Node::construct(
            "cat".to_string(),
            vec![],
            stream::DataStream::default(),
            stream::DataStream::new(stream::StreamType::RemoteFile, "b/a"),
            stream::DataStream::new(stream::StreamType::LocalStdout, ""),
            node::OpAction::Run,
            node::ExecutionLocation::Client,
        );
        let expected_args = vec!["foo".to_string()];
        assert_eq!(
            shell_parse(args, true, 0, &filemap).unwrap(),
            (expected_op, expected_args)
        );
    }

    #[test]
    fn test_shell_split() {
        let filemap = get_test_filemap();
        let command = "grep < /d/c/foo.txt | wc -l | sort > blah.txt";
        let mut expected_output: Vec<(node::Node, Vec<String>)> = Vec::new();
        expected_output.push((
            node::Node::construct(
                "grep".to_string(),
                vec![],
                stream::DataStream::new(stream::StreamType::RemoteFile, "foo.txt"),
                stream::DataStream::new(stream::StreamType::Pipe, "pipe_0"),
                stream::DataStream::new(stream::StreamType::LocalStdout, ""),
                node::OpAction::Spawn,
                node::ExecutionLocation::Client,
            ),
            vec![],
        ));
        expected_output.push((
            node::Node::construct(
                "wc".to_string(),
                vec![],
                stream::DataStream::new(stream::StreamType::Pipe, "pipe_0"),
                stream::DataStream::new(stream::StreamType::Pipe, "pipe_1"),
                stream::DataStream::new(stream::StreamType::LocalStdout, ""),
                node::OpAction::Spawn,
                node::ExecutionLocation::Client,
            ),
            vec!["-l".to_string()],
        ));
        expected_output.push((
            node::Node::construct(
                "sort".to_string(),
                vec![],
                stream::DataStream::new(stream::StreamType::Pipe, "pipe_1"),
                stream::DataStream::new(stream::StreamType::LocalFile, "blah.txt"),
                stream::DataStream::new(stream::StreamType::LocalStdout, ""),
                node::OpAction::Run,
                node::ExecutionLocation::Client,
            ),
            vec![],
        ));

        assert_eq!(shell_split(command, &filemap).unwrap(), expected_output);
    }
}
