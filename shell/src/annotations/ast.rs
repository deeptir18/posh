///! eventually for parsing more complicated syntax of bash commands
extern crate dash;
extern crate shellwords;
use dash::dag::{node, stream};
use dash::util::Result;
use failure::bail;
use shellwords::split;

pub fn parse_input(input: String) -> Result<node::Program> {
    let shell_split = match split(&input) {
        Ok(s) => s,
        Err(e) => bail!("Mismatched quotes error: {:?}", e),
    };
    let piped_commands = split_by_pipe(shell_split);

    let mut ops: Vec<node::Op> = Vec::new();
    for (i, x) in piped_commands.iter().enumerate() {
        let is_last: bool = i == piped_commands.len() - 1;
        match parse_command(x.clone(), is_last, i) {
            Ok(op) => {
                ops.push(op);
            }
            Err(e) => {
                bail!("Error parsing {:?} into operation: {:?}", x.clone(), e);
            }
        }
    }

    Ok(node::Program::new(ops))
}

// iterates through input looking for where the pipes stop and start
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

// for now: everything must be separated by a whitespace
fn parse_command(mut command: Vec<String>, is_last: bool, idx: usize) -> Result<node::Op> {
    if command.len() == 0 {
        bail!("{:?} did not have initial base_command", command);
    }
    let base_command = command.remove(0);
    let stdout_default = match is_last {
        false => {
            // stdout is piped
            stream::DataStream {
                stream_type: stream::StreamType::Pipe,
                name: format!("pipe_{:?}", idx),
            }
        }
        true => stream::DataStream {
            stream_type: stream::StreamType::LocalStdout,
            name: "".to_string(),
        },
    };

    // iterate through command to get vector of rest of args

    let stdout_arg = get_datastream_arg(&command, ">", stdout_default)?;

    let stderr_default = stream::DataStream {
        stream_type: stream::StreamType::LocalStdout,
        name: "".to_string(),
    };

    let stderr_arg = get_datastream_arg(&command, "2>", stderr_default)?;

    let stdin_arg: Option<stream::DataStream> = match idx != 0 {
        true => Some(stream::DataStream {
            stream_type: stream::StreamType::Pipe,
            name: format!("pipe_{:?}", idx - 1),
        }),
        false => None,
    };

    let action = match is_last {
        true => node::OpAction::Run,
        false => node::OpAction::Spawn,
    };

    // parse the args
    let mut cmd_args: Vec<node::OpArg> = Vec::new();
    for arg in command {
        if arg.contains(">") {
            break; // ignore standard err or standard output redirection, already handled
        }

        match parse_file_location(arg.clone()) {
            Some(s) => {
                cmd_args.push(node::OpArg::Stream(s));
            }
            None => {
                cmd_args.push(node::OpArg::Arg(arg.clone()));
            }
        }
    }

    Ok(node::Op::ShellCommand {
        name: base_command,
        arguments: cmd_args,
        stdin: stdin_arg,
        stdout: stdout_arg,
        stderr: stderr_arg,
        action: action,
    })
}

fn get_datastream_arg(
    args: &Vec<String>,
    pattern: &str,
    default_val: stream::DataStream,
) -> Result<stream::DataStream> {
    match get_arg_following(args, pattern) {
        Some(s) => {
            if let Some(file_stream) = parse_file_location(s) {
                Ok(file_stream)
            } else {
                bail!("Provided '{}' without filename argument after", pattern);
            }
        }
        None => Ok(default_val),
    }
}

fn get_arg_following(args: &Vec<String>, pattern: &str) -> Option<String> {
    match args
        .iter()
        .position(|s| s.to_string() == pattern.to_string())
    {
        Some(p) => {
            if p + 1 >= args.len() {
                return None;
            }
            Some(args[p + 1].clone())
        }
        None => None,
    }
}

// tries to guess if the argument is a file or not
fn parse_file_location(arg: String) -> Option<stream::DataStream> {
    if arg.contains("LOCAL:") {
        let filename = arg.replace("LOCAL:", "").to_string();
        return Some(stream::DataStream {
            stream_type: stream::StreamType::LocalFile,
            name: filename,
        });
    } else if arg.contains("REMOTE:") {
        let filename = arg.replace("REMOTE:", "").to_string();
        return Some(stream::DataStream {
            stream_type: stream::StreamType::RemoteFile,
            name: filename,
        });
    } else {
        return None;
    }
}

// write some tests to make sure this super basic CLI work
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        let output = parse_input("cat LOCAL:foo".to_string()).unwrap();
        let answer = node::Program::new(vec![node::Op::ShellCommand {
            name: "cat".to_string(),
            arguments: vec![node::OpArg::Stream(stream::DataStream {
                name: "foo".to_string(),
                stream_type: stream::StreamType::LocalFile,
            })],
            stdin: None,
            stdout: stream::DataStream {
                name: "".to_string(),
                stream_type: stream::StreamType::LocalStdout,
            },
            stderr: stream::DataStream {
                name: "".to_string(),
                stream_type: stream::StreamType::LocalStdout,
            },
            action: node::OpAction::Run,
        }]);
        assert!(
            output == answer,
            format!("output: {:?}\n, true answer: {:?}", output, answer)
        );
    }

    #[test]
    fn test_basic_pipe() {
        let output = parse_input("cat REMOTE:foo | grep foobar".to_string()).unwrap();
        let answer = node::Program::new(vec![
            node::Op::ShellCommand {
                name: "cat".to_string(),
                arguments: vec![node::OpArg::Stream(stream::DataStream {
                    name: "foo".to_string(),
                    stream_type: stream::StreamType::RemoteFile,
                })],
                stdin: None,
                stdout: stream::DataStream {
                    name: "pipe_0".to_string(),
                    stream_type: stream::StreamType::Pipe,
                },
                stderr: stream::DataStream {
                    name: "".to_string(),
                    stream_type: stream::StreamType::LocalStdout,
                },
                action: node::OpAction::Spawn,
            },
            node::Op::ShellCommand {
                name: "grep".to_string(),
                arguments: vec![node::OpArg::Arg("foobar".to_string())],
                stdin: Some(stream::DataStream {
                    name: "pipe_0".to_string(),
                    stream_type: stream::StreamType::Pipe,
                }),
                stdout: stream::DataStream {
                    name: "".to_string(),
                    stream_type: stream::StreamType::LocalStdout,
                },
                stderr: stream::DataStream {
                    name: "".to_string(),
                    stream_type: stream::StreamType::LocalStdout,
                },
                action: node::OpAction::Run,
            },
        ]);
        assert!(
            output == answer,
            format!("\noutput:\n{:?}\n\n true answer:\n {:?}", output, answer)
        );
    }
    #[test]
    fn test_multiple_pipes() {
        let output = parse_input("cat REMOTE:foo | sort | uniq".to_string()).unwrap();
        let answer = node::Program::new(vec![
            node::Op::ShellCommand {
                name: "cat".to_string(),
                arguments: vec![node::OpArg::Stream(stream::DataStream {
                    name: "foo".to_string(),
                    stream_type: stream::StreamType::RemoteFile,
                })],
                stdin: None,
                stdout: stream::DataStream {
                    name: "pipe_0".to_string(),
                    stream_type: stream::StreamType::Pipe,
                },
                stderr: stream::DataStream {
                    name: "".to_string(),
                    stream_type: stream::StreamType::LocalStdout,
                },
                action: node::OpAction::Spawn,
            },
            node::Op::ShellCommand {
                name: "sort".to_string(),
                arguments: vec![],
                stdin: Some(stream::DataStream {
                    name: "pipe_0".to_string(),
                    stream_type: stream::StreamType::Pipe,
                }),
                stdout: stream::DataStream {
                    name: "pipe_1".to_string(),
                    stream_type: stream::StreamType::Pipe,
                },
                stderr: stream::DataStream {
                    name: "".to_string(),
                    stream_type: stream::StreamType::LocalStdout,
                },
                action: node::OpAction::Spawn,
            },
            node::Op::ShellCommand {
                name: "uniq".to_string(),
                arguments: vec![],
                stdin: Some(stream::DataStream {
                    name: "pipe_1".to_string(),
                    stream_type: stream::StreamType::Pipe,
                }),
                stdout: stream::DataStream {
                    name: "".to_string(),
                    stream_type: stream::StreamType::LocalStdout,
                },
                stderr: stream::DataStream {
                    name: "".to_string(),
                    stream_type: stream::StreamType::LocalStdout,
                },
                action: node::OpAction::Run,
            },
        ]);
        assert!(
            output == answer,
            format!("\noutput:\n{:?}\n\n true answer:\n{:?}", output, answer)
        );
    }

    // TODO: debug this test
    fn test_awk_string() {
        let output = parse_input("cat REMOTE:shakes_new.txt | wc | awk '{print \"Lines: \" $1 \"\tWords: \"$2 \"\tCharacter: \" $3}'".to_string()).unwrap();
        let answer = node::Program::new(vec![
            node::Op::ShellCommand {
                name: "cat".to_string(),
                arguments: vec![node::OpArg::Stream(stream::DataStream {
                    name: "shakes_new.txt".to_string(),
                    stream_type: stream::StreamType::RemoteFile,
                })],
                stdin: None,
                stdout: stream::DataStream {
                    name: "pipe_0".to_string(),
                    stream_type: stream::StreamType::Pipe,
                },
                stderr: stream::DataStream {
                    name: "".to_string(),
                    stream_type: stream::StreamType::LocalStdout,
                },
                action: node::OpAction::Spawn,
            },
            node::Op::ShellCommand {
                name: "wc".to_string(),
                arguments: vec![],
                stdin: Some(stream::DataStream {
                    name: "pipe_0".to_string(),
                    stream_type: stream::StreamType::Pipe,
                }),
                stdout: stream::DataStream {
                    name: "pipe_1".to_string(),
                    stream_type: stream::StreamType::Pipe,
                },
                stderr: stream::DataStream {
                    name: "".to_string(),
                    stream_type: stream::StreamType::LocalStdout,
                },
                action: node::OpAction::Spawn,
            },
            node::Op::ShellCommand {
                name: "awk".to_string(),
                arguments: vec![node::OpArg::Arg(
                    "{print \"Lines: \" $1 \"\tWords: \" $2 \"\tCharacter: \" $3}".to_string(),
                )],
                stdin: Some(stream::DataStream {
                    name: "pipe_1".to_string(),
                    stream_type: stream::StreamType::Pipe,
                }),
                stdout: stream::DataStream {
                    name: "".to_string(),
                    stream_type: stream::StreamType::LocalStdout,
                },
                stderr: stream::DataStream {
                    name: "".to_string(),
                    stream_type: stream::StreamType::LocalStdout,
                },
                action: node::OpAction::Run,
            },
        ]);
        assert!(
            output == answer,
            format!("\noutput:\n{:?}\n\n true answer:\n{:?}", output, answer)
        );
    }

    #[test]
    fn test_stdout_redirection() {
        let output = parse_input("cat REMOTE:foo | sort | uniq > LOCAL:foo".to_string()).unwrap();
        let answer = node::Program::new(vec![
            node::Op::ShellCommand {
                name: "cat".to_string(),
                arguments: vec![node::OpArg::Stream(stream::DataStream {
                    name: "foo".to_string(),
                    stream_type: stream::StreamType::RemoteFile,
                })],
                stdin: None,
                stdout: stream::DataStream {
                    name: "pipe_0".to_string(),
                    stream_type: stream::StreamType::Pipe,
                },
                stderr: stream::DataStream {
                    name: "".to_string(),
                    stream_type: stream::StreamType::LocalStdout,
                },
                action: node::OpAction::Spawn,
            },
            node::Op::ShellCommand {
                name: "sort".to_string(),
                arguments: vec![],
                stdin: Some(stream::DataStream {
                    name: "pipe_0".to_string(),
                    stream_type: stream::StreamType::Pipe,
                }),
                stdout: stream::DataStream {
                    name: "pipe_1".to_string(),
                    stream_type: stream::StreamType::Pipe,
                },
                stderr: stream::DataStream {
                    name: "".to_string(),
                    stream_type: stream::StreamType::LocalStdout,
                },
                action: node::OpAction::Spawn,
            },
            node::Op::ShellCommand {
                name: "uniq".to_string(),
                arguments: vec![],
                stdin: Some(stream::DataStream {
                    name: "pipe_1".to_string(),
                    stream_type: stream::StreamType::Pipe,
                }),
                stdout: stream::DataStream {
                    name: "foo".to_string(),
                    stream_type: stream::StreamType::LocalFile,
                },
                stderr: stream::DataStream {
                    name: "".to_string(),
                    stream_type: stream::StreamType::LocalStdout,
                },
                action: node::OpAction::Run,
            },
        ]);
        assert!(
            output == answer,
            format!("\noutput:\n{:?}\n\n true answer:\n{:?}", output, answer)
        );
    }
    #[test]
    fn test_stderr_redirection() {
        let output =
            parse_input("cat REMOTE:foo | sort | uniq > LOCAL:foo 2> LOCAL:/dev/null".to_string())
                .unwrap();
        let answer = node::Program::new(vec![
            node::Op::ShellCommand {
                name: "cat".to_string(),
                arguments: vec![node::OpArg::Stream(stream::DataStream {
                    name: "foo".to_string(),
                    stream_type: stream::StreamType::RemoteFile,
                })],
                stdin: None,
                stdout: stream::DataStream {
                    name: "pipe_0".to_string(),
                    stream_type: stream::StreamType::Pipe,
                },
                stderr: stream::DataStream {
                    name: "".to_string(),
                    stream_type: stream::StreamType::LocalStdout,
                },
                action: node::OpAction::Spawn,
            },
            node::Op::ShellCommand {
                name: "sort".to_string(),
                arguments: vec![],
                stdin: Some(stream::DataStream {
                    name: "pipe_0".to_string(),
                    stream_type: stream::StreamType::Pipe,
                }),
                stdout: stream::DataStream {
                    name: "pipe_1".to_string(),
                    stream_type: stream::StreamType::Pipe,
                },
                stderr: stream::DataStream {
                    name: "".to_string(),
                    stream_type: stream::StreamType::LocalStdout,
                },
                action: node::OpAction::Spawn,
            },
            node::Op::ShellCommand {
                name: "uniq".to_string(),
                arguments: vec![],
                stdin: Some(stream::DataStream {
                    name: "pipe_1".to_string(),
                    stream_type: stream::StreamType::Pipe,
                }),
                stdout: stream::DataStream {
                    name: "foo".to_string(),
                    stream_type: stream::StreamType::LocalFile,
                },
                stderr: stream::DataStream {
                    name: "/dev/null".to_string(),
                    stream_type: stream::StreamType::LocalFile,
                },
                action: node::OpAction::Run,
            },
        ]);
        assert!(
            output == answer,
            format!("\noutput:\n{:?}\n\n true answer:\n{:?}", output, answer)
        );
    }
}
