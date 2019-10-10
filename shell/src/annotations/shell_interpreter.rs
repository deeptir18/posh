extern crate dash;
extern crate shellwords;

use dash::dag::{node, stream};
use dash::util::Result;
use failure::bail;
use shellwords::split;

/// Splits command into different stdin, stdout.
/// Creates node::Program where args are just string args.
/// Next steps will take the args and modify them.
pub fn shell_split(command: &str) -> Result<Vec<(node::Node, Vec<String>)>> {
    let shell_split = match split(&command) {
        Ok(s) => s,
        Err(e) => bail!("Mismatched quotes error: {:?}", e),
    };

    let piped_commands = split_by_pipe(shell_split);
    let mut ops: Vec<(node::Node, Vec<String>)> = Vec::new();
    for (i, x) in piped_commands.iter().enumerate() {
        let is_last: bool = i == piped_commands.len() - 1;
        match shell_parse(x.clone(), is_last, i) {
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
) -> Result<(node::Node, Vec<String>)> {
    if command.len() == 0 {
        bail!("{:?} did not have initial base_command", command);
    }
    let base_command: String = command.remove(0);

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
    let stdout_arg = get_arg(&command, ">", stdout_default)?;
    op.set_stdout(stdout_arg);
    let stderr_arg = get_arg(&command, "2>", stderr_default)?;
    op.set_stderr(stderr_arg);
    let stdin_arg = get_arg(&command, "<", stdin_default)?;
    op.set_stdin(stdin_arg);

    // set the spawn or the run directives
    if !is_last {
        op.set_action(node::OpAction::Spawn);
    } else {
        op.set_action(node::OpAction::Run);
    }
    let mut args: Vec<String> = Vec::new();
    let mut skip_next = false;
    for arg in command {
        if skip_next {
            skip_next = false;
            continue;
        }
        if (arg.contains(">") || arg.contains("2>") || arg.contains("<")) {
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
) -> Result<stream::DataStream> {
    match get_arg_following(args, pattern) {
        Ok(s) => match s {
            Some(a) => Ok(stream::DataStream::new(stream::StreamType::LocalFile, &a)),
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
