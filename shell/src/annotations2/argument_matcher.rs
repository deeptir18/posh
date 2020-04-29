use super::config::filecache::FileCache;
use super::config::network::FileNetwork;
use super::grammar::*;
use clap::ArgMatches;
use dash::graph::command::NodeArg;
use dash::graph::filestream::FileStream;
use dash::graph::Location;
use dash::util::Result;
use failure::bail;
use glob::glob;
use std::collections::HashMap;
use std::env;
use std::path::Path;
/// Attempts to run glob on the input FileStream and returns a vector of NodeArgs.
/// TODO: some of the Errors in glob might result from certain directories being unreadable.
fn glob_wrapper(input: &FileStream) -> Option<Vec<NodeArg>> {
    let input_path = input.get_path();
    let input_str = match input_path.to_str() {
        Some(s) => s,
        None => {
            return None;
        }
    };
    tracing::debug!("Calling glob on {:?}", input_str);
    match glob(input_str) {
        Ok(list) => {
            // TODO: unwrap here is not the best way to do this
            let args: Vec<NodeArg> = list
                .map(|x| {
                    NodeArg::Stream(FileStream::new(x.unwrap().as_path(), Location::default()))
                })
                .collect();
            if args.len() == 0 {
                return None;
            }
            Some(args)
        }
        Err(_) => None,
    }
}
/// Specifies a parsed mapping between arguments that appear at runtime.
/// TODO: would be eaiser if clap just exposed a argmatch method itself.
/// TODO: with this, you can handle subcommands properly
#[derive(Debug, PartialEq, Clone)]
pub struct ArgMatch {
    /// What is the command name
    cmd_name: Vec<String>,
    /// List of arguments in invocation. By default, entire unparsed string.
    arg_list: Vec<Vec<NodeArg>>,
    /// Map between Arguments and index into the arg list that the argument corresponds to.
    map: HashMap<usize, Argument>,
    /// Corresponding annotation's parsing options.
    parsing_options: ParsingOptions,
    /// Is there an arg that is splittable?
    splittable_arg: Option<usize>,
}

/// Helper struct to handle when nodes are assigned to locations where a certain argument doesn't
/// live
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct RemoteAccessInfo {
    /// Original name of the filestream,
    pub filestream: FileStream,
    /// What is the name of the temporary location of the filestream
    tmp: FileStream,
    /// Which argument is it,
    arg_idx: usize,
    /// Which value in the values for this argument is it,
    val_idx: usize,
    /// original location of the file,
    pub origin_location: Location,
    /// Final location of the file,
    pub access_location: Location,
    /// Argument type (can only be "input" or "output")
    pub argtype: ArgType,
}

impl RemoteAccessInfo {
    pub fn new(
        fs: FileStream,
        arg_idx: usize,
        val_idx: usize,
        origin_location: Location,
        access_location: Location,
        argtype: ArgType,
    ) -> Self {
        RemoteAccessInfo {
            filestream: fs,
            arg_idx: arg_idx,
            val_idx: val_idx,
            origin_location: origin_location,
            access_location: access_location,
            argtype: argtype,
            ..Default::default()
        }
    }

    pub fn set_tmp_name(&mut self, mut tmp_name: FileStream) {
        tmp_name.set_location(self.access_location.clone());
        self.tmp = tmp_name
    }

    pub fn get_arg_idx(&self) -> usize {
        self.arg_idx
    }

    pub fn get_val_idx(&self) -> usize {
        self.val_idx
    }
}

impl ArgMatch {
    pub fn new(
        command_name: Vec<String>,
        matches: ArgMatches,
        annotation: &Command,
        annotation_map: HashMap<String, usize>,
    ) -> Result<Self> {
        let mut arg_list: Vec<Vec<NodeArg>> = Vec::new();
        let mut map: HashMap<usize, Argument> = HashMap::default();
        let mut ct: usize = 0;
        let mut splittable_arg: Option<usize> = None;
        let handle_param = |arg_key: String,
                            argument: &Argument,
                            ct: &mut usize,
                            map: &mut HashMap<usize, Argument>,
                            arg_list: &mut Vec<Vec<NodeArg>>|
         -> () {
            // unwrap safe because arg_key is known to occur in matches
            let mut values = matches.values_of(arg_key).unwrap();
            let mut val_list: Vec<NodeArg> = Vec::new();
            while let Some(value) = values.next() {
                match argument {
                    Argument::LoneOption(_) => {}
                    Argument::OptWithParam(_, param) | Argument::LoneParam(param) => {
                        match param.param_type {
                            ArgType::Str => val_list.push(NodeArg::Str(value.to_string())),
                            ArgType::InputFile
                            | ArgType::OutputFile
                            | ArgType::InputFileList
                            | ArgType::OutputFileList => {
                                val_list.push(NodeArg::Stream(FileStream::new(
                                    Path::new(value),
                                    Location::default(),
                                )));
                            }
                        }
                    }
                }
            }
            arg_list.push(val_list);
            map.insert(*ct, argument.clone());
            *ct += 1;
        };
        // first pass for the options or options with parameters
        for arg in matches.args.iter() {
            let arg_info = &annotation.args[annotation_map[arg.0.clone()] as usize];
            match arg_info {
                Argument::LoneOption(opt) => {
                    arg_list.push(vec![]);
                    map.insert(ct, Argument::LoneOption(opt.clone()));
                    ct += 1;
                }
                Argument::OptWithParam(_opt, param) => {
                    if param.splittable {
                        splittable_arg = Some(ct);
                    }
                    handle_param(
                        arg.0.to_string(),
                        &arg_info,
                        &mut ct,
                        &mut map,
                        &mut arg_list,
                    );
                }
                _ => {}
            }
        }
        // second pass for the lone parameter
        // NOTE: need to do n^2 scan over matches -> make sure lone params appear in order they
        // appear in Command Struct
        let mut count: u32 = 0;
        for annotation_arg in annotation.args.iter() {
            match annotation_arg {
                Argument::LoneOption(_) | Argument::OptWithParam(_, _) => {}
                Argument::LoneParam(_param) => {
                    for arg in matches.args.iter() {
                        if arg.0.clone() == count.to_string() {
                            let arg_info: &Argument =
                                &annotation.args[annotation_map[arg.0.clone()] as usize];
                            match arg_info {
                                Argument::LoneParam(param) => {
                                    if param.splittable {
                                        splittable_arg = Some(ct);
                                    }
                                    handle_param(
                                        arg.0.to_string(),
                                        &annotation_arg,
                                        &mut ct,
                                        &mut map,
                                        &mut arg_list,
                                    );
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            count += 1;
        }
        Ok(ArgMatch {
            cmd_name: command_name,
            arg_list: arg_list,
            map: map,
            parsing_options: annotation.parsing_options.clone(),
            splittable_arg: splittable_arg,
        })
    }

    pub fn new_default(cmd: &str, invocation: &Vec<String>) -> Self {
        let arg_list: Vec<Vec<NodeArg>> = invocation
            .iter()
            .map(|x| vec![NodeArg::Str(x.clone())])
            .collect();
        ArgMatch {
            cmd_name: vec![cmd.to_string()],
            arg_list: arg_list,
            map: HashMap::default(),
            parsing_options: ParsingOptions::default(),
            splittable_arg: None,
        }
    }

    fn clone_with_repl_arg(&self, ind: usize, repl: Vec<NodeArg>) -> Result<ArgMatch> {
        let mut list_repl = self.arg_list.clone();
        if ind >= list_repl.len() {
            bail!("Replacing argument with index that is too large: {:?}", ind);
        }
        list_repl[ind] = repl;
        Ok(ArgMatch {
            cmd_name: self.cmd_name.clone(),
            arg_list: list_repl,
            map: self.map.clone(),
            parsing_options: self.parsing_options.clone(),
            splittable_arg: None,
        })
    }

    /// Reconstructs the arguments into a string that can be used at runtime.
    pub fn reconstruct(&self) -> Result<Vec<NodeArg>> {
        // TODO: handle subcommands in a more robust way

        let mut ret: Vec<NodeArg> = Vec::new();
        if self.cmd_name.len() > 1 {
            for cmpt in self.cmd_name[1..].iter() {
                ret.push(NodeArg::Str(format!("{}", cmpt)));
            }
        }
        // iterate through the options first, then lone parameters last
        for (ind, args) in self.arg_list.iter().enumerate() {
            let arg_info = self.map.get(&ind).unwrap();
            match arg_info {
                Argument::LoneOption(opt) => {
                    assert!(args.len() == 0);
                    if opt.short != "" {
                        ret.push(NodeArg::Str(format!("-{}", &opt.short)));
                    } else {
                        if self.parsing_options.long_arg_single_dash {
                            ret.push(NodeArg::Str(format!("-{}", &opt.long)));
                        } else {
                            ret.push(NodeArg::Str(format!("--{}", &opt.long)));
                        }
                    }
                }
                Argument::OptWithParam(opt, param) => {
                    if param.attached_to_short && opt.short != "" {
                        if param.param_type != ArgType::Str {
                            bail!("Dash doesn't handle attached_to_short for non-string arg types");
                        }
                        assert!(param.size == ParamSize::One);
                        assert!(args.len() == 1);
                        match &args[0] {
                            NodeArg::Str(s) => {
                                ret.push(NodeArg::Str(format!("-{}{}", &opt.short, s)));
                            }
                            NodeArg::Stream(_) => {
                                unreachable!();
                            }
                        }
                        continue;
                    }

                    if opt.short != "" {
                        ret.push(NodeArg::Str(format!("-{}", &opt.short)));
                    } else {
                        if self.parsing_options.long_arg_single_dash {
                            ret.push(NodeArg::Str(format!("-{}", &opt.long)));
                        } else {
                            ret.push(NodeArg::Str(format!("--{}", &opt.long)));
                        }
                    }
                    // add the values
                    ret.append(&mut args.clone());
                }
                Argument::LoneParam(_) => {}
            }
        }

        // now add the lone parameters in to the return list
        for (ind, args) in self.arg_list.iter().enumerate() {
            let argument = self.map.get(&ind).unwrap();
            match argument {
                Argument::LoneOption(_) => {}
                Argument::OptWithParam(_, _) => {}
                Argument::LoneParam(_param) => {
                    // push all the values
                    ret.append(&mut args.clone());
                }
            }
        }

        Ok(ret)
    }

    /// Splits into multiple matches by the given argument.
    /// TODO: should eventually also be a part of scheduling -- but right now splits into chunk by
    /// machine in order
    /// TODO: is there a way to make this simpler/cleaner?
    pub fn split(&mut self, splitting_factor: u32, config: &FileNetwork) -> Result<Vec<ArgMatch>> {
        match self.splittable_arg {
            Some(ind) => {
                let mut args: Vec<Vec<NodeArg>> = Vec::new();
                let values = &mut self.arg_list[ind];
                let argument = self.map.get_mut(&ind).unwrap();
                let is_str_arg: bool = match argument {
                    Argument::OptWithParam(_, param) => match param.param_type {
                        ArgType::Str => true,
                        _ => false,
                    },
                    Argument::LoneParam(param) => match param.param_type {
                        ArgType::Str => true,
                        _ => false,
                    },
                    _ => {
                        unreachable!();
                    }
                };
                match is_str_arg {
                    // split evenly into even chunks
                    true => {
                        if splitting_factor == 1 {
                            return Ok(vec![]);
                        } else {
                            let chunk_size =
                                (values.len() as f32 / splitting_factor as f32).round() as usize;
                            if chunk_size > 0 {
                                for chunk in values.chunks(chunk_size) {
                                    args.push(chunk.iter().map(|x| x.clone()).collect());
                                }
                            } else {
                                // there are more splits then chunks; everything can be separate
                                args.append(&mut values.iter().map(|x| vec![x.clone()]).collect());
                            }
                            let res: Result<Vec<ArgMatch>> = args
                                .into_iter()
                                .map(|repl| self.clone_with_repl_arg(ind, repl))
                                .collect();
                            res
                        }
                    }
                    false => {
                        // separate into chunk by current location
                        let mut chunks: Vec<Vec<NodeArg>> = Vec::new();
                        let mut current_location: Option<Location> = None;
                        let mut location_map: HashMap<usize, Location> = HashMap::default();
                        let mut location_count: HashMap<Location, usize> = HashMap::default();
                        for arg in values.iter() {
                            match arg {
                                NodeArg::Str(_) => {
                                    bail!(
                                        "Arguments should be a filestream as param has type file"
                                    );
                                }
                                NodeArg::Stream(fs) => {
                                    let loc = config.get_location(&fs).clone();
                                    if Some(loc.clone()) == current_location {
                                        let chunksize = chunks.len() - 1;
                                        let last_chunk = &mut chunks[chunksize];
                                        last_chunk.push(NodeArg::Stream(fs.clone()));
                                    } else {
                                        if let Some(current) = current_location {
                                            // add marker for previous chunk index
                                            location_map.insert(chunks.len() - 1, current.clone());
                                        }
                                        // switch current and make a new chunk
                                        current_location = Some(loc.clone());
                                        chunks.push(vec![NodeArg::Stream(fs.clone())]);
                                        let current_count =
                                            location_count.get(&loc).unwrap_or(&0).clone();
                                        location_count.insert(loc, current_count + 1);
                                    }
                                }
                            }
                        }
                        // need to add entry into location_map in for the last chunk
                        match current_location {
                            Some(curr) => {
                                location_map.insert(chunks.len() - 1, curr);
                            }
                            None => {
                                bail!("Current location still none, this means no args");
                            }
                        }
                        let mut new_chunks: Vec<Vec<NodeArg>> = Vec::new();
                        for chunk_ind in 0..chunks.len() {
                            let loc = location_map.get(&chunk_ind).unwrap();
                            let ct = location_count.get(loc).unwrap();
                            let chunk_values = chunks[chunk_ind].clone();
                            if *ct == 1 {
                                let chunk_size =
                                    (chunk_values.len() as f32 / splitting_factor as f32).round()
                                        as usize;
                                if chunk_size > 0 {
                                    for chunk in chunk_values.chunks(chunk_size) {
                                        new_chunks.push(chunk.iter().map(|x| x.clone()).collect());
                                    }
                                } else {
                                    // there are more splits then chunks; everything can be separate
                                    new_chunks.append(
                                        &mut chunk_values.iter().map(|x| vec![x.clone()]).collect(),
                                    );
                                }
                            } else {
                                // when different mounts appear in different parts of the array in
                                // different orders
                                unimplemented!();
                            }
                        }
                        let res: Result<Vec<ArgMatch>> = new_chunks
                            .into_iter()
                            .map(|repl| self.clone_with_repl_arg(ind, repl))
                            .collect();
                        res
                    }
                }
            }
            None => Ok(vec![]),
        }
    }

    pub fn get_access_type(&self) -> AccessType {
        self.parsing_options.access_type
    }

    pub fn get_reduces_input(&self) -> bool {
        self.parsing_options.reduces_input
    }

    pub fn get_splittable_across_input(&self) -> bool {
        self.parsing_options.splittable_across_input
    }

    pub fn get_needs_current_dir(&self) -> bool {
        self.parsing_options.needs_current_dir
    }

    /// Returns vector of all the file related dependencies this node sees.
    pub fn file_dependencies(&self) -> Vec<(ArgType, FileStream)> {
        let mut ret: Vec<(ArgType, FileStream)> = Vec::new();
        for (i, args) in self.arg_list.iter().enumerate() {
            match self.map.get(&i).unwrap() {
                Argument::LoneOption(_) => {}
                Argument::OptWithParam(_, param) | Argument::LoneParam(param) => {
                    if param.is_file_type() {
                        for arg in args.iter() {
                            match arg {
                                NodeArg::Str(_) => {
                                    unreachable!();
                                }
                                NodeArg::Stream(fs) => {
                                    ret.push((param.param_type, fs.clone()));
                                }
                            }
                        }
                    }
                }
            }
        }
        ret
    }

    /// Resolve any glob related things within any of the arguments.
    pub fn resolve_glob(&mut self) -> Result<()> {
        let mut repl_list: Vec<(usize, Vec<NodeArg>)> = Vec::new();
        for (i, args) in self.arg_list.iter().enumerate() {
            match self.map.get(&i).unwrap() {
                Argument::LoneOption(_) => {}
                Argument::OptWithParam(_, param) | Argument::LoneParam(param) => {
                    if param.is_file_type() {
                        let mut repl_args: Vec<NodeArg> = Vec::new();
                        let mut changed = false;
                        for arg in args.iter() {
                            match arg {
                                NodeArg::Stream(fs) => match glob_wrapper(&fs) {
                                    Some(globbed) => {
                                        repl_args.append(&mut globbed.clone());
                                        changed = true;
                                    }
                                    None => {
                                        repl_args.push(arg.clone());
                                    }
                                },
                                NodeArg::Str(_) => {}
                            }
                        }
                        if changed {
                            repl_list.push((i, repl_args));
                        }
                    }
                }
            }
        }
        for (i, args) in repl_list.into_iter() {
            let _ = std::mem::replace(&mut self.arg_list[i], args);
        }

        Ok(())
    }

    /// Resolves any environment variables present in any arguments in the invocation.
    pub fn resolve_env_vars(&mut self) -> Result<()> {
        for arg_list in self.arg_list.iter_mut() {
            for arg in arg_list.iter_mut() {
                match arg {
                    NodeArg::Str(ref mut arg) => {
                        if arg.starts_with("$") {
                            let var_name = arg.split_at(1).1.to_string();
                            match env::var(&var_name) {
                                Ok(val) => {
                                    *arg = val;
                                }
                                Err(e) => {
                                    bail!(
                                        "Could not resolve environment variable {:?}->{:?}",
                                        arg,
                                        e
                                    );
                                }
                            }
                        }
                    }
                    NodeArg::Stream(ref mut fs) => {
                        fs.resolve_env_var()?;
                    }
                }
            }
        }
        Ok(())
    }

    /// For any file related arguments, resolve to full path.
    pub fn resolve_file_paths(&mut self, filecache: &mut FileCache, pwd: &Path) -> Result<()> {
        for (i, args) in self.arg_list.iter_mut().enumerate() {
            match self.map.get(&i).unwrap() {
                Argument::LoneOption(_) => {}
                Argument::OptWithParam(_, param) | Argument::LoneParam(param) => {
                    if param.is_file_type() {
                        for arg in args.iter_mut() {
                            match arg {
                                NodeArg::Stream(ref mut fs) => {
                                    filecache.resolve_path(fs, pwd)?;
                                }
                                NodeArg::Str(_) => unreachable!(),
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Iterates through file arguments and strips prefix in preparation for being sent to another
    /// machine.
    /// Also changes location of filepath to reflect the access location
    pub fn strip_file_paths(
        &mut self,
        origin_location: Location,
        location: Location,
        config: &FileNetwork,
    ) -> Result<Vec<RemoteAccessInfo>> {
        let mut remote_access: Vec<RemoteAccessInfo> = Vec::new();
        for (arg_idx, args) in self.arg_list.iter_mut().enumerate() {
            match self.map.get(&arg_idx).unwrap() {
                Argument::LoneOption(_) => {}
                Argument::OptWithParam(_, param) | Argument::LoneParam(param) => {
                    if param.is_file_type() {
                        for (val_idx, arg) in args.iter_mut().enumerate() {
                            match arg {
                                NodeArg::Stream(ref mut fs) => {
                                    let file_location = config.get_location(fs);
                                    if file_location == location {
                                        // modify path
                                        config.strip_file_path(fs, &origin_location, &location)?;
                                        fs.set_location(location.clone());
                                    } else {
                                        // need to modify argument for remote access
                                        if file_location == Location::Client
                                            && location != Location::Client
                                        {
                                            bail!("File {:?} cannot be accessed in loc {:?} from outside the client", fs, location);
                                        }
                                        let remote_access_info = RemoteAccessInfo::new(
                                            fs.clone(),
                                            arg_idx,
                                            val_idx,
                                            file_location.clone(),
                                            location.clone(),
                                            param.param_type,
                                        );
                                        remote_access.push(remote_access_info);
                                    }
                                }
                                NodeArg::Str(_) => {
                                    unreachable!();
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(remote_access)
    }

    pub fn change_arg(&mut self, info: &RemoteAccessInfo) -> Result<()> {
        let args = &mut self.arg_list[info.arg_idx];
        std::mem::replace(&mut args[info.val_idx], NodeArg::Stream(info.tmp.clone()));
        Ok(())
    }
}
