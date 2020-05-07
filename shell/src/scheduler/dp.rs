use super::annotations2::argument_matcher::ArgMatch;
use super::annotations2::grammar::*;
use super::config::filecache::FileCache;
use super::config::network::FileNetwork;
use super::Scheduler;
use dash::graph::info::Info;
use dash::graph::program::{Elem, Link, NodeId, Program};
use dash::graph::stream::DashStream;
use dash::graph::stream::IOType;
use dash::graph::Location;
use dash::util::Result;
use failure::bail;
use std::collections::HashMap;
use std::f64::INFINITY;
use std::path::{Path, PathBuf};
use std::time::Instant;

type NodeAssignment = (NodeId, Location);
#[derive(PartialEq, Debug, Clone, Default)]
struct DP {
    /// Cumulative transfer time until that node
    dp: HashMap<NodeAssignment, f64>,
    /// Keep track of "minimum location" to execute a node
    min_loc: HashMap<NodeId, (Location, f64)>,
    /// Predecessor location for backtracking: node assignment to map of pred -> pred location
    pred_loc: HashMap<NodeAssignment, HashMap<NodeId, (Location, f64)>>,
}

impl DP {
    pub fn get(&self, entry: &NodeAssignment) -> Result<f64> {
        match self.dp.get(entry) {
            Some(val) => Ok(*val),
            None => bail!("Dp not filled in for entry {:?}", entry),
        }
    }

    pub fn insert(&mut self, entry: NodeAssignment, val: f64) {
        self.dp.insert(entry, val);
    }

    pub fn add_pred_loc(&mut self, entry: NodeAssignment, pred: NodeId, loc: Location, val: f64) {
        match self.pred_loc.get_mut(&entry) {
            Some(pred_loc) => {
                pred_loc.insert(pred, (loc, val));
            }
            None => {
                let mut pred_loc: HashMap<NodeId, (Location, f64)> = HashMap::default();
                pred_loc.insert(pred, (loc, val));
                self.pred_loc.insert(entry, pred_loc);
            }
        }
    }

    pub fn get_pred_loc(&self, entry: &NodeAssignment) -> Result<HashMap<NodeId, (Location, f64)>> {
        match self.pred_loc.get(entry) {
            Some(map) => Ok(map.clone()),
            None => bail!("No pred loc available for node assignment {:?}", entry),
        }
    }

    pub fn get_min_loc(&self, id: &NodeId) -> Result<Location> {
        match self.min_loc.get(id) {
            Some(val) => Ok(val.0.clone()),
            None => bail!("No min location recorded for id {:?}", id),
        }
    }

    pub fn set_min_loc(&mut self, id: NodeId, location: Location, val: f64) -> Result<()> {
        match self.min_loc.get(&id) {
            Some((loc, old_val)) => {
                if *loc != location && *old_val != val {
                    tracing::error!(
                        "Min loc for id {:?} already set as {:?}, not {:?}, current val: {:?}, proposed val: {:?}",
                        id,
                        loc,
                        location,
                        old_val,
                        val,
                    );
                    bail!("Min loc for id {:?} already set as {:?}", id, loc);
                }
                Ok(())
            }
            None => {
                self.min_loc.insert(id, (location, val));
                Ok(())
            }
        }
    }
}
pub struct DPScheduler;

impl Scheduler for DPScheduler {
    fn schedule(
        &mut self,
        prog: &Program,
        match_map: &mut HashMap<NodeId, ArgMatch>,
        config: &FileNetwork,
        filecache: &mut FileCache,
        pwd: &Path,
    ) -> Result<HashMap<NodeId, Location>> {
        let mut assignments: HashMap<NodeId, Location> = HashMap::new();
        // iterate through each node to cache input file sizes
        let start = Instant::now();
        let mut query_paths: Vec<PathBuf> = Vec::new();
        for (id, node) in prog.get_nodes_iter() {
            match node.get_elem() {
                Elem::Cmd(_cmdnode) => {
                    let argmatch = match_map.get(id).unwrap();
                    // query for all of the file locations at once, at the server
                    for (argtype, fs) in argmatch.file_dependencies().iter() {
                        match argtype {
                            ArgType::InputFile => {
                                query_paths.push(fs.get_path());
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
        // query for file sizes
        filecache.get_sizes(&query_paths)?;
        tracing::error!(
            "Took {:?} to get all necessary filepaths",
            start.elapsed().as_secs()
        );
        // estimate weights of each edge
        let edge_weights = calculate_edge_weights(prog, match_map, filecache)?;

        // define a new DP to fill in
        let mut dp = DP::default();

        // execution order corresponds to topological ordering where all dependencies come first
        for id in prog.execution_order() {
            let entries: Vec<NodeAssignment> = config
                .get_location_list()
                .into_iter()
                .map(|loc| (id, loc))
                .collect();
            for entry in entries.into_iter() {
                tracing::debug!("Calculating dp for {:?}", entry);
                let val = calculate_dp(
                    &entry,
                    prog,
                    match_map,
                    config,
                    filecache,
                    pwd,
                    &edge_weights,
                    &mut dp,
                )?;
                tracing::debug!("Calculating dp for {:?} -> {:?}", entry, val);
                dp.insert(entry, val);
            }
        }

        backtrack(prog, config, &mut dp)?;

        // optimal assignment corresponds to the minimum location for each node
        for id in prog.execution_order() {
            // assumes location is never double counted between assignments
            // TODO: handle case where it is double counted
            let location = dp.get_min_loc(&id)?;
            assignments.insert(id, location);
        }

        Ok(assignments)
    }
}

fn backtrack(prog: &Program, config: &FileNetwork, dp: &mut DP) -> Result<()> {
    // backtrack the DP to figure out where to execute everything
    for sink_id in prog.get_sinks().iter() {
        // where is optimal location for each sink?
        let options: Vec<(Location, f64)> = config
            .get_location_list()
            .into_iter()
            .map(|loc| (loc.clone(), dp.get(&(*sink_id, loc.clone())).unwrap()))
            .collect();
        let mut min_loc_opt: Option<Location> = None;
        let mut min_val = INFINITY;
        for opt in options.into_iter() {
            if opt.1 < min_val {
                min_val = opt.1;
                min_loc_opt = Some(opt.0.clone());
            }
        }
        if min_val == INFINITY {
            bail!("All options for sink node {:?} are infinity", sink_id);
        }
        let min_loc = min_loc_opt.unwrap();
        dp.set_min_loc(*sink_id, min_loc.clone(), min_val)?;

        // now do the backtracking
        // if sink is for stderr, ignore while backtracking
        match prog.get_node(*sink_id).unwrap().get_elem() {
            Elem::Write(writenode) => match writenode.get_stdout().unwrap() {
                DashStream::Stderr => {
                    continue;
                }
                _ => {}
            },
            _ => {}
        }
        let mut stack: Vec<NodeId> = Vec::new();
        stack.insert(0, *sink_id);
        while stack.len() > 0 {
            let node = stack.pop().unwrap();
            tracing::info!("Exploring node {:?}", node);
            let current_min = dp.get_min_loc(&node).unwrap();
            if prog.get_dependent_nodes(node).len() > 0 {
                let predecessors = dp.get_pred_loc(&(node, current_min)).unwrap();
                for (id, (loc, val)) in predecessors.iter() {
                    tracing::debug!(
                        "Setting for {:?} loc {:?} with val {:?} as min",
                        id,
                        loc,
                        val
                    );
                    stack.insert(0, *id);
                    dp.set_min_loc(*id, loc.clone(), *val)?;
                }
            }
        }
    }

    Ok(())
}
fn calculate_edge_weights(
    prog: &Program,
    match_map: &mut HashMap<NodeId, ArgMatch>,
    filecache: &mut FileCache,
) -> Result<HashMap<Link, f64>> {
    let execution_order = prog.execution_order();
    let mut edge_weights: HashMap<Link, f64> = HashMap::new();
    for id in execution_order.iter() {
        let mut input_size: f64 = 0.0;
        // find the total size of the preceeding edges
        let preceeding_edges = prog.get_dependent_edges(*id);
        for preceeding_edge in preceeding_edges.into_iter() {
            match edge_weights.get(&preceeding_edge) {
                Some(size) => {
                    input_size += size;
                }
                None => {
                    bail!(
                        "Topological sort should prevent getting edge weight before it is recorded {:?}", preceeding_edge
                    );
                }
            }
        }
        let mut is_filter_node = false;
        match prog.get_node(*id).unwrap().get_elem() {
            Elem::Cmd(_cmdnode) => {
                let argmatch = match_map.get(id).unwrap();
                if argmatch.get_reduces_input() {
                    is_filter_node = true;
                }
                for (argtype, fs) in argmatch.file_dependencies().iter() {
                    match argtype {
                        ArgType::InputFile => {
                            let size = filecache.get_size(fs.get_path())?;
                            input_size += size as f64;
                        }
                        _ => {}
                    }
                }
            }
            Elem::Write(_writenode) => {
                // no input dependencies
            }
            Elem::Read(readnode) => {
                let fs = readnode.get_input_ref();
                let size = filecache.get_size(fs.get_path())?;
                input_size += size as f64;
            }
        }

        let mut output_size = match is_filter_node {
            true => input_size / 2.0,
            false => input_size as f64,
        };

        // if this node is a cmdnode, and writes to output files, output edge size is 0
        // assume all flow is directed to the output file
        match prog.get_node(*id).unwrap().get_elem() {
            Elem::Cmd(_cmdnode) => {
                let argmatch = match_map.get(id).unwrap();
                for (argtype, _) in argmatch.file_dependencies().iter() {
                    match argtype {
                        ArgType::OutputFile => {
                            output_size = 0.0;
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }

        let outgoing_edges = prog.get_outgoing_edges(*id);
        // right now, nodes can only have up to 2 outgoing edges:
        // 1 for stdout, 1 for stderr
        assert!(outgoing_edges.len() <= 2);
        for (iotype, outgoing_edge) in outgoing_edges.into_iter() {
            match iotype {
                IOType::Stdout => {
                    edge_weights.insert(outgoing_edge, output_size);
                }
                IOType::Stderr => {
                    // In our estimation, stderr edges carry no weight
                    edge_weights.insert(outgoing_edge, 0.0);
                }
                IOType::Stdin => {
                    unreachable!();
                }
            }
        }
    }

    Ok(edge_weights)
}

/// Represent constraints by returning 0 if the assigned location is not the potential location
fn constraint(assigned_loc: &Location, potential_loc: &Location) -> Result<f64> {
    if assigned_loc != potential_loc {
        return Ok(INFINITY);
    } else {
        return Ok(0.0);
    }
}

fn calculate_dp(
    entry: &NodeAssignment,
    prog: &Program,
    match_map: &HashMap<NodeId, ArgMatch>,
    config: &FileNetwork,
    filecache: &mut FileCache,
    pwd: &Path,
    edge_weights: &HashMap<Link, f64>,
    dp: &mut DP,
) -> Result<f64> {
    let id = entry.0;
    let location = &entry.1;
    let locations = config.get_location_list();
    // minimum DP over locations of a preceeding node + transfer time to current location
    let min_term = |prev_id: NodeId, dp: &mut DP| -> Result<f64> {
        let vals_result: Result<Vec<f64>> = locations
            .iter()
            .map(|loc| {
                let edge_size = match edge_weights.get(&Link::new(prev_id, id)) {
                    Some(e) => e,
                    None => {
                        bail!("No edge between {:?} and {:?}");
                    }
                };
                // if no link between two machines, dp is infinite
                let bw = config.network_speed(&loc, location).unwrap_or(0.0);
                let dp_val = dp.get(&(prev_id, loc.clone()))?;
                tracing::debug!("prev node: {:?}, curr node: {:?}, prev node loc: {:?}, edge size: {:?}, bw: {:?}, prev dp: {:?}, res: {:?}", prev_id, id, loc.clone(), edge_size, bw, dp_val, dp_val + edge_size/bw);
                if bw != 0.0 {
                    Ok(dp_val + edge_size / bw)
                } else {
                    Ok(INFINITY)
                }
            })
            .collect();
        let vals = vals_result?;
        tracing::debug!("prev vals: {:?}, prev locs: {:?}", vals, locations);
        let mut min_val = INFINITY;
        let mut min_idx: Option<usize> = None;
        for (idx, val) in vals.into_iter().enumerate() {
            if val <= min_val {
                min_idx = Some(idx);
                min_val = val;
            }
        }
        match min_idx {
            Some(idx) => {
                tracing::debug!(
                    "For entry {:?}, pred {:?}, adding min location of {:?}",
                    entry,
                    prev_id,
                    locations[idx]
                );
                dp.add_pred_loc(entry.clone(), prev_id, locations[idx].clone(), min_val);
                Ok(min_val)
            }
            None => bail!("Could not find min DP value"),
        }
    };

    let node = prog.get_node(id).unwrap();
    match node.get_elem() {
        Elem::Cmd(_cmdnode) => {
            // TODO: if all dependencies are the same -- don't need to query for size or pwd
            // location
            let argmatch = match_map.get(&id).unwrap();
            // time to gather all file dependencies at this location
            let mut input_time: f64 = 0.0;
            for (argtype, fs) in argmatch.file_dependencies().iter() {
                match argtype {
                    ArgType::InputFile => {
                        let file_location = config.get_location(fs);
                        let speed = config
                            .network_speed(location, &file_location)
                            .unwrap_or(0.0);
                        let file_size = filecache.get_size(fs.get_path())?;
                        if speed == 0.0 {
                            input_time += INFINITY;
                        } else {
                            input_time += file_size / speed;
                        }
                    }
                    _ => {}
                }
            }

            if argmatch.get_needs_current_dir() {
                // Querying for filesize can be extremely expensive
                // Especially for large nested repos (e.g., git)
                // Instead, assume that the directory size is infinity
                /*let dir_size = filecache.get_size(pwd.to_path_buf())?;
                let pwd_location = config.get_path_location(pwd.to_path_buf());
                let speed = config.network_speed(location, &pwd_location).unwrap_or(0.0);
                if speed == 0.0 {
                    input_time += INFINITY;
                } else {
                    input_time += dir_size / speed;
                }*/
                let pwd_location = config.get_path_location(pwd.to_path_buf());
                let time = constraint(location, &pwd_location)?;
                input_time += time;
            }

            // calculate sum of transferring previous nodes
            let mut dp_val = 0.0;
            for preceeding_id in prog.get_dependent_nodes(id).iter() {
                // will calculate min execution location for all preceeding terms
                let transfer_term = min_term(*preceeding_id, dp)?;
                dp_val += transfer_term;
            }
            return Ok(dp_val + input_time);
        }
        Elem::Read(readnode) => {
            assert!(prog.get_dependent_nodes(id).len() == 0);
            return constraint(location, &config.get_location(readnode.get_input_ref()));
        }
        Elem::Write(writenode) => {
            // calculate the preceeding min execution times
            let mut dp_val = 0.0;
            for preceeding_id in prog.get_dependent_nodes(id).iter() {
                let transfer_term = min_term(*preceeding_id, dp)?;
                dp_val += transfer_term;
            }
            let writecost = match writenode.get_stdout() {
                Some(stdout) => match stdout {
                    DashStream::File(fs) => constraint(location, &config.get_location(&fs)),
                    DashStream::Stdout | DashStream::Stderr => {
                        constraint(location, &Location::Client)
                    }
                    _ => {
                        unreachable!();
                    }
                },
                None => {
                    unreachable!();
                }
            }?;
            return Ok(dp_val + writecost);
        }
    }
}
