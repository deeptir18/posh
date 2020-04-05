use super::annotations2::argument_matcher::ArgMatch;
use super::config::filecache::FileCache;
use super::config::network::FileNetwork;
use super::Scheduler;
use dash::graph::filestream::FileStream;
use dash::graph::program::{Elem, NodeId, Program};
use dash::graph::stream::DashStream;
use dash::graph::Location;
use dash::util::Result;
use failure::bail;
use std::collections::{HashMap, HashSet};
use std::convert::Into;
use std::f64;
use std::iter::FromIterator;
use std::path::Path;
pub struct HeuristicScheduler;

/// Custom scheduling algorithm (DP/Max Flow based) to assign locations for nodes that haven't
/// previously been assigned.
/// Algorithm works as follows:
///     Iterate through each source->sink in the graph by following the edges from every node.
///     Assumes source->sink changes location (from the forced assignments) at most once.
///     For each path, assign each edge a weight depending on if the command node "reduces
///     output" or not.
///     Start the weight count at 1, and reduce by .5 if the node reduces output (absolute
///     numbers don't really matter here).
///     Once all edges in this path has a weight, calculate the optimal cut by looking for the
///     edge with the least weight.
///     If there are multiple edges with the least weight, choose among the edges with the
///     least weight that causes more nodes to be assigned to the server.
///     This source->sink path defines a location for each node.
///     Calculate the location according to each source->sink path.
///     If a node in the end has multiple locations, assign to the client.
///     Ignore all edges that go to a write node that writes to stderr.
/// There might be a better way to define this algorithm, figure that out.
/// Tries to optimize each "path" in the program independently.
fn optimize_node_schedule(
    prog: &Program,
    assigned: &mut HashMap<NodeId, Location>,
    match_map: &HashMap<NodeId, ArgMatch>,
) -> Result<()> {
    let mut possible_assignments: HashMap<NodeId, HashMap<Location, u32>> = HashMap::default();
    // closure to insert new assignments
    let increment =
        |id: NodeId, loc: Location, assignments: &mut HashMap<NodeId, HashMap<Location, u32>>| {
            if assignments.contains_key(&id) {
                let entry = assignments.get_mut(&id).unwrap();
                if entry.contains_key(&loc) {
                    let count = entry.get_mut(&loc).unwrap();
                    *count += 1;
                } else {
                    entry.insert(loc.clone(), 1);
                }
            } else {
                let mut new_map: HashMap<Location, u32> = HashMap::default();
                new_map.insert(loc.clone(), 1);
                assignments.insert(id, new_map);
            }
        };
    for graphpath in prog.get_stdout_forward_paths().iter() {
        let mut all_assigned = true;
        for id in graphpath.iter() {
            if !assigned.contains_key(&id) {
                all_assigned = false;
                break;
            }
        }
        if all_assigned {
            continue;
        }

        // if path source and sink are in the same location, assign all in between to be in the
        // same location
        assert!(graphpath.len() >= 2);
        let first_node_loc = match assigned.get(&graphpath[0]) {
            Some(loc) => loc,
            None => {
                bail!("In path, first node is not assigned!");
            }
        };
        let last_node_loc = match assigned.get(&graphpath[graphpath.len() - 1]) {
            Some(loc) => loc,
            None => {
                bail!("In path, first node is not assigned!");
            }
        };
        tracing::debug!(
            "first node loc: {:?}
            last node loc: {:?}",
            first_node_loc,
            last_node_loc
        );
        if first_node_loc == last_node_loc {
            for id in graphpath.iter() {
                tracing::debug!("Setting {:?} to location {:?}", id, first_node_loc.clone());
                increment(*id, first_node_loc.clone(), &mut possible_assignments);
            }
            continue;
        } else {
            tracing::debug!("doing weight thing");
        }
        // otherwise, find the edge with the min assigned 'weight' based on if the node reduces
        // input or not
        // first: assign 'weights' to the nodes.
        let mut weights: Vec<(usize, f64)> = Vec::new();
        let mut last_id = graphpath[0];
        let mut current_weight: f64 = 1.0;
        for (ind, id) in graphpath.iter().enumerate() {
            if ind == 0 {
                continue;
            }

            // figure out if the previous
            // node reduces input or not
            let last_node = prog.get_node(last_id).unwrap();
            let reduces_input = match last_node.get_elem() {
                Elem::Cmd(_cmdnode) => match_map.get(&last_id).unwrap().get_reduces_input(),
                Elem::Read(_readnode) => false,
                Elem::Write(_writenode) => {
                    // writenode is always a sink, never left side of an edge
                    unreachable!();
                }
            };

            if reduces_input {
                current_weight = current_weight / 2 as f64;
            }

            // insert the weight of the *previous edge*;
            weights.push((ind - 1, current_weight));

            last_id = *id;
        }

        // now, find the min "cut"
        let mut min_weight = f64::INFINITY;
        for (_, weight) in weights.iter() {
            if weight.clone() < min_weight {
                min_weight = *weight;
            }
        }

        let mut min_weight_inds: Vec<usize> = Vec::new();
        for (id, weight) in weights.iter() {
            if *weight == min_weight {
                min_weight_inds.push(*id);
            }
        }

        if min_weight_inds.len() == 1 {
            // assign all the nodes until the min weight id to the source location
            let min_ind = min_weight_inds[0];
            for (ind, node_id) in graphpath.iter().enumerate() {
                if ind <= min_ind && !assigned.contains_key(&node_id) {
                    increment(*node_id, first_node_loc.clone(), &mut possible_assignments);
                } else if ind > min_ind && !assigned.contains_key(&node_id) {
                    increment(*node_id, last_node_loc.clone(), &mut possible_assignments);
                } else {
                }
            }
        } else {
            // choose cut node such that *more* nodes are assigned to the server
            let mut min_ind = min_weight_inds[min_weight_inds.len() - 1];
            if *first_node_loc == Location::Client {
                min_ind = min_weight_inds[0];
            } else {
            }
            for (ind, node_id) in graphpath.iter().enumerate() {
                if ind <= min_ind && !assigned.contains_key(&node_id) {
                    increment(*node_id, first_node_loc.clone(), &mut possible_assignments);
                } else if ind > min_ind && !assigned.contains_key(&node_id) {
                    increment(*node_id, last_node_loc.clone(), &mut possible_assignments);
                } else {
                }
            }
        }
    }

    // iterate through possible assignments, and assign to the server if all agree
    for (id, options) in possible_assignments.iter() {
        match options.len() {
            0 => {
                bail!("There should not be any set with length 0 for possible assignments");
            }
            1 => {
                for (loc, _count) in options.iter() {
                    assigned.insert(*id, loc.clone());
                }
            }
            _ => {
                // disagreement: just assign to the client
                assigned.insert(*id, Location::Client);
            }
        }
    }

    Ok(())
}
impl Scheduler for HeuristicScheduler {
    fn schedule(
        &mut self,
        prog: &Program,
        match_map: &mut HashMap<NodeId, ArgMatch>,
        config: &FileNetwork,
        _filecache: &mut FileCache,
        pwd: &Path,
    ) -> Result<HashMap<NodeId, Location>> {
        let mut assigned: HashMap<NodeId, Location> = HashMap::default();

        // constraints/mandatory assigments
        for (id, node) in prog.get_nodes_iter() {
            match node.get_elem() {
                Elem::Read(readnode) => {
                    let location = config.get_location(readnode.get_input_ref());
                    assigned.insert(*id, location);
                }
                Elem::Write(writenode) => {
                    let location = match writenode.get_output_ref() {
                        DashStream::File(fs) => config.get_location(fs),
                        DashStream::Stdout => Location::Client,
                        DashStream::Stderr => Location::Client,
                        _ => {
                            bail!("During scheduling stage, writenode cannot have TCP, Pipe or Fifo as output");
                        }
                    };
                    assigned.insert(*id, location);
                }
                Elem::Cmd(_cmdnode) => {
                    // if the node depends on the current directory, need to set it to run where
                    // that directory is located
                    let argmatch = match_map.get(id).unwrap();
                    let file_locations: Vec<Location> = argmatch
                        .file_dependencies()
                        .into_iter()
                        .map(|(_t, arg)| {
                            let file_option: Option<FileStream> = arg.into();
                            config.get_location(&file_option.unwrap())
                        })
                        .collect();
                    let mut dependent_locations: HashSet<Location> =
                        HashSet::from_iter(file_locations);
                    if argmatch.get_needs_current_dir() {
                        let pwd_location =
                            config.get_location(&FileStream::new(pwd, Location::Client));
                        dependent_locations.insert(pwd_location);
                    }
                    match dependent_locations.len() {
                        0 => {}
                        1 => {
                            for loc in dependent_locations.drain() {
                                assigned.insert(*id, loc.clone());
                            }
                        }
                        _ => {
                            assigned.insert(*id, Location::Client);
                        }
                    }
                }
            }
        }
        tracing::debug!("Assigned so far: {:?}", assigned);
        optimize_node_schedule(prog, &mut assigned, match_map)?;
        Ok(assigned)
    }
}
