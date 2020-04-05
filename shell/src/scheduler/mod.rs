use super::config::filecache::FileCache;
use super::config::network::FileNetwork;
use super::{annotations2, config};
use annotations2::argument_matcher::ArgMatch;
use dash::graph::program::{NodeId, Program};
use dash::graph::Location;
use dash::util::Result;
use std::collections::HashMap;
use std::path::Path;
pub trait Scheduler {
    fn schedule(
        &mut self,
        prog: &Program,
        match_map: &mut HashMap<NodeId, ArgMatch>,
        config: &FileNetwork,
        filecache: &mut FileCache,
        pwd: &Path,
    ) -> Result<HashMap<NodeId, Location>>;
}

pub mod dp;
pub mod heuristic;
