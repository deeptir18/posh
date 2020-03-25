use super::annotations2::argument_matcher::ArgMatch;
use dash::graph::program::{NodeId, Program};
use dash::graph::Location;
use dash::util::Result;
use std::collections::HashMap;
pub trait Scheduler {
    fn schedule(
        &mut self,
        _prog: &Program,
        _match_map: &mut HashMap<NodeId, ArgMatch>,
    ) -> Result<HashMap<NodeId, Location>>;

    fn cost_metric(
        &mut self,
        _prog: &Program,
        _match_map: &mut HashMap<NodeId, ArgMatch>,
    ) -> Result<()>;
}

pub struct DPScheduler {}

impl Scheduler for DPScheduler {
    fn cost_metric(
        &mut self,
        _prog: &Program,
        _match_map: &mut HashMap<NodeId, ArgMatch>,
    ) -> Result<()> {
        Ok(())
    }
    fn schedule(
        &mut self,
        _prog: &Program,
        _match_map: &mut HashMap<NodeId, ArgMatch>,
    ) -> Result<HashMap<NodeId, Location>> {
        unimplemented!();
    }
}
