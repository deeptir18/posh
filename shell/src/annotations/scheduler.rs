use super::interpreter::Interpreter;
use dash::graph::{program, Location};
use dash::util::Result;
use program::{Elem, Node, NodeId, Program};
use std::collections::{HashMap, HashSet};

// More principled scheduling:
// - There are *constraints* and *statistics estimations*
// - Constraints:
//  -> who has read access to the mount?
//  -> who has write access to the mount?

/// TODO: trait might not be the best way to experiment with this?
pub trait Scheduler {
    // use the cost estimation to evalute supposed schedules
    fn schedule(&mut self, prog: &mut Program) -> Result<()>;

    // cost estimation
    fn assign_edge_costs(prog: &mut Program) -> Result<()>;

    // evaluate the cost of running a particular node at a particular location?
    fn evaluate_cost(node_id: NodeId, location: Location) -> Result<u32>;

    // but maybe later there's a more general
}

// also should have an `abstract` version of the graph used for the scheduling purpose that you can
// 'evaluate the plan' on
// the "statistics" and the "evaluation" should be different from the DP formulation

pub struct DPScheduler {}
