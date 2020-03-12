use dash::util::Result;
use failure::bail;
use dash::graph::program::Program;

pub trait Scheduler {
    fn schedule(&mut self, prog: &mut Program) -> Result<()>;
}

pub struct DPScheduler {};

impl Scheduler for DPScheduler {
    fn cost_metric(&mut self, prog: &Program) -> Result<()> {

    }
    fn schedule(&mut self, prog: &Program) -> Result<()> {
        Ok(())
    }
}
