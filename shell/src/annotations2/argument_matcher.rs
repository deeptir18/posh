use super::grammar::*;
use clap::ArgMatches;
use dash::graph::command::NodeArg;
use dash::util::Result;
use failure::bail;
use std::collections::HashMap;

/// Specifies a parsed mapping between arguments that appear at runtime.
pub struct ArgMatch {
    map: HashMap<Argument, String>,
}

/// This can be attached to the command nodes? to aid scheduling?
impl ArgMatch {
    /// Constructs an arg match object from the clap arg matches object.
    /// TODO: would be easier if clap just exposed methods on ArgMatches.
    pub fn new(
        matches: &ArgMatches,
        annotation: &Command,
        annotation_map: HashMap<String, usize>,
    ) -> Self {
        unimplemented!();
    }

    pub fn new_default(&self, e) -> Self {
        unimplemented!();
    }

    /// Reconstructs the arguments into a string that can be used at runtime.
    pub fn reconstruct(&self) -> Result<Vec<String>> {
        unimplemented!();
    }

    /// Splits into multiple matches by the given argument.
    /// Technically might need to be more complicated if there's a splitting factor.
    pub fn split(&self, arg: &Argument, splitting_factor: u8) -> Result<Vec<ArgMatch>> {
        unimplemented!();
    }

    /// Returns vector of all the file related dependencies this node sees.
    pub fn file_dependencies(&self) -> Vec<(ArgType, NodeArg)> {
        unimplemented!();
    }

    /// TODO: does this need to be there?
    /// Need to resolve glob and the environment variables as well
    pub fn glob_resolution(&mut self) -> Result<()> {}
}
