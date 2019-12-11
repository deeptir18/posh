extern crate dash;
use dash::util::Result;
use failure::bail;
use nom::types::CompleteByteSlice;
use nom::*;
use std::str;

named_complete!(
    parse_export<Result<(String, String)>>,
    map!(
        do_parse!(
            tag!("export ")
                >> var_name: take_until!("=")
                >> tag!("=")
                >> value: rest
                >> (var_name, value)
        ),
        |(var_name, value): (CompleteByteSlice, CompleteByteSlice)| {
            let var = str::from_utf8(&var_name.0)?;
            let val = str::from_utf8(&value.0)?;
            Ok((var.to_string(), val.to_string()))
        }
    )
);

pub fn parse_export_command(cmd: &str) -> Result<(String, String)> {
    match parse_export(CompleteByteSlice(cmd.as_bytes())) {
        Ok(a) => a.1,
        Err(e) => bail!("Nom failed to parse export cmd: {:?} -> {:}", cmd, e),
    }
}
