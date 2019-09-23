extern crate dash;
use dash::util::Result;
use failure::bail;
use nom::types::CompleteByteSlice;
use nom::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, prelude::*, BufReader};
use std::*;
pub type FileMap = HashMap<String, String>;
named_complete!(
    parse_file_info<(&str, &str)>,
    do_parse!(
        folder: map!(take_until!(":"), |n: CompleteByteSlice| {
            str::from_utf8(n.0).unwrap()
        }) >> tag!(":")
            >> ip: map!(rest, |n: CompleteByteSlice| {
                str::from_utf8(n.0).unwrap()
            })
            >> (folder, ip)
    )
);

pub fn parse_mount_file(mount_info: &str) -> Result<FileMap> {
    let mut ret: FileMap = HashMap::default();
    let file = File::open(mount_info)?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line_src = line?;
        let (file, ip) = match parse_file_info(CompleteByteSlice(line_src.as_ref())) {
            Ok(b) => b.1,
            Err(e) => {
                bail!("line {:?} failed with {:?}", line_src, e.to_string());
            }
        };
        ret.insert(file.to_string(), ip.to_string());
    }

    Ok(ret)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_parse_mount() {
        let (_, tup): (CompleteByteSlice, (&str, &str)) =
            parse_file_info(CompleteByteSlice(b"/mod/foo:127.0.0.1")).unwrap();

        assert_eq!(String::from(tup.0), String::from("/mod/foo"));
        assert_eq!(String::from(tup.1), String::from("127.0.0.1"));
    }
}
