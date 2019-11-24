extern crate dash;
use dash::graph::{stream, Location};
use dash::util::Result;
use failure::bail;
use nom::types::CompleteByteSlice;
use nom::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{prelude::*, BufReader};
use std::path::Path;
use std::*;
use stream::FileStream;
/// Map of mount to IP addresses
pub struct FileMap {
    map: HashMap<String, String>,
}
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

// TODO: handle relative filenames?
// To handle relative filenames, try to stat the file and see where the mount is
// If the file doesn't exist, create the file and then stat it, see where the file is,
// and delete it.
// How do you make sure everything you deleted is correct?
fn in_mount(filename: &str, mount: &str) -> bool {
    Path::new(filename).starts_with(Path::new(mount))
}

impl FileMap {
    pub fn construct(map: HashMap<String, String>) -> Self {
        FileMap { map: map }
    }

    pub fn new(mount_info: &str) -> Result<Self> {
        let mut ret: HashMap<String, String> = HashMap::default();
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
        Ok(FileMap { map: ret })
    }

    /// Checks which mount the filename resolves to (if any)
    pub fn find_match(&self, filename: &str) -> Option<(String, String)> {
        for (mount, ip) in self.map.iter() {
            if in_mount(&filename, &mount) {
                return Some((mount.clone(), ip.clone()));
            }
        }
        None
    }

    /// Modifies the filestream to be remote if necessary.
    pub fn modify_stream_to_remote(&self, filestream: &mut FileStream) -> Result<()> {
        match filestream.get_location() {
            Location::Client => match self.find_match(&filestream.get_name()) {
                Some((mount, ip)) => {
                    filestream.set_location(Location::Server(ip));
                    filestream.strip_prefix(&mount)?;
                    Ok(())
                }
                None => Ok(()),
            },
            Location::Server(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // This could be a thing to ask Sadjad about
    #[test]
    fn test_parse_mount() {
        let (_, tup): (CompleteByteSlice, (&str, &str)) =
            parse_file_info(CompleteByteSlice(b"/mod/foo:127.0.0.1")).unwrap();

        assert_eq!(String::from(tup.0), String::from("/mod/foo"));
        assert_eq!(String::from(tup.1), String::from("127.0.0.1"));
    }

    #[test]
    fn test_in_mount_basic() {
        assert_eq!(in_mount("/d/c/b/a", "/d/c"), true);
        assert_eq!(in_mount("/d/c/b/a", "/f/e"), false);
    }

    #[test]
    fn test_find_match_basic() {
        let mut map: HashMap<String, String> = HashMap::default();
        map.insert("/d/c/b/a".to_string(), "127.0.0.1".to_string());
        map.insert("/e/c/b/a".to_string(), "127.0.0.2".to_string());
        let filemap = FileMap { map: map };

        assert_eq!(
            filemap.find_match("/d/c/b/a/0"),
            Some(("/d/c/b/a".to_string(), "127.0.0.1".to_string()))
        );
        assert_eq!(
            filemap.find_match("/e/c/b/a/0"),
            Some(("/e/c/b/a".to_string(), "127.0.0.2".to_string()))
        );
        assert_eq!(filemap.find_match("/f/c/b/a/0"), None);
    }
}
