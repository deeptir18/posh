extern crate dash;
use dash::graph::{stream, Location};
use dash::util::Result;
use failure::bail;
use glob::glob;
use nom::types::CompleteByteSlice;
use nom::*;
use std::collections::HashMap;
use std::env;
use std::fs::canonicalize;
use std::fs::File;
use std::io::{prelude::*, BufReader};
use std::path::Path;
use std::path::PathBuf;
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

/// Attempts to cannonicalize a filepath.
/// If the file does not exist, need to prepend the current directory to this path and then try
/// again to cannonicalize.
/// TODO: If that STILL doesn't work you have to do some more work.
fn dash_cannonicalize(filename: &str, pwd: &PathBuf) -> Result<PathBuf> {
    let path = Path::new(filename);
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }

    // Then try to cannonicalize the file:
    match canonicalize(filename) {
        Ok(pathbuf) => {
            return Ok(pathbuf);
        }
        Err(_) => {}
    }

    // prepend the pwd and then try to cannonicalize
    // if there are "." or ".." in the relative paths this might not work
    // Which is no good especially for the git case right?
    // We can explicitly address this as a concern in the implementation.

    let new_relative_path = pwd.clone().as_path().join(path);
    return Ok(new_relative_path);

    // Iterate through the pathbuf,
    // Pop off each component,
    //
}

// Run FS Metadata to see if is_dir or is_file => and then cannonicalize
// If it can't be cannonicalized:
//  Mkdir -p the thing
//  Then run FS Metadata
//  Then run "pop" to get the TOP level dir and remove that -> it should be safe to remove this.
fn in_mount(filename: &str, mount: &str, pwd: &PathBuf) -> bool {
    // attempt the cannonicaize the path
    match dash_cannonicalize(filename, pwd) {
        Ok(pathbuf) => {
            return pathbuf.as_path().starts_with(Path::new(mount));
        }
        Err(_) => {
            return false;
        }
    }
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
    pub fn find_match(&self, filename: &str, pwd: &PathBuf) -> Option<(String, String)> {
        for (mount, ip) in self.map.iter() {
            if in_mount(&filename, &mount, pwd) {
                return Some((mount.clone(), ip.clone()));
            }
        }
        None
    }

    /// Used to resolve any filestream arguments that might contain a pattern the resulting list of
    /// multiple files.
    /// TODO: This *only* resolves the pattern with local or absolute paths -- so to do this
    /// correctly, the pwd must be set to the correct thing.
    /// TODO: In addition, this won't handle if the pattern includes an environment variable is
    /// included. Or maybe it will? Who knows?
    /// TODO: This function assumes that the client sees a filesystem view of the underlying files
    ///  -- but if later, Dash wanted to work with a *ssh* backend, this would need to change.
    pub fn resolve_filestream_with_pattern(
        &self,
        filestream: &mut FileStream,
    ) -> Result<Vec<FileStream>> {
        let mut res: Vec<FileStream> = Vec::new();
        for entry in glob(&filestream.get_name()).expect("Failed to read glob pattern") {
            match entry {
                Ok(path) => {
                    let name = match path.to_str() {
                        Some(n) => n.to_string(),
                        None => bail!("Could not turn path: {:?} to string", path),
                    };
                    res.push(FileStream::new_exact(
                        name,
                        filestream.get_location(),
                        filestream.get_mode(),
                    ));
                }
                Err(e) => {
                    bail!("One of the paths is an error: {:?}", e);
                }
            }
        }
        Ok(res)
    }

    /// Modifies the filestream to be remote if necessary.
    pub fn resolve_filestream(&self, filestream: &mut FileStream, pwd: &PathBuf) -> Result<()> {
        // first, see if there's an environment variable
        // Note: won't work on filestreams with patterns
        if filestream.get_name().starts_with("$") {
            let var_name = filestream.get_name().split_at(1).1.to_string();
            match env::var(var_name) {
                Ok(val) => {
                    filestream.set_name(&val);
                }
                Err(e) => {
                    bail!(
                        "Couldn't find environment variable {:?}: {:?}",
                        filestream.get_name(),
                        e
                    );
                }
            }
        }
        match filestream.get_location() {
            Location::Client => match self.find_match(&filestream.get_name(), pwd) {
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
