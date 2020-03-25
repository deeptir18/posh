use dash::graph::filestream::FileStream;
use dash::util::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(PartialEq, Debug, Clone, Eq, Default)]
pub struct FileCache {
    /// Maps relative filepaths to full paths.
    map: HashMap<PathBuf, PathBuf>,
}

impl FileCache {
    pub fn resolve_path(&mut self, filestream: &mut FileStream, pwd: &Path) -> Result<()> {
        if filestream.is_absolute() {
            return Ok(());
        }
        match self.get_cached_value(filestream.get_path()) {
            Some(fullpath) => {
                filestream.set_path(fullpath.as_path());
                Ok(())
            }
            None => {
                // cannonicalize the path here and cache the parent
                let mut original_relative = filestream.get_path();
                original_relative.pop();
                filestream.dash_canonicalize(pwd)?;
                let mut parent = filestream.get_path();
                match parent.pop() {
                    true => {
                        self.map.insert(original_relative, parent);
                    }
                    false => {}
                }
                Ok(())
            }
        }
    }

    fn get_cached_value(&self, mut filepath: PathBuf) -> Option<PathBuf> {
        let original = filepath.clone();
        while !filepath.pop() {
            match self.map.get(&filepath) {
                Some(path) => {
                    // calculate the final filepath
                    let relative = original.strip_prefix(filepath).unwrap();
                    let mut result = Path::new(&path).to_path_buf();
                    result.push(relative);
                    return Some(result);
                }
                None => {}
            }
        }
        None
    }
}
