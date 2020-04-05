use super::filesize::{FileSize, QueryFileSize};
use dash::graph::filestream::FileStream;
use dash::util::Result;
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
pub struct FileCache {
    /// Maps relative filepaths to full paths.
    path_map: HashMap<PathBuf, PathBuf>,
    /// Stores mapping between paths and file sizes, for scheduling.
    size_map: HashMap<PathBuf, u64>,
    /// module to find directory sizes
    file_size_module: Box<dyn FileSize>,
}

impl fmt::Debug for FileCache {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad(&format!("path_map: {:?}\n", self.path_map))?;
        f.pad(&format!("size_map: {:?}\n", self.size_map))?;
        // TODO: display which type of filesize module it is
        Ok(())
    }
}

impl Default for FileCache {
    fn default() -> FileCache {
        FileCache {
            path_map: Default::default(),
            size_map: Default::default(),
            file_size_module: Box::new(QueryFileSize {}),
        }
    }
}

impl FileCache {
    pub fn new(file_size_module: Box<dyn FileSize>) -> Self {
        FileCache {
            file_size_module: file_size_module,
            ..Default::default()
        }
    }
    /// Used to resolve the size of a path.
    pub fn get_size(&mut self, path: PathBuf) -> Result<f64> {
        match self.size_map.get(&path) {
            Some(size) => {
                return Ok(*size as f64);
            }
            None => {}
        }

        // query for the size
        let size = match self.file_size_module.is_dir(path.as_path()) {
            true => self.file_size_module.dir_size(path.as_path())?,
            false => self.file_size_module.file_size(path.as_path())?,
        };
        self.size_map.insert(path, size);
        Ok(size as f64)
    }
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
                        self.path_map.insert(original_relative, parent);
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
            match self.path_map.get(&filepath) {
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
