extern crate walkdir;

use dash::util::Result;
use std::path::Path;
use walkdir::WalkDir;
/// Actually query the system for file size related information.
pub struct QueryFileSize;

pub trait FileSize {
    fn file_size(&self, path: &Path) -> Result<u64>;
    fn is_dir(&self, path: &Path) -> bool;
    fn dir_size(&self, path: &Path) -> Result<u64>;
}

impl FileSize for QueryFileSize {
    fn file_size(&self, path: &Path) -> Result<u64> {
        let metadata = path.metadata()?;
        Ok(metadata.len())
    }

    fn is_dir(&self, path: &Path) -> bool {
        path.is_dir()
    }
    fn dir_size(&self, path: &Path) -> Result<u64> {
        let total_size = WalkDir::new(path)
            .min_depth(1)
            .max_depth(10)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.metadata().ok())
            .filter(|metadata| metadata.is_file())
            .fold(0, |acc, m| acc + m.len());
        Ok(total_size)
    }
}
