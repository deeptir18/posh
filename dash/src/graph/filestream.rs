use super::Location;
use super::Result;
use failure::bail;
use serde::{Deserialize, Serialize};
use std::fs::{canonicalize, File, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Hash, Eq, Copy)]
pub enum FileMode {
    /// Create the file and write to it.
    CREATE,
    /// Just read permissions.
    READ,
    /// Append to an existing file.
    APPEND,
    /// Regular (unclear what to do so just putting a placeholder here).
    REGULAR,
}

impl Default for FileMode {
    fn default() -> Self {
        FileMode::REGULAR
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Hash, Eq, Default)]
pub struct FileStream {
    /// Where the file lives.
    location: Location,
    /// The path of the file.
    path: PathBuf,
    /// TODO: I think I'm thinking about how to use permissions incorrectly.
    mode: FileMode,
}

impl FileStream {
    pub fn open(&self) -> Result<File> {
        let mut open_options = OpenOptions::new();
        match self.mode {
            FileMode::CREATE => open_options.write(true).create(true).read(true),
            FileMode::READ => open_options.read(true),
            FileMode::APPEND => open_options.write(true).append(true).read(true),
            FileMode::REGULAR => open_options.read(true).write(true).create(true),
        };
        let file = open_options.open(self.path.as_path())?;
        Ok(file)
    }

    pub fn open_with_append(&self) -> Result<File> {
        let mut open_options = OpenOptions::new();
        open_options.write(true).append(true);
        let file = open_options.open(self.path.as_path())?;
        Ok(file)
    }

    pub fn new(path: &Path, loc: Location) -> Self {
        FileStream {
            location: loc,
            path: path.to_path_buf(),
            mode: Default::default(),
        }
    }

    pub fn new_with_mode(path: PathBuf, mode: FileMode, loc: Location) -> Self {
        FileStream {
            location: loc,
            path: path,
            mode: mode,
        }
    }

    pub fn prepend_directory(&mut self, parent_dir: &Path) {
        let mut new_path = parent_dir.to_path_buf();
        new_path.push(self.path.as_path());
        self.path = new_path;
    }

    pub fn set_path(&mut self, path: &Path) {
        self.path = path.to_path_buf();
    }

    pub fn get_path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn get_location(&self) -> Location {
        self.location.clone()
    }

    pub fn set_location(&mut self, loc: Location) {
        self.location = loc;
    }

    pub fn get_mode(&self) -> FileMode {
        self.mode.clone()
    }

    pub fn set_mode(&mut self, mode: FileMode) {
        self.mode = mode;
    }

    pub fn get_dot_label(&self) -> String {
        format!(
            " (file: {:?}\nloc: {:?}\nmode {:?})",
            self.path, self.location, self.mode
        )
    }

    pub fn strip_prefix(&mut self, prefix: &Path) -> Result<()> {
        self.path.strip_prefix(prefix)?;
        Ok(())
    }

    pub fn is_absolute(&self) -> bool {
        self.path.as_path().is_absolute()
    }

    /// Tries to return a string representation of the filepath.
    pub fn get_name(&self) -> Result<String> {
        match self.path.to_path_buf().to_str() {
            Some(s) => Ok(s.to_string()),
            None => bail!("Could not turn filepath {:?} into string", self.path),
        }
    }

    /// Attempts to cannonicalize the filepath.
    /// If the file does not exist, modifies the path to prefix the pwd.
    pub fn dash_cannonicalize(&mut self, pwd: &PathBuf) -> Result<()> {
        match canonicalize(self.path.as_path()) {
            Ok(full_path) => {
                self.path = full_path;
                return Ok(());
            }
            Err(e) => match e.kind() {
                ErrorKind::NotFound => {}
                _ => bail!("{:?}", e),
            },
        }

        let new_relative_path = pwd.clone().as_path().join(self.path.clone());
        self.path = new_relative_path;
        Ok(())
    }
}
