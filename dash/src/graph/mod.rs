use super::util::Result;
pub mod cmd;
pub mod program;
pub mod rapper;
pub mod read;
pub mod stream;
pub mod write;

use failure::bail;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::mem::drop;
use std::sync::{Arc, Mutex};

// TODO: the current design allows nodes to have multiple stdout or stderr handles,
// but the way the code is written, it would panic if any node ever had more than 1 stderr or
// stdout handle, because the first copy would only copy into the first output stream.

/// Represents where a computation should take place, or where a stream leads to.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Hash, Eq)]
pub enum Location {
    /// Client/orchestration machine
    Client,
    /// Address of this particular server
    Server(String),
}

impl Default for Location {
    fn default() -> Self {
        Location::Client
    }
}

/// Safe shared wrapper around a HashMap.
pub struct SharedMap<K, V>(pub Arc<Mutex<HashMap<K, V>>>);

impl<K: PartialEq + Debug + Clone + Default + Hash + Eq, V> SharedMap<K, V> {
    pub fn new() -> SharedMap<K, V> {
        let map: HashMap<K, V> = HashMap::default();
        SharedMap(Arc::new(Mutex::new(map)))
    }

    /// Returns a new reference to the underlying map
    pub fn clone(&self) -> SharedMap<K, V> {
        SharedMap(self.0.clone())
    }

    /// Removes the key from the map if it exists
    pub fn remove(&mut self, key: &K) -> Result<V> {
        let mut map = match self.0.lock() {
            Ok(m) => m,
            Err(e) => bail!("Lock is poisoned: {:?}", e),
        };
        let v = match map.remove(key) {
            Some(v) => v,
            None => bail!("Could not find key: {:?} in shared map", key),
        };
        // TODO: is explicit drop here necessary
        drop(map);
        Ok(v)
    }

    /// Inserts the key if the key does not exist in the map.
    pub fn insert(&mut self, key: K, value: V) -> Result<()> {
        let mut map = match self.0.lock() {
            Ok(m) => m,
            Err(e) => bail!("Lock is poisoned: {:?}", e),
        };

        match map.insert(key.clone(), value) {
            Some(_old_v) => {
                bail!("Key had a prev value in map: {:?}", key);
            }
            None => {}
        }

        Ok(())
    }
}
