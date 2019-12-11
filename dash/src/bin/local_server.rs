extern crate dash;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};

fn main() {
    let mut client_map: HashMap<IpAddr, String> = HashMap::default();
    // local loopback
    client_map.insert(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        "/home/deeptir/research/fs_project/client_folders/remote".to_string(),
    );
    let runtime_port = "1234";
    dash::start_runtime(runtime_port, client_map, true);
}
