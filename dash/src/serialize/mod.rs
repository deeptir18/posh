use super::util::Result;
use std;
use std::io::prelude::*;
use std::net::TcpStream;
use std::vec::Vec;

pub mod rpc;

use bytes::{ByteOrder, LittleEndian};

pub fn write_size(buf: &mut [u8], num: u128) {
    LittleEndian::write_u128(buf, num);
}

pub fn read_size(buf: &[u8]) -> u128 {
    LittleEndian::read_u128(buf)
}

pub fn write_msg(payload: Vec<u8>, stream: &mut TcpStream) -> Result<()> {
    let mut buf = [0u8; 16];
    write_size(&mut buf, payload.len() as u128);
    stream.write(&buf)?;
    stream.write(&payload)?;
    Ok(())
}

pub fn read_msg(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut buf = [0u8; 16];
    stream.read(&mut buf)?;
    let size = read_size(&buf);
    let vec = read_to_size(stream, size as usize)?;
    Ok(vec)
}

pub fn read_to_size(mut stream: &TcpStream, size: usize) -> Result<Vec<u8>> {
    let mut bytes_read: usize = 0;
    let mut ret: Vec<u8> = Vec::new();
    let mut buf = [0; 4096];
    loop {
        let more_bytes = stream.read(&mut buf)?;
        bytes_read += more_bytes;
        ret.extend_from_slice(&mut buf[0..more_bytes]);

        if bytes_read == size {
            break;
        }
    }
    Ok(ret)
}
