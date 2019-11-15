use super::graph::{program, stream, Location};
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

pub fn write_type(buf: &mut [u8], t: rpc::MessageType) {
    LittleEndian::write_u32(buf, t.to_u32());
}

pub fn read_size(buf: &[u8]) -> u128 {
    LittleEndian::read_u128(buf)
}

pub fn read_type(buf: &[u8]) -> rpc::MessageType {
    rpc::MessageType::from_u32(LittleEndian::read_u32(buf))
}

pub fn write_msg(payload: Vec<u8>, stream: &mut TcpStream) -> Result<()> {
    let mut buf = [0u8; 16];
    write_size(&mut buf, payload.len() as u128);
    stream.write(&buf)?;
    stream.write(&payload)?;
    Ok(())
}

pub fn write_msg_and_type(
    payload: Vec<u8>,
    t: rpc::MessageType,
    stream: &mut TcpStream,
) -> Result<()> {
    let mut buf = [0u8; 16];
    write_size(&mut buf, payload.len() as u128);
    let mut type_buf = [0u8; 4];
    write_type(&mut type_buf, t);
    stream.write(&buf)?;
    stream.write(&type_buf)?;
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

pub fn read_msg_and_type(stream: &mut TcpStream) -> Result<(rpc::MessageType, Vec<u8>)> {
    let mut buf = [0u8; 16];
    stream.read(&mut buf)?;
    let size = read_size(&buf);
    let mut type_buf = [0u8; 4];
    stream.read(&mut type_buf)?;
    let msg_type = read_type(&type_buf);
    let vec = read_to_size(stream, size as usize)?;
    Ok((msg_type, vec))
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
