extern crate dash;
#[allow(dead_code)]
use std::io;
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::thread;

/// Client needs to have multiple *threads* representing the streams of data connecting to it
/// A merge node
fn main() {
    same_node();
    distributed();
}
fn same_node() {
    let first_echo = Command::new("echo")
        .args(vec!["first echo\n"])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let second_echo = Command::new("echo")
        .args(vec!["second echo\n"])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let third_echo = Command::new("echo")
        .args(vec!["third\n"])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    // try to feed into the next command in order (even though the commands are being executed in
    // order)
    let grep = Command::new("grep")
        .args(vec!["echo"])
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    let mut grep_stdin = grep.stdin.unwrap();
    io::copy(&mut first_echo.stdout.unwrap(), &mut grep_stdin).unwrap(); // this does internal buffering
    io::copy(&mut second_echo.stdout.unwrap(), &mut grep_stdin).unwrap(); // this will also do some internal buffering
    io::copy(&mut third_echo.stdout.unwrap(), &mut grep_stdin).unwrap(); // this will also do some internal buffering
}

fn distributed() {
    // single loop on the client with handles to all the outward connections it makes
    // Then try to copy the single TCP connections it has to aggregate into one output

    let server1 = thread::spawn(move || {
        let listener = TcpListener::bind("127.0.0.1:7834").unwrap();
        if let Ok((mut stream, _addr)) = listener.accept() {
            let child = Command::new("echo")
                .args(vec!["server1 hello"])
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let mut stdout = child.stdout.unwrap();
            io::copy(&mut stdout, &mut stream).unwrap();
        }
    });

    let server2 = thread::spawn(move || {
        let listener = TcpListener::bind("127.0.0.1:7835").unwrap();
        if let Ok((mut stream, _addr)) = listener.accept() {
            let child = Command::new("echo")
                .args(vec!["server2 hello"])
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let mut stdout = child.stdout.unwrap();
            io::copy(&mut stdout, &mut stream).unwrap();
        }
    });

    let server1_conn = thread::spawn(move || TcpStream::connect("127.0.0.1:7834").unwrap());

    let server2_conn = thread::spawn(move || TcpStream::connect("127.0.0.1:7835").unwrap());

    let mut stream1 = server1_conn.join().unwrap();
    let mut stream2 = server2_conn.join().unwrap();

    let grep = Command::new("grep")
        .args(vec!["hello"])
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    let mut grep_stdin = grep.stdin.unwrap();

    io::copy(&mut stream1, &mut grep_stdin).unwrap();
    io::copy(&mut stream2, &mut grep_stdin).unwrap();

    server1.join().unwrap();
    server2.join().unwrap();
}
