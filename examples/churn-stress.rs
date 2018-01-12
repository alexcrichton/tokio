extern crate getopts;
extern crate tokio;
extern crate futures_timer;
extern crate futures;
#[macro_use]
extern crate tokio_io;
extern crate futures_cpupool;

use std::env;
use std::net::SocketAddr;
use std::time::Duration;
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT};
use std::sync::atomic::Ordering::SeqCst;
use std::thread;
use std::io::{self, Write, Read};

use futures::executor::CurrentThread;
use futures::prelude::*;
use futures::stream;
use futures_cpupool::CpuPool;
use futures_timer::{Sleep, Interval};
use getopts::Options;
use tokio::net::{TcpListener, TcpStream};

struct Config {
    addr: SocketAddr,
    print_interval: Duration,
    io_delay: Duration,
    concurrency: usize,
    num_connections: usize,
}

static CLIENTS: AtomicUsize = ATOMIC_USIZE_INIT;
static ERRORS: AtomicUsize = ATOMIC_USIZE_INIT;
static BYTES_READ: AtomicUsize = ATOMIC_USIZE_INIT;
static BYTES_WRITTEN: AtomicUsize = ATOMIC_USIZE_INIT;

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optflag("h", "help", "print this help menu");
    opts.optflag("", "server", "run the server");
    opts.reqopt("a", "addr", "address to bind/connect to", "ADDR");
    opts.optopt("i", "interval", "interval to print stats on (ms)", "MS");
    opts.optopt("d", "delay", "delay after connecting/accepting to do I/O (ms)", "MS");
    opts.optopt("c", "concurrency", "number of connections to send (client)", "CONNS");
    opts.optopt("n", "num-connections", "total number of connections to send (client)", "TOTAL");
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(f) => panic!(f.to_string()),
    };
    if matches.opt_present("h") {
        return print_usage(&program, opts)
    }
    let addr = matches.opt_str("a").expect("must pass `-a` argument");
	let addr = addr.parse().expect("failed to parse `-a` as socket address");

    let print_interval = matches.opt_str("i")
        .map(|s| s.parse().expect("failed to parse `-i` as a number"))
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(1));

    let io_delay = matches.opt_str("d")
        .map(|s| s.parse().expect("failed to parse `-d` as a number"))
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_millis(200));

    let concurrency = matches.opt_str("c")
        .map(|s| s.parse().expect("failed to parse `-c` as a number"))
        .unwrap_or(1000);

    let num_connections = matches.opt_str("n")
        .map(|s| s.parse().expect("failed to parse `-n` as a number"))
        .unwrap_or(10_000);

    let config = Config {
        addr,
        print_interval,
        io_delay,
        concurrency,
        num_connections,
    };

    let print = config.print_interval;
    thread::spawn(move || {
        let interval = Interval::new(print);
        let interval = stream::blocking(interval);
        let mut old_reads = 0;
        let mut old_writes = 0;
        for (i, _) in interval.enumerate() {
            let reads = BYTES_READ.load(SeqCst);
            let writes = BYTES_WRITTEN.load(SeqCst);
            print!("\
                {:<3} \
                clients: {:<6} \
                errors: {:<4} \
                reads: {:<6} \
                writes: {:<6} \
                reads/i: {:<4} \
                writes/i: {:<4} \
                \r\
            ",
               i,
               CLIENTS.load(SeqCst),
               ERRORS.load(SeqCst),
               reads,
               writes,
               reads - old_reads,
               writes - old_writes,
            );
            old_reads = reads;
            old_writes = writes;
            io::stdout().flush().unwrap();
        }
    });

	if matches.opt_present("server") {
        server(&config);
    } else {
        client(&config);
    }
}

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} [options]", program);
    print!("{}", opts.usage(&brief));
}

fn server(config: &Config) {
    use futures::sync::mpsc;

    let listener = TcpListener::bind(&config.addr).expect("failed to bind listener");

    println!("listening on {}", listener.local_addr().unwrap());

    let (tx, rx) = mpsc::unbounded();

    let delay = config.io_delay;
    let srv = listener.incoming()
        .map(|(s, _)| {
            CLIENTS.fetch_add(1, SeqCst);
            unlinger(&s);
            s
        })
        .for_each(move |socket| {
            tx.unbounded_send(socket).unwrap();
            Ok(())
        });

    let t = thread::spawn(move || {
        CurrentThread::run(|_| {
            CurrentThread::execute(rx.for_each(move |socket| {
                let client = ReadByte(Some(socket))
                    .and_then(move |(socket, byte)| {
                        BYTES_READ.fetch_add(1, SeqCst);
                        Sleep::new(delay)
                            .and_then(move |_| WriteByte(Some(socket), byte))
                            .map(|_| {
                                BYTES_WRITTEN.fetch_add(1, SeqCst);
                            })
                    });
                CurrentThread::execute(client.then(|res| {
                    CLIENTS.fetch_sub(1, SeqCst);
                    if res.is_err() {
                        ERRORS.fetch_add(1, SeqCst);
                    }
                    Ok::<(), ()>(())
                }));
                Ok(())
            }))
        })
    });

    CurrentThread::run(|_| {
        CurrentThread::execute(srv.map_err(|e| {
            panic!("listener error: {}", e);
        }));
    });

    t.join().unwrap();
}

fn client(config: &Config) {
    let addr = config.addr;
    let delay = config.io_delay;
    let clients = stream::iter_ok(0..config.num_connections)
        .map(move |_| {
            TcpStream::connect(&addr)
                .map(|s| {
                    CLIENTS.fetch_add(1, SeqCst);
                    unlinger(&s);
                    s
                })
                .and_then(move |socket| Sleep::new(delay).map(|_| socket))
                .and_then(|s| WriteByte(Some(s), 1))
                .and_then(|s| {
                    BYTES_WRITTEN.fetch_add(1, SeqCst);
                    ReadByte(Some(s))
                })
                .map(|_| {
                    BYTES_READ.fetch_add(1, SeqCst);
                })
        })
        .buffer_unordered(config.concurrency);

    CurrentThread::run(|_| {
        CurrentThread::execute(clients.then(|res| {
            CLIENTS.fetch_sub(1, SeqCst);
            if res.is_err() {
                ERRORS.fetch_add(1, SeqCst);
            }
            Ok(())
        }).for_each(|_| Ok(())));
    });
}

struct ReadByte(Option<TcpStream>);

impl Future for ReadByte {
    type Item = (TcpStream, u8);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let mut buf = [0];
        let n = try_nb!(self.0.as_mut().unwrap().read(&mut buf));
        if n == 0 {
            Err(io::Error::new(io::ErrorKind::Other, "unexpected eof"))
        } else {
            Ok(Async::Ready((self.0.take().unwrap(), buf[0])))
        }
    }
}

struct WriteByte(Option<TcpStream>, u8);

impl Future for WriteByte {
    type Item = TcpStream;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let buf = [self.1];
        let n = try_nb!(self.0.as_mut().unwrap().write(&buf));
        if n == 0 {
            Err(io::Error::new(io::ErrorKind::Other, "unexpected eof"))
        } else {
            Ok(Async::Ready(self.0.take().unwrap()))
        }
    }
}

fn unlinger(s: &TcpStream) {
    s.set_linger(Some(Duration::from_secs(0))).unwrap();
}
