#![feature(test, conservative_impl_trait)]

extern crate futures;
extern crate tokio;
#[macro_use]
extern crate tokio_io;
extern crate test;

use std::time::Duration;

use tokio::net::TcpStream;

fn unlinger(s: &TcpStream) {
    s.set_linger(Some(Duration::from_secs(0))).unwrap();
}

mod connect_churn {
    use std::io;
    use std::thread;
    use std::sync::mpsc;
    use std::sync::{Arc, Barrier};

    use futures::prelude::*;
    use futures::stream;
    use futures::sync::oneshot;
    use test::Bencher;
    use tokio::net::{TcpListener, TcpStream};
    use tokio_io::io::read_to_end;

    use super::unlinger;

    const NUM: usize = 300;
    const CONCURRENT: usize = 8;

    fn n_workers(n: usize, b: &mut Bencher) {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let listener = TcpListener::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let listener_thread = thread::spawn(move || {
            listener.incoming()
                .map_err(|e| panic!("server err: {:?}", e))
                .for_each(|(s, _)| {
                    unlinger(&s);
                    Ok(())
                })
                .select(shutdown_rx)
                .wait()
                .ok()
                .unwrap()
        });

        let barrier = Arc::new(Barrier::new(n + 1));
        let workers = (0..n).map(|_| {
            let barrier = barrier.clone();
            let (tx, rx) = mpsc::channel();
            let thread = thread::spawn(move || {
                for () in rx {
                    let connects = stream::iter_ok((0..(NUM / n)).map(|_| {
                        let tcp = TcpStream::connect(&addr);
                        Ok(tcp.and_then(|sock| {
                            unlinger(&sock);
                            read_to_end(sock, vec![])
                        }))
                    }));

                    connects.buffer_unordered(CONCURRENT)
                        .map_err(|e: io::Error| panic!("client err: {:?}", e))
                        .for_each(|_| Ok(()))
                        .wait()
                        .unwrap();

                    barrier.wait();
                }
            });
            (thread, tx)
        }).collect::<Vec<_>>();

        b.iter(|| {
            // signal that everyone should start connecting, then wait for
            // everyone to be done on the barrier.
            for &(_, ref tx) in workers.iter() {
                tx.send(()).unwrap();
            }

            barrier.wait();
        });

        for (thread, tx) in workers {
            drop(tx);
            thread.join().unwrap();
        }

        shutdown_tx.send(()).unwrap();
        listener_thread.join().unwrap();
    }

    #[bench]
    fn one_thread(b: &mut Bencher) {
        n_workers(1, b);
    }

    #[bench]
    fn two_threads(b: &mut Bencher) {
        n_workers(2, b);
    }

    #[bench]
    fn multi_threads(b: &mut Bencher) {
        n_workers(4, b);
    }
}

mod transfer {
    use std::cmp;
    use std::io::{self, Read, Write};
    use std::sync::mpsc;
    use std::sync::{Arc, Barrier};
    use std::thread;

    use futures::prelude::*;
    use test::Bencher;
    use tokio::net::{TcpListener, TcpStream};

    use super::unlinger;

    const MB: usize = 3 * 1024 * 1024;

    struct Drain {
        sock: TcpStream,
        rem: usize,
        chunk: Box<[u8]>,
    }

    impl Future for Drain {
        type Item = ();
        type Error = io::Error;

        fn poll(&mut self) -> Poll<(), io::Error> {
            while self.rem > 0 {
                match try_nb!(self.sock.read(&mut self.chunk)) {
                    0 => panic!("hit eof"),
                    n => self.rem -= n,
                }
            }

            Ok(Async::Ready(()))
        }
    }

    struct Transfer {
        sock: TcpStream,
        rem: usize,
        chunk: &'static [u8],
    }

    impl Future for Transfer {
        type Item = ();
        type Error = io::Error;

        fn poll(&mut self) -> Poll<(), io::Error> {
            while self.rem > 0 {
                let len = cmp::min(self.rem, self.chunk.len());
                let buf = &self.chunk[..len];

                let n = try_nb!(self.sock.write(&buf));
                self.rem -= n;
            }

            Ok(Async::Ready(()))
        }
    }

    static DATA: [u8; 1024] = [0; 1024];

    fn one_thread(b: &mut Bencher, read_size: usize, write_size: usize) {
        let listener = TcpListener::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();
        let sock1 = TcpStream::connect(&addr).wait().unwrap();
        let sock2 = listener.incoming().into_future().wait().ok().unwrap().0.unwrap().0;
        unlinger(&sock1);
        unlinger(&sock2);

        let mut read = Drain {
            sock: sock1,
            rem: MB,
            chunk: vec![0; read_size].into_boxed_slice(),
        };
        let mut write = Transfer {
            sock: sock2,
            rem: MB,
            chunk: &DATA[..write_size],
        };

        b.iter(|| {
            write.rem = MB;
            read.rem = MB;
            (&mut read).join(&mut write).wait().unwrap();
        });
    }

    fn two_threads(b: &mut Bencher, read_size: usize, write_size: usize) {
        let listener = TcpListener::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();
        let sock1 = TcpStream::connect(&addr).wait().unwrap();
        let sock2 = listener.incoming().into_future().wait().ok().unwrap().0.unwrap().0;
        unlinger(&sock1);
        unlinger(&sock2);

        let mut read = Drain {
            sock: sock1,
            rem: MB,
            chunk: vec![0; read_size].into_boxed_slice(),
        };
        let mut write = Transfer {
            sock: sock2,
            rem: MB,
            chunk: &DATA[..write_size],
        };

        let barrier = Arc::new(Barrier::new(3));
        let (tx1, rx1) = mpsc::channel();
        let (tx2, rx2) = mpsc::channel();
        let barrier2 = barrier.clone();
        let t1 = thread::spawn(move || {
            for () in rx1 {
                write.rem = MB;
                (&mut write).wait().unwrap();
                barrier2.wait();
            }
        });
        let barrier2 = barrier.clone();
        let t2 = thread::spawn(move || {
            for () in rx2 {
                read.rem = MB;
                (&mut read).wait().unwrap();
                barrier2.wait();
            }
        });

        b.iter(|| {
            tx1.send(()).unwrap();
            tx2.send(()).unwrap();
            barrier.wait();
        });

        drop((tx1, tx2));
        t1.join().unwrap();
        t2.join().unwrap();
    }

    mod small_chunks {
        use test::Bencher;

        #[bench]
        fn one_thread(b: &mut Bencher) {
            super::one_thread(b, 32, 32);
        }

        #[bench]
        fn two_threads(b: &mut Bencher) {
            super::two_threads(b, 32, 32);
        }
    }

    mod big_chunks {
        use test::Bencher;

        #[bench]
        fn one_thread(b: &mut Bencher) {
            super::one_thread(b, 1_024, 1_024);
        }

        #[bench]
        fn two_threads(b: &mut Bencher) {
            super::two_threads(b, 1_024, 1_024);
        }
    }
}
