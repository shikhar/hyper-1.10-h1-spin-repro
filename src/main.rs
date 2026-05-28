// Minimal reproducer: hyper 1.10.0 busy-loops (100% CPU) on an HTTP/1 *client*
// connection when the peer half-closes (sends FIN) without sending a response while
// the client still has an open streaming request body.
//
// Regression vs 1.9.0 (set `hyper = "=1.9.0"` in Cargo.toml and re-run: it parks).
//
// The transport here is turmoil's in-memory simulated TCP, which (like many in-memory
// transports) has `poll_flush` always Ready and surfaces a received FIN as a readable
// EOF. A `CountingIo` wrapper counts client-side `poll_flush` calls; a watchdog prints
// the count after 3s. Spinning => tens/hundreds of millions; parked => a handful.
// (The spin also makes `sim.run()` never return — the original symptom.)

use std::convert::Infallible;
use std::net::Ipv4Addr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use hyper::body::{Body, Frame};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

static FLUSHES: AtomicU64 = AtomicU64::new(0);
static WRITES: AtomicU64 = AtomicU64::new(0);
static READS: AtomicU64 = AtomicU64::new(0);

struct CountingIo<T>(T);

impl<T: AsyncRead + Unpin> AsyncRead for CountingIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        READS.fetch_add(1, Ordering::Relaxed);
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}
impl<T: AsyncWrite + Unpin> AsyncWrite for CountingIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        WRITES.fetch_add(1, Ordering::Relaxed);
        Pin::new(&mut self.0).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        FLUSHES.fetch_add(1, Ordering::Relaxed);
        Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

// A streaming request body that never produces a frame and is never closed,
// so the dispatcher's `body_rx` stays `Some` for the whole connection.
struct OpenBody;
impl Body for OpenBody {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Poll::Pending
    }
}

fn main() {
    std::thread::spawn(|| {
        std::thread::sleep(std::time::Duration::from_secs(3));
        let f = FLUSHES.load(Ordering::Relaxed);
        println!(
            "after ~3s: poll_flush={f}  poll_write={}  poll_read={}",
            WRITES.load(Ordering::Relaxed),
            READS.load(Ordering::Relaxed),
        );
        println!("VERDICT: {}", verdict(f));
        std::process::exit(0);
    });

    let mut sim = turmoil::Builder::new().build();

    // Server: accept, read the request head, then half-close (FIN) WITHOUT responding,
    // while continuing to read so the client->server direction stays open.
    sim.host("server", || async move {
        let listener = turmoil::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, 80)).await?;
        let (mut stream, _) = listener.accept().await?;
        let mut buf = [0u8; 1024];
        let _ = stream.read(&mut buf).await?;
        stream.shutdown().await?; // FIN, no response
        loop {
            match stream.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
        Ok(())
    });

    // Client: open a POST with a never-ending streaming body and drive the connection.
    sim.client("client", async move {
        let stream = turmoil::net::TcpStream::connect(("server", 80)).await?;
        let io = hyper_util::rt::TokioIo::new(CountingIo(stream));
        let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
        let req = http::Request::builder()
            .method("POST")
            .uri("/")
            .header("transfer-encoding", "chunked")
            .body(OpenBody)?;
        let resp = sender.send_request(req);
        let _ = tokio::join!(async { conn.await.ok(); }, async { resp.await.ok(); });
        Ok(())
    });

    let _ = sim.run();
    let f = FLUSHES.load(Ordering::Relaxed);
    println!("sim.run() returned. poll_flush={f}  VERDICT: {}", verdict(f));
}

fn verdict(flushes: u64) -> &'static str {
    if flushes > 100_000 {
        "BUSY-LOOP (spinning at 100% CPU)"
    } else {
        "parked (ok)"
    }
}
