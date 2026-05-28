// Minimal reproducer: hyper 1.10.0 busy-loops (100% CPU) on an HTTP/1 *client*
// connection when the peer half-closes (sends FIN) without sending a response while
// the client still has an open streaming request body.
//
// This uses a real OS TCP socket (tokio::net) — no special/simulated transport.
// Regression vs 1.9.0 (set `hyper = "=1.9.0"` in Cargo.toml and re-run: it parks).
//
// Server: accept, read the request head, then half-close its write side (FIN) WITHOUT
// responding, while still reading so the client->server direction stays open.
// Client: HTTP/1 POST with an open streaming (chunked) request body.
//
// A `CountingIo` wrapper counts client-side transport poll calls. Spinning =>
// `poll_flush` in the tens/hundreds of millions; parked => a handful.

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use hyper::body::{Body, Frame};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};

static FLUSHES: AtomicU64 = AtomicU64::new(0);
static WRITES: AtomicU64 = AtomicU64::new(0);
static READS: AtomicU64 = AtomicU64::new(0);

struct CountingIo<T>(T);
impl<T: AsyncRead + Unpin> AsyncRead for CountingIo<T> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>)
        -> Poll<std::io::Result<()>> {
        READS.fetch_add(1, Ordering::Relaxed);
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}
impl<T: AsyncWrite + Unpin> AsyncWrite for CountingIo<T> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8])
        -> Poll<std::io::Result<usize>> {
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

// Streaming request body that never yields a frame and is never closed, so the
// dispatcher's `body_rx` stays `Some` for the life of the connection.
struct OpenBody;
impl Body for OpenBody {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(self: Pin<&mut Self>, _cx: &mut Context<'_>)
        -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Poll::Pending
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = sock.read(&mut buf).await;  // read the request head
        sock.shutdown().await.unwrap();     // FIN on server->client, no response
        loop {
            match sock.read(&mut buf).await { Ok(0) | Err(_) => break, Ok(_) => {} }
        }
    });

    let stream = TcpStream::connect(addr).await.unwrap();
    stream.set_nodelay(true).unwrap();
    let io = hyper_util::rt::TokioIo::new(CountingIo(stream));
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(async move { let _ = conn.await; });

    let req = http::Request::builder()
        .method("POST")
        .uri("/")
        .header("transfer-encoding", "chunked")
        .body(OpenBody)
        .unwrap();
    let resp_fut = sender.send_request(req);
    tokio::spawn(async move { let _ = resp_fut.await; });

    let f0 = FLUSHES.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let f = FLUSHES.load(Ordering::Relaxed) - f0;
    println!(
        "poll_flush in 2s: {f}  poll_write={}  poll_read={}",
        WRITES.load(Ordering::Relaxed),
        READS.load(Ordering::Relaxed)
    );
    println!(
        "VERDICT: {}",
        if f > 100_000 { "BUSY-LOOP (spinning at 100% CPU)" } else { "parked (ok)" }
    );
    std::process::exit(0);
}
