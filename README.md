# hyper 1.10.0 HTTP/1 client busy-loop repro

Minimal, deterministic reproducer for a 100% CPU busy-loop in **hyper 1.10.0**'s
HTTP/1 **client** connection. Uses a **real OS TCP socket** (`tokio::net`) — no
simulated/custom transport.

## Trigger

An HTTP/1 client has an open **streaming request body**, and the peer
**half-closes (FIN) without sending a response** (e.g. a server restart/crash, a
deploy rollout / pod eviction, an upload or idle timeout, or a load balancer dropping
the upstream). The client connection task then spins at 100% CPU indefinitely (until
the application produces the next body frame), doing no I/O — just re-polling `flush`.

Regression vs **hyper 1.9.0** (which parks correctly).

## Run

```sh
cargo run --release
```

Expected output:

- **hyper 1.10.0** (default in `Cargo.toml`):
  ```
  poll_flush in 2s: 140837985  poll_write=1  poll_read=3
  VERDICT: BUSY-LOOP (spinning at 100% CPU)
  ```
- **hyper 1.9.0** (change the pin in `Cargo.toml` to `version = "=1.9.0"`):
  ```
  poll_flush in 2s: 2  poll_write=1  poll_read=2
  VERDICT: parked (ok)
  ```

The transport's `poll_write`/`poll_read` are called only 1–3 times while `poll_flush`
is called hundreds of millions of times: the connection makes no I/O progress, it is
purely spinning in `Dispatcher::poll_loop`.

## Root cause

The only behavioral change between 1.9.0 and 1.10.0 here is the rewrite of
`Dispatcher::poll_loop` in `src/proto/h1/dispatch.rs`. 1.10.0 added a write-side
continuation gate:

```rust
let wants_write_again = self.can_write_again() && (write_ready || flush_ready);
// ...
if !wants_read_again && wants_write_again {
    if write_ready { continue; } // hot path
    // ...
}
```

with `fn can_write_again(&mut self) -> bool { self.body_rx.is_some() }`.

`can_write_again()` is `true` for the entire lifetime of a streaming request body.
After the peer half-closes with no response, the read side is finished
(`wants_read_again() == false`) and `poll_write` returns `Ready` without doing I/O,
so `write_ready == true`, `flush_ready == true` (TCP flush is a no-op that returns
`Ready`), and `body_rx.is_some() == true` ⇒ `wants_write_again == true`. The loop
never parks; `if write_ready { continue; }` is taken every iteration, the
`for _ in 0..16` runs out, and `task::yield_now` reschedules the task immediately ⇒
100% CPU busy-loop.

## Scope

HTTP/1 client only (`proto::h1::dispatch::poll_loop` is h1-only code). The HTTP/2
client body-send path was separately reworked in 1.10.0 but parks correctly under the
analogous trigger (server RST_STREAMs / closes the connection without responding while
the request body is open).

This also reproduces deterministically under
[`turmoil`](https://github.com/tokio-rs/turmoil)'s in-memory TCP; the version here uses
a real socket so the bug is unambiguous.

## Workaround

Pin `hyper = "=1.9.0"`.
