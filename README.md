# hyper 1.10.0 HTTP/1 client busy-loop repro

Minimal, deterministic reproducer for a 100% CPU busy-loop in **hyper 1.10.0**'s
HTTP/1 **client** connection.

## Trigger

An HTTP/1 client has an open **streaming request body**, and the peer
**half-closes (FIN) without sending a response**. The client connection task then
spins at 100% CPU indefinitely (until the application produces the next body frame),
doing no I/O — just re-polling `flush` forever.

Regression vs **hyper 1.9.0** (which parks correctly).

## Run

```sh
cargo run --release
```

Expected output:

- **hyper 1.10.0** (default in `Cargo.toml`):
  ```
  after ~3s: poll_flush=305726705  poll_write=1  poll_read=2
  VERDICT: BUSY-LOOP (spinning at 100% CPU)
  ```
- **hyper 1.9.0** (change the pin in `Cargo.toml` to `version = "=1.9.0"`):
  ```
  sim.run() returned. poll_flush=2  VERDICT: parked (ok)
  ```

Note that the transport's `poll_write`/`poll_read` are called only 1/2 times while
`poll_flush` is called hundreds of millions of times: the connection makes no I/O
progress, it is purely spinning in `Dispatcher::poll_loop`.

## How it works

The transport is [`turmoil`](https://github.com/tokio-rs/turmoil)'s in-memory
simulated TCP, chosen because (like many in-memory transports) its `poll_flush` is
always `Ready` and it surfaces a received FIN as a readable EOF — the conditions that
trigger the bug. A `CountingIo` wrapper counts client-side `poll_flush` calls; a
watchdog thread prints the count after 3s (a spin makes `sim.run()` never return,
which is the original symptom).

## Root cause

Only behavioral change between 1.9.0 and 1.10.0 here is the rewrite of
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
so `write_ready == true`, `flush_ready == true`, `body_rx.is_some() == true` ⇒
`wants_write_again == true`. The loop never parks; `if write_ready { continue; }` is
taken every iteration, the `for _ in 0..16` runs out, and `task::yield_now`
reschedules the task immediately ⇒ 100% CPU busy-loop.

## Workaround

Pin `hyper = "=1.9.0"`.
