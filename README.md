# Unio.d

Unio is a work-in-progress library that provides a simple, but powerful abstraction over asynchronous IO primitives. It serves as a building block for crafting custom event loops based on the Proactor pattern.

The name "Unio" is an abbreviation for "Unified IO" which reflects the main purpose of the library.

The project was started as an attempt to bring clearness, simplicity, flexibility, and ease of use to the landscape of D's asynchronous libraries: instead of providing yet another all-in-one event loop implementation, it focuses just on the IO. This approach somewhat resembles the "Do one thing and do it well" principle of the Unix philosophy.

## Goals

1. `@safe` (when possible), `@nogc`
2. Fast compilation time
3. No external dependencies
4. Easy, but flexible configuration
5. Asynchronous file IO (via the multi-threaded work queue, when needed)
6. Unified abstraction over `poll`, `epoll`, `io_uring`, `kqueue`, IOCP/OverlappedIO

## Non-goals

1. Directory watchers, interrupts, signal handling, and other OS-specific features (no bloat!)
2. Thread pools
3. Green threads, coroutines, fibers and other program execution models
4. High level networking and protocols

## Planned Features

1. Timers and timeouts
2. Buffer pools (similar to io_uring)
3. Worker queue for parallelizing the file IO (epoll, kqueue)
4. Multi-threaded mode with shared backend (similar to io_uring)

## To Do

- [X] epoll
- [ ] poll
- [ ] kqueue
- [ ] IOCP / Overlapped IO
- [ ] io_uring
- [ ] Multi-threaded IO work queue

## Usage

A simple "Hello World" program that writes some text to the standard output:

```d
import std;
import unio;

void main()
{
    auto io = new EpollEngine();

    with (io) {
        submit(Write(stdout, "Hello "));
        submit(Write(stdout, "World!\n"));

        wait();
    }

    // Read the completion queue
    io.each!((ev) => writefln("Result for %d: %d", ev.op, ev.result));
}
```
