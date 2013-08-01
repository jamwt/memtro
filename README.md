Memtro is a sample toy project using nitro.

It implements a very simple key/value in-memory storage
system using protocol buffers for verbs.

Current versions of nitro have it doing 300-400k pipelined
commands per second over loopback using the built-in benchmarker.

You'll need nitro, protocol buffers (2.5.x,) and protobuf-c (0.15).

Use `redo` to build.
