# Tiny Redis (Python Implementation)

A lightweight, multi-threaded, RESP-compliant Redis server implementation built from scratch using Python standard libraries (`socket`, `threading`, `datetime`).

## Features

- **Multi-Client Concurrency:** Spawns thread-safe client handlers using Python's `threading` module to handle concurrent client TCP connections without blocking.
- **RESP Protocol Parser:** Full low-level parsing of Redis Serialization Protocol (RESP) arrays and bulk strings.
- **Key-Value Store:** Supports core commands (`SET`, `GET`, `DEL`) with thread-safe `Lock` synchronization.
- **Key Expirations (TTL):** Time-to-live expiration support (`SET key val EX seconds`) with passive key eviction upon retrieval.
- **Pub/Sub Messaging:** Dynamic channel subscription and message broadcasting (`SUBSCRIBE`, `PUBLISH`) with automatic stale socket cleanup on disconnect.

## Handled Commands

| Command | Syntax | Description |
| :--- | :--- | :--- |
| `PING` | `PING` | Returns `+PONG` connection health check |
| `ECHO` | `ECHO message` | Returns bulk string copy of input message |
| `SET` | `SET key value [EX seconds]` | Stores key-value pair with optional TTL |
| `GET` | `GET key` | Retrieves key value or `$-1` if missing/expired |
| `DEL` | `DEL key` | Evicts key from key-value store |
| `SUBSCRIBE` | `SUBSCRIBE channel` | Registers client socket to receive channel messages |
| `PUBLISH` | `PUBLISH channel message` | Broadcasts 3-element RESP payload to subscribers |
| `EXIT` | `EXIT` | Gracefully closes the client session |

## Getting Started

### Prerequisites
- Python 3.10+
- `redis-cli` or any TCP netcat tool for testing

### Running the Server
```bash
python main.py
