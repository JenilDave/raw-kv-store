# Raw KV Store

A simple socket-based key-value store written in Python. Clients connect via TCP sockets to a server and can perform GET and SET operations on the store.

## Features

- **Socket-based Communication**: Uses TCP sockets for client-server communication
- **Binary Protocol**: Efficient message serialization using MessagePack
- **Thread-safe**: Multiple concurrent client connections supported
- **Simple Operations**: GET and SET operations

## Project Setup

This project uses [UV](https://docs.astral.sh/uv/) for Python dependency management.

### Prerequisites

- Python 3.11+
- UV (Python package manager)

### Installation

```bash
# Sync dependencies
uv sync
```

## Project Structure

```
raw-kv-store/
├── src/kv_store/
│   ├── __init__.py          # Package initialization
│   ├── store.py             # In-memory KV store implementation
│   ├── protocol.py          # Message protocol (MessagePack)
│   ├── server.py            # Socket server implementation
│   └── client.py            # Socket client implementation
├── main.py                  # Entry point
├── pyproject.toml           # UV project configuration
└── README.md                # This file
```

## Usage

### Running the Server

```bash
uv run main.py server --host localhost --port 5000
```

The server will start listening on `localhost:5000` and accept client connections.

### Running the Client (Example)

```bash
uv run src/kv_store/client.py
```

The example client will:
1. Connect to the server
2. SET some key-value pairs
3. GET and display the values

### Client Library Usage

You can import and use the client in your own code:

```python
from src.kv_store.client import KVStoreClient

# Connect and use
with KVStoreClient(host='localhost', port=5000) as client:
    client.set('key1', 'value1')
    result = client.get('key1')
    print(result)  # Output: value1
```

## Protocol

Messages are serialized using MessagePack for efficient binary communication.

### Request Format
- **Operation**: GET or SET
- **Key**: String identifier
- **Value**: Any Python object (for SET operations)

### Response Format
- **Success**: Boolean indicating operation success
- **Data**: Result data (for GET operations)
- **Error**: Error message (if operation failed)

## License

See LICENSE file for details.
Raw KV Store (with Sockets interface)
