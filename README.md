# Raw KV Store

A simple socket-based key-value store written in Python. Clients connect via TCP sockets to a server and can perform GET and SET operations on the store.

## Features

- **Socket-based Communication**: Uses TCP sockets for client-server communication
- **Binary Protocol**: Efficient message serialization using MessagePack
- **Thread-safe**: Multiple concurrent client connections supported
- **Simple Operations**: GET and SET operations
- **Comprehensive Logging**: Centralized logging with file rotation and console output

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
│   ├── client.py            # Socket client implementation
│   └── logging_util.py      # Centralized logging utility
├── logs/                    # Log files (created automatically)
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
uv run main.py client
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

## Logging

The application uses a centralized logging utility that captures detailed server operations.

### Server-Only Logging

Only the server generates logs. Client operations are not logged to ensure minimal overhead and to keep logging focused on server-side events.

### Log Details

Each log entry includes:
- **Timestamp**: YYYY-MM-DD HH:MM:SS format
- **Filename**: Name of the source file and line number
- **Function Name**: Name of the function where the log originated
- **Log Level**: INFO, WARNING, ERROR, etc.
- **Message**: Detailed log message

### Log Format

```
2026-03-08 18:45:11 | server.py:39 | start() | INFO | KV Store server listening on localhost:5000
2026-03-08 18:45:26 | server.py:44 | start() | INFO | Client connected from ('127.0.0.1', 52844)
```

### Log Files

Log files are automatically created in the `logs/` directory with a timestamp:
- `server_YYYY-MM-DD_HH-MM-SS.log` - Server operations and client connections

Each time the server starts, a new timestamped log file is created to maintain separate logs for different server instances.

### Log Rotation

Log files use automatic rotation:
- Maximum file size: 10 MB
- Backup files kept: 5 previous versions

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
