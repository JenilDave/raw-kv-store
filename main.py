import sys
import argparse
from src.kv_store.server import KVStoreServer, main as server_main
from src.kv_store.client import main as client_main


def main():
    parser = argparse.ArgumentParser(description="Raw KV Store - Socket-based Key-Value Store")
    parser.add_argument("mode", choices=["server", "client"], help="Run as server or client")
    parser.add_argument("--host", default="localhost", help="Server host (default: localhost)")
    parser.add_argument("--port", type=int, default=5000, help="Server port (default: 5000)")
    parser.add_argument("--storage", default="data/kv_store.jsonl", help="Storage file path (default: data/kv_store.jsonl)")
    
    args = parser.parse_args()
    
    if args.mode == "server":
        server = KVStoreServer(host=args.host, port=args.port, storage_file=args.storage)
        server.start()
    else:
        client_main()


if __name__ == "__main__":
    main()

