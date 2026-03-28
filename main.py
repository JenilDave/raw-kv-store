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
    parser.add_argument("--server-mode", choices=["primary", "replica", "standalone"], default="standalone",
                        help="Server mode: primary (accepts writes, syncs to replica), replica (read-only), or standalone (independent)")
    parser.add_argument("--peer-host", help="Peer server host (required if server-mode is 'primary')")
    parser.add_argument("--peer-port", type=int, help="Peer server port (required if server-mode is 'primary')")
    
    args = parser.parse_args()
    
    if args.mode == "server":
        server = KVStoreServer(host=args.host, port=args.port, storage_file=args.storage,
                              mode=args.server_mode, peer_host=args.peer_host, peer_port=args.peer_port)
        server.start()
    else:
        client_main()


if __name__ == "__main__":
    main()

