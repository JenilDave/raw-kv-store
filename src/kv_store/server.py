"""Socket-based KV store server."""

import socket
import threading
import traceback
from typing import Optional

from src.kv_store.store import KVStore
from src.kv_store.protocol import Message, Response
from src.kv_store.logging_util import setup_logger, get_timestamped_logfile


logger = None  # Will be initialized when server starts


class KVStoreServer:
    """TCP socket-based key-value store server."""
    
    def __init__(self, host: str = "localhost", port: int = 5000, storage_file: str = "data/kv_store.jsonl",
                 mode: str = "standalone", peer_host: str = None, peer_port: int = None):
        self.host = host
        self.port = port
        self.store = KVStore(storage_file=storage_file)
        self.store._load_from_file()
        self.log_sequence_number = self.store.get_latest_log_sequence_number()  # Relevant for Primary only. Replica will sync this from Primary.
        self.socket: Optional[socket.socket] = None
        self.running = False
        self.mode = mode  # "primary", "replica", or "standalone"
        self.peer_host = peer_host
        self.peer_port = peer_port
    
    def start(self) -> None:
        """Start the server and listen for connections."""
        global logger
        
        # Initialize logger with timestamped filename when server starts
        logger = setup_logger(__name__, log_filename=get_timestamped_logfile("server"))
        
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.socket.bind((self.host, self.port))
            self.socket.listen(5)
            self.socket.settimeout(1.0)  # Allow Ctrl+C to interrupt accept()
            self.running = True
            logger.info(f"KV Store server listening on {self.host}:{self.port}")
            logger.info(f"Mode: {self.mode}")
            if self.mode == "primary" and self.peer_host:
                logger.info(f"Replica server: {self.peer_host}:{self.peer_port}")
            
            self._sync_log_sequence_with_replica()
            self._request_primary_to_sync(Message(operation="sync_request", key="", internal=True))
            while self.running:
                try:
                    client_socket, client_addr = self.socket.accept()
                    logger.info(f"Client connected from {client_addr}")
                    
                    # Handle each client in a separate thread
                    client_thread = threading.Thread(
                        target=self._handle_client,
                        args=(client_socket, client_addr),
                        daemon=True
                    )
                    client_thread.start()
                    
                except socket.timeout:
                    # Timeout is normal, just continue the loop
                    continue
                except KeyboardInterrupt:
                    logger.info("Server shutdown requested")
                    break
                    
        except Exception as e:
            logger.error(f"Server error: {e}")
            logger.error(traceback.format_exc())
        finally:
            self.stop()

    def recv_exactly(self, sock, n):
        data = b''
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:
                raise ConnectionError("Connection closed")
            data += chunk
        return data
    
    def _handle_client(self, client_socket: socket.socket, client_addr: tuple) -> None:
        """Handle a single client connection."""
        try:
            while self.running:
                # Receive message length first (4 bytes)
                length_bytes = self.recv_exactly(client_socket, 4)
                if not length_bytes:
                    break
                
                message_length = int.from_bytes(length_bytes, byteorder='big')
                
                # Receive the actual message
                message_data = b''
                while len(message_data) < message_length:
                    chunk = self.recv_exactly(client_socket, min(4096, message_length - len(message_data)))
                    if not chunk:
                        break
                    message_data += chunk
                
                if not message_data:
                    break
                
                # Parse and process the message
                try:
                    message = Message.from_bytes(message_data)
                    # Force internal flag to False - only server can set it to True
                    response = self._process_message(message)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    response = Response(success=False, error=str(e))
                
                # Send response
                response_bytes = response.to_bytes()
                response_length = len(response_bytes).to_bytes(4, byteorder='big')
                client_socket.send(response_length + response_bytes)
                
        except Exception as e:
            logger.error(f"Client handler error: {e}")
        finally:
            client_socket.close()
            logger.info(f"Client disconnected: {client_addr}")
    
    def _process_message(self, message: Message) -> Response:
        """Process a message and return response."""
        operation = message.operation.lower()
        
        if operation == "get":
            # GET is always local, no replication needed
            value = self.store.get(message.key)
            if value is not None:
                return Response(success=True, data=value)
            else:
                return Response(success=False, error=f"Key '{message.key}' not found")
        
        elif operation == "set":
            # Reject direct writes on replica servers (unless it's an internal replication message)
            if self.mode == "replica" and not message.internal:
                return Response(success=False, error="Replica server rejects direct writes. Write to primary server instead.")
            
            if message.value is None:
                return Response(success=False, error="Value is required for SET operation")

            # Process locally first (idempotent with request_id)
            result = self.store.set(message.key, message.value, request_id=message.request_id, log_sequence_number=message.log_sequence_number)
            status = "SET (duplicate request)" if result['is_duplicate'] else "SET"
            message.log_sequence_number = result.get('log_sequence_number', self.log_sequence_number)
            
            # Then sync to replica if primary
            if self.mode == "primary" and self.peer_host:
                replica_response = self._sync_to_replica(message)
                if not replica_response or not replica_response.success:
                    error_msg = replica_response.error if replica_response else "Replica sync failed"
                    return Response(success=False, error=f"Replica rejected SET: {error_msg}")
            
            return Response(success=True, data=f"{status} '{message.key}' = {message.value}")
        
        elif operation == "delete":
            # Reject direct writes on replica servers (unless it's an internal replication message)
            if self.mode == "replica" and not message.internal:
                return Response(success=False, error="Replica server rejects direct writes. Write to primary server instead.")

            # Process locally first (idempotent with request_id)
            result = self.store.delete(message.key, request_id=message.request_id, log_sequence_number=message.log_sequence_number)
            message.log_sequence_number = result.get('log_sequence_number', self.log_sequence_number)

            # Then sync to replica if primary
            if self.mode == "primary" and self.peer_host:
                replica_response = self._sync_to_replica(message)
                if not replica_response or not replica_response.success:
                    error_msg = replica_response.error if replica_response else "Replica sync failed"
                    return Response(success=False, error=f"Replica rejected DELETE: {error_msg}")
            
            if result['success']:
                status = "DELETE (duplicate request)" if result['is_duplicate'] else "DELETE"
                return Response(success=True, data=f"{status} key '{message.key}'")
            else:
                return Response(success=False, error=f"Key '{message.key}' not found")
        
        elif operation == "get_lsn" and message.internal:
            # Special internal operation to get latest log sequence number for replication ordering
            lsn = self._get_latest_log_sequence_number()
            return Response(success=True, data=lsn)

        elif operation == "sync_request" and message.internal:
            if self.mode == "primary":
                # Primary receives sync request from replica - send all logs after replica's LSN
                self._sync_log_sequence_with_replica()
                return Response(success=True, data="Primary log sequence synced with replica")
    
    def _sync_to_replica(self, message: Message) -> Optional[Response]:
        """Send operation to replica and get response."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as replica_socket:
                replica_socket.settimeout(5.0)
                replica_socket.connect((self.peer_host, self.peer_port))
                
                # Mark message as internal replication message
                message.internal = True
                
                # Send message to replica
                message_bytes = message.to_bytes()
                message_length = len(message_bytes).to_bytes(4, byteorder='big')
                replica_socket.send(message_length + message_bytes)
                
                # Receive response from replica
                response_length_bytes = self.recv_exactly(replica_socket, 4)
                if not response_length_bytes:
                    logger.error(f"No response length from replica")
                    return None
                
                response_length = int.from_bytes(response_length_bytes, byteorder='big')
                response_data = b''
                while len(response_data) < response_length:
                    chunk = self.recv_exactly(replica_socket, min(4096, response_length - len(response_data)))
                    if not chunk:
                        break
                    response_data += chunk
                
                response = Response.from_bytes(response_data)
                logger.info(f"Replica response for {message.operation}: {response.success}")
                return response
                
        except Exception as e:
            logger.error(f"Failed to sync with replica: {e}")
            return None
    
    def _sync_log_sequence_with_replica(self) -> None:
        """Sync log sequence number with replica for ordering."""

        if self.mode != "primary" or not self.peer_host:
            return

        replica_lsn = self._get_latest_log_sequence_number_from_replica()

        if replica_lsn < 0:
            logger.error("Unable to connect replica server. Failed to get replica LSN.")
            return

        primary_lsn = self._get_latest_log_sequence_number()

        if primary_lsn > replica_lsn:
            logger.info(f"Primary LSN ({primary_lsn}) is ahead of replica LSN ({replica_lsn}). Syncing logs...")
            logs_to_sync = self.store.get_entries_from_log_sequence_number(replica_lsn)
            print(f"Found {len(logs_to_sync)} logs to sync to replica")
            for log_entry in logs_to_sync:
                # Create a Message from log entry and sync to replica
                message = Message(
                    operation=log_entry["op"],
                    key=log_entry["key"],
                    value=log_entry.get("value"),
                    request_id=log_entry.get("request_id"),
                    internal=True,
                    log_sequence_number=log_entry.get("log_sequence_number")
                )
                self._sync_to_replica(message)

        elif primary_lsn < replica_lsn:
            logger.error(f"Primary LSN ({primary_lsn}) is behind replica LSN ({replica_lsn}). This should not happen in normal operation.")
            raise RuntimeError("Primary LSN is behind replica LSN. Manual intervention needed.")

        return

    def _get_latest_log_sequence_number_from_replica(self) -> Optional[int]:
        """Get the latest log sequence number from the replica."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as replica_socket:
            try:
                replica_socket.settimeout(5.0)
                replica_socket.connect((self.peer_host, self.peer_port))
                
                # Send a special message to get the latest log sequence number
                message = Message(operation="get_lsn", key="", internal=True)
                message_bytes = message.to_bytes()
                message_length = len(message_bytes).to_bytes(4, byteorder='big')
                replica_socket.send(message_length + message_bytes)
                
                # Receive response from replica
                response_length_bytes = self.recv_exactly(replica_socket, 4)
                if not response_length_bytes:
                    logger.error(f"No response length from replica for LSN request")
                    return 0
                
                response_length = int.from_bytes(response_length_bytes, byteorder='big')
                response_data = b''
                while len(response_data) < response_length:
                    chunk = self.recv_exactly(replica_socket, min(4096, response_length - len(response_data)))
                    if not chunk:
                        break
                    response_data += chunk
                
                response = Response.from_bytes(response_data)
                if response.success and isinstance(response.data, int):
                    logger.info(f"Replica latest log sequence number: {response.data}")
                    return response.data
                else:
                    logger.error(f"Invalid LSN response from replica: {response.error}")
                    return 0
            
            except Exception as e:
                logger.error(f"Failed to get LSN from replica: {e}")
        return -1

    def _get_latest_log_sequence_number(self) -> int:
        """Get the latest log sequence number from the store."""
        return self.store.get_latest_log_sequence_number()
    
    def _request_primary_to_sync(self, message: Message) -> Optional[Response]:
        """Request primary server to sync a log entry to replica."""

        if self.mode != "replica" or not self.peer_host:
            return None

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as primary_socket:
                primary_socket.settimeout(5.0)
                primary_socket.connect((self.peer_host, self.peer_port))
                
                # Mark message as internal replication message
                message.internal = True
                
                # Send message to primary
                message_bytes = message.to_bytes()
                message_length = len(message_bytes).to_bytes(4, byteorder='big')
                primary_socket.send(message_length + message_bytes)
                
                # Receive response from primary
                response_length_bytes = self.recv_exactly(primary_socket, 4)
                if not response_length_bytes:
                    logger.error(f"No response length from primary")
                    return None
                
                response_length = int.from_bytes(response_length_bytes, byteorder='big')
                response_data = b''
                while len(response_data) < response_length:
                    chunk = self.recv_exactly(primary_socket, min(4096, response_length - len(response_data)))
                    if not chunk:
                        break
                    response_data += chunk
                
                response = Response.from_bytes(response_data)
                logger.info(f"Primary response for sync request: {response.success}")
                return response
                
        except Exception as e:
            logger.error(f"Failed to request sync from primary: {e}")
            return None

    def stop(self) -> None:
        """Stop the server."""
        self.running = False
        if self.socket:
            self.socket.close()
        logger.info("Server stopped")


def main():
    """Run the KV store server."""
    server = KVStoreServer(host="localhost", port=5000, storage_file="data/kv_store.jsonl")
    try:
        server.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")


if __name__ == "__main__":
    main()
