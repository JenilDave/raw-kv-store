"""Socket-based KV store server."""

import socket
import threading
from typing import Optional

from src.kv_store.store import KVStore
from src.kv_store.protocol import Message, Response
from src.kv_store.logging_util import setup_logger, get_timestamped_logfile


logger = None  # Will be initialized when server starts


class KVStoreServer:
    """TCP socket-based key-value store server."""
    
    def __init__(self, host: str = "localhost", port: int = 5000, storage_file: str = "data/kv_store.jsonl",
                 mode: str = "standalone", replica_host: str = None, replica_port: int = None):
        self.host = host
        self.port = port
        self.store = KVStore(storage_file=storage_file)
        self.socket: Optional[socket.socket] = None
        self.running = False
        self.mode = mode  # "primary", "replica", or "standalone"
        self.replica_host = replica_host
        self.replica_port = replica_port
    
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
            if self.mode == "primary" and self.replica_host:
                logger.info(f"Replica server: {self.replica_host}:{self.replica_port}")
            
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
        finally:
            self.stop()
    
    def _handle_client(self, client_socket: socket.socket, client_addr: tuple) -> None:
        """Handle a single client connection."""
        try:
            while self.running:
                # Receive message length first (4 bytes)
                length_bytes = client_socket.recv(4)
                if not length_bytes:
                    break
                
                message_length = int.from_bytes(length_bytes, byteorder='big')
                
                # Receive the actual message
                message_data = b''
                while len(message_data) < message_length:
                    chunk = client_socket.recv(min(4096, message_length - len(message_data)))
                    if not chunk:
                        break
                    message_data += chunk
                
                if not message_data:
                    break
                
                # Parse and process the message
                try:
                    message = Message.from_bytes(message_data)
                    # Force internal flag to False - only server can set it to True
                    message.internal = False
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
            result = self.store.set(message.key, message.value, request_id=message.request_id)
            status = "SET (duplicate request)" if result['is_duplicate'] else "SET"
            
            # Then sync to replica if primary
            if self.mode == "primary" and self.replica_host:
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
            result = self.store.delete(message.key, request_id=message.request_id)
            
            # Then sync to replica if primary
            if self.mode == "primary" and self.replica_host:
                replica_response = self._sync_to_replica(message)
                if not replica_response or not replica_response.success:
                    error_msg = replica_response.error if replica_response else "Replica sync failed"
                    return Response(success=False, error=f"Replica rejected DELETE: {error_msg}")
            
            if result['success']:
                status = "DELETE (duplicate request)" if result['is_duplicate'] else "DELETE"
                return Response(success=True, data=f"{status} key '{message.key}'")
            else:
                return Response(success=False, error=f"Key '{message.key}' not found")
        
        else:
            return Response(success=False, error=f"Unknown operation: {operation}")
    
    def _sync_to_replica(self, message: Message) -> Optional[Response]:
        """Send operation to replica and get response."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as replica_socket:
                replica_socket.settimeout(5.0)
                replica_socket.connect((self.replica_host, self.replica_port))
                
                # Mark message as internal replication message
                message.internal = True
                
                # Send message to replica
                message_bytes = message.to_bytes()
                message_length = len(message_bytes).to_bytes(4, byteorder='big')
                replica_socket.send(message_length + message_bytes)
                
                # Receive response from replica
                response_length_bytes = replica_socket.recv(4)
                if not response_length_bytes:
                    logger.error(f"No response length from replica")
                    return None
                
                response_length = int.from_bytes(response_length_bytes, byteorder='big')
                response_data = b''
                while len(response_data) < response_length:
                    chunk = replica_socket.recv(min(4096, response_length - len(response_data)))
                    if not chunk:
                        break
                    response_data += chunk
                
                response = Response.from_bytes(response_data)
                logger.info(f"Replica response for {message.operation}: {response.success}")
                return response
                
        except Exception as e:
            logger.error(f"Failed to sync with replica: {e}")
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
