"""Socket-based KV store client."""

import socket
import uuid
from typing import Any, Optional

from src.kv_store.protocol import Message, Response


class KVStoreClient:
    """Client for connecting to KV store server."""
    
    def __init__(self, host: str = "localhost", port: int = 5000):
        self.host = host
        self.port = port
        self.socket: Optional[socket.socket] = None
        self._request_id_map = {}  # Map for tracking request IDs by operation
    
    def connect(self) -> None:
        """Connect to the server."""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
    
    def disconnect(self) -> None:
        """Disconnect from the server."""
        if self.socket:
            self.socket.close()
    
    def get(self, key: str, request_id: str = None) -> Any:
        """Get value for key from server.
        
        Args:
            key: Key to retrieve
            request_id: Optional request ID for idempotency
        """
        response = self._send_request("GET", key, request_id=request_id)
        if response.success:
            return response.data
        else:
            raise KeyError(response.error)
    
    def set(self, key: str, value: Any, request_id: str = None) -> str:
        """Set value for key on server.
        
        Args:
            key: Key to set
            value: Value to set
            request_id: Optional request ID for idempotency (auto-generated if not provided)
            
        Returns:
            The request ID used for this operation
        """
        request_id = request_id or str(uuid.uuid4())
        response = self._send_request("SET", key, value, request_id=request_id)
        if not response.success:
            raise RuntimeError(response.error)
        return request_id
    
    def delete(self, key: str, request_id: str = None) -> str:
        """Delete key from server.
        
        Args:
            key: Key to delete
            request_id: Optional request ID for idempotency (auto-generated if not provided)
            
        Returns:
            The request ID used for this operation
        """
        request_id = request_id or str(uuid.uuid4())
        response = self._send_request("DELETE", key, request_id=request_id)
        if not response.success:
            raise RuntimeError(response.error)
        return request_id
    
    def _send_request(self, operation: str, key: str, value: Any = None, request_id: str = None) -> Response:
        """Send a request to server and get response.
        
        Args:
            operation: Operation type (GET, SET, DELETE)
            key: Key for the operation
            value: Value (for SET operations)
            request_id: Optional request ID for idempotency
        """
        if not self.socket:
            raise RuntimeError("Not connected to server")
        
        # Create and send message with request_id
        message = Message(operation=operation, key=key, value=value, request_id=request_id or str(uuid.uuid4()))
        message_bytes = message.to_bytes()
        message_length = len(message_bytes).to_bytes(4, byteorder='big')
        
        self.socket.send(message_length + message_bytes)
        
        # Receive response
        response_length_bytes = self.socket.recv(4)
        response_length = int.from_bytes(response_length_bytes, byteorder='big')
        
        response_data = b''
        while len(response_data) < response_length:
            chunk = self.socket.recv(min(4096, response_length - len(response_data)))
            if not chunk:
                break
            response_data += chunk
        
        return Response.from_bytes(response_data)
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()


def main():
    """Example client usage."""
    try:
        with KVStoreClient() as client:
            # Set some values
            print("Setting values...")
            req_id_1 = client.set("name", "Alice")
            req_id_2 = client.set("age", 30)
            req_id_3 = client.set("city", "New York")
            print(f"  name: request_id={req_id_1}")
            print(f"  age: request_id={req_id_2}")
            print(f"  city: request_id={req_id_3}")
            
            # Get values back
            print("\nGetting values...")
            print(f"name: {client.get('name')}")
            print(f"age: {client.get('age')}")
            print(f"city: {client.get('city')}")
            
            # Demonstrate idempotency with duplicate request
            print("\nDemonstrating idempotency (resending SET with same request_id)...")
            req_id_dup = client.set("name", "Alice", request_id=req_id_1)
            print(f"  Resent with same request_id={req_id_dup} (server detected duplicate)")
            
            # Delete a key
            print("\nDeleting 'city' key...")
            req_id_del = client.delete("city")
            print(f"  delete: request_id={req_id_del}")
            
            # Try to get deleted key (should fail)
            print("\nTrying to get deleted key...")
            try:
                client.get("city")
            except KeyError as e:
                print(f"  KeyError: {e}")
            
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
