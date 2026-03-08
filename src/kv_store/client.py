"""Socket-based KV store client."""

import socket
from typing import Any, Optional

from src.kv_store.protocol import Message, Response


class KVStoreClient:
    """Client for connecting to KV store server."""
    
    def __init__(self, host: str = "localhost", port: int = 5000):
        self.host = host
        self.port = port
        self.socket: Optional[socket.socket] = None
    
    def connect(self) -> None:
        """Connect to the server."""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
    
    def disconnect(self) -> None:
        """Disconnect from the server."""
        if self.socket:
            self.socket.close()
    
    def get(self, key: str) -> Any:
        """Get value for key from server."""
        response = self._send_request("GET", key)
        if response.success:
            return response.data
        else:
            raise KeyError(response.error)
    
    def set(self, key: str, value: Any) -> None:
        """Set value for key on server."""
        response = self._send_request("SET", key, value)
        if not response.success:
            raise RuntimeError(response.error)
    
    def _send_request(self, operation: str, key: str, value: Any = None) -> Response:
        """Send a request to server and get response."""
        if not self.socket:
            raise RuntimeError("Not connected to server")
        
        # Create and send message
        message = Message(operation=operation, key=key, value=value)
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
            client.set("name", "Alice")
            client.set("age", 30)
            client.set("city", "New York")
            
            # Get values back
            print("\nGetting values...")
            print(f"name: {client.get('name')}")
            print(f"age: {client.get('age')}")
            print(f"city: {client.get('city')}")
            
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
