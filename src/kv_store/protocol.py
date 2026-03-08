"""Message protocol for KV store communication."""

import msgpack
from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class Message:
    """Base message format."""
    operation: str
    key: str
    value: Any = None
    
    def to_bytes(self) -> bytes:
        """Serialize message to bytes."""
        data = {
            "operation": self.operation,
            "key": self.key,
            "value": self.value,
        }
        return msgpack.packb(data)
    
    @classmethod
    def from_bytes(cls, data: bytes) -> "Message":
        """Deserialize message from bytes."""
        decoded = msgpack.unpackb(data, raw=False)
        return cls(
            operation=decoded["operation"],
            key=decoded["key"],
            value=decoded.get("value"),
        )


@dataclass
class Response:
    """Response message format."""
    success: bool
    data: Any = None
    error: str = None
    
    def to_bytes(self) -> bytes:
        """Serialize response to bytes."""
        response_data = {
            "success": self.success,
            "data": self.data,
            "error": self.error,
        }
        return msgpack.packb(response_data)
    
    @classmethod
    def from_bytes(cls, data: bytes) -> "Response":
        """Deserialize response from bytes."""
        decoded = msgpack.unpackb(data, raw=False)
        return cls(
            success=decoded["success"],
            data=decoded.get("data"),
            error=decoded.get("error"),
        )
