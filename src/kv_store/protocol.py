"""Message protocol for KV store communication."""

import msgpack
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class Message:
    """Base message format."""
    operation: str
    key: str
    value: Any = None
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    internal: bool = False  # True if this is an internal replication message
    
    def to_bytes(self) -> bytes:
        """Serialize message to bytes."""
        data = {
            "operation": self.operation,
            "key": self.key,
            "value": self.value,
            "request_id": self.request_id,
            "internal": self.internal,
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
            request_id=decoded.get("request_id", str(uuid.uuid4())),
            internal=decoded.get("internal", False),
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
