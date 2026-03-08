"""In-memory key-value store implementation."""

import threading
from typing import Any, Dict, Optional


class KVStore:
    """Thread-safe in-memory key-value store."""
    
    def __init__(self):
        self._data: Dict[str, Any] = {}
        self._lock = threading.RLock()
    
    def get(self, key: str) -> Optional[Any]:
        """Get value for key. Returns None if key doesn't exist."""
        with self._lock:
            return self._data.get(key)
    
    def set(self, key: str, value: Any) -> None:
        """Set value for key."""
        with self._lock:
            self._data[key] = value
    
    def keys(self) -> list:
        """Get all keys in the store."""
        with self._lock:
            return list(self._data.keys())
    
    def delete(self, key: str) -> bool:
        """Delete key from store. Returns True if key existed."""
        with self._lock:
            if key in self._data:
                del self._data[key]
                return True
            return False
    
    def clear(self) -> None:
        """Clear all data from store."""
        with self._lock:
            self._data.clear()
