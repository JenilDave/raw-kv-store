"""Key-value store with persistent file storage and in-memory optimization."""

import json
import threading
import os
from typing import Any, Dict, Optional
from pathlib import Path


class KVStore:
    """Thread-safe key-value store with persistent file storage.
    
    - Reads: Served from in-memory dictionary (fast)
    - Writes: Persisted to append-only JSON lines file (optimized)
    - Startup: Loads data from persistent file into memory
    """
    
    def __init__(self, storage_file: str = "data/kv_store.jsonl"):
        """Initialize KV store with persistent file storage.
        
        Args:
            storage_file: Path to the persistent storage file
        """
        self._data: Dict[str, Any] = {}
        self._lock = threading.RLock()
        self.storage_file = Path(storage_file)
        
        # Create directory if it doesn't exist
        self.storage_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Load existing data from file
        self._load_from_file()
    
    def _load_from_file(self) -> None:
        """Load data from persistent storage file into memory."""
        if not self.storage_file.exists():
            return
        
        try:
            with open(self.storage_file, 'r') as f:
                for line in f:
                    if line.strip():
                        record = json.loads(line)
                        operation = record.get('op')
                        key = record.get('key')
                        value = record.get('value')
                        
                        if operation == 'set' and key is not None:
                            self._data[key] = value
                        elif operation == 'delete' and key is not None:
                            self._data.pop(key, None)
        except Exception as e:
            raise RuntimeError(f"Failed to load data from {self.storage_file}: {e}")
    
    def _persist_to_file(self, operation: str, key: str, value: Any = None) -> None:
        """Append operation record to persistent storage file (write-optimized).
        
        Args:
            operation: 'set' or 'delete'
            key: The key being operated on
            value: The value (for 'set' operations)
        """
        try:
            record = {
                'op': operation,
                'key': key,
                'value': value
            }
            with open(self.storage_file, 'a') as f:
                f.write(json.dumps(record) + '\n')
        except Exception as e:
            raise RuntimeError(f"Failed to persist data to {self.storage_file}: {e}")
    
    def get(self, key: str) -> Optional[Any]:
        """Get value for key from in-memory store (read-optimized).
        
        Returns None if key doesn't exist.
        """
        with self._lock:
            return self._data.get(key)
    
    def set(self, key: str, value: Any) -> None:
        """Set value for key in both in-memory store and persistent file."""
        with self._lock:
            self._data[key] = value
            self._persist_to_file('set', key, value)
    
    def keys(self) -> list:
        """Get all keys in the store."""
        with self._lock:
            return list(self._data.keys())
    
    def delete(self, key: str) -> bool:
        """Delete key from store. Returns True if key existed."""
        with self._lock:
            if key in self._data:
                del self._data[key]
                self._persist_to_file('delete', key)
                return True
            return False
    
    def clear(self) -> None:
        """Clear all data from store."""
        with self._lock:
            keys_to_delete = list(self._data.keys())
            self._data.clear()
            for key in keys_to_delete:
                self._persist_to_file('delete', key)
