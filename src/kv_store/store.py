"""Key-value store with persistent file storage and in-memory optimization."""

import json
import threading
import os
from typing import Any, Dict, Optional, Set
from pathlib import Path


class KVStore:
    """Thread-safe key-value store with persistent file storage and idempotent operations.
    
    - Reads: Served from in-memory dictionary (fast)
    - Writes: Persisted to append-only JSON lines file (optimized)
    - Startup: Loads data from persistent file into memory
    - Idempotency: Tracks request IDs to prevent duplicate operation processing
    - Compaction: Periodically deduplicates JSONL file
    """
    
    def __init__(self, storage_file: str = "data/kv_store.jsonl", compact_threshold: int = 100):
        """Initialize KV store with persistent file storage.
        
        Args:
            storage_file: Path to the persistent storage file
            compact_threshold: Number of operations before triggering compaction
        """
        self._data: Dict[str, Any] = {}
        self._lock = threading.RLock()
        self._processed_requests: Set[str] = set()
        self._operation_count = 0
        self.storage_file = Path(storage_file)
        self.compact_threshold = compact_threshold
        
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
                        request_id = record.get('request_id')
                        
                        # Track processed request IDs for idempotency
                        if request_id:
                            self._processed_requests.add(request_id)
                        
                        if operation == 'set' and key is not None:
                            self._data[key] = value
                        elif operation == 'delete' and key is not None:
                            self._data.pop(key, None)
        except Exception as e:
            raise RuntimeError(f"Failed to load data from {self.storage_file}: {e}")
    
    def _persist_to_file(self, operation: str, key: str, request_id: str, value: Any = None) -> None:
        """Append operation record to persistent storage file (write-optimized).
        
        Args:
            operation: 'set' or 'delete'
            key: The key being operated on
            request_id: Unique request ID for idempotency
            value: The value (for 'set' operations)
        """
        try:
            record = {
                'op': operation,
                'key': key,
                'value': value,
                'request_id': request_id
            }
            with open(self.storage_file, 'a') as f:
                f.write(json.dumps(record) + '\n')
            
            self._operation_count += 1
            
            # Trigger compaction if threshold exceeded
            if self._operation_count >= self.compact_threshold:
                self._compact_storage()
                
        except Exception as e:
            raise RuntimeError(f"Failed to persist data to {self.storage_file}: {e}")
    
    def _compact_storage(self) -> None:
        """Compact storage file by removing redundant operations."""
        try:
            # Read all records and build final state
            all_records = {}
            
            with open(self.storage_file, 'r') as f:
                for line in f:
                    if line.strip():
                        record = json.loads(line)
                        operation = record.get('op')
                        key = record.get('key')
                        request_id = record.get('request_id')
                        
                        if key not in all_records:
                            all_records[key] = []
                        all_records[key].append(record)
            
            # Write only the final state of each key
            with open(self.storage_file, 'w') as f:
                for key, records in all_records.items():
                    # Use the last operation for this key
                    final_record = records[-1]
                    f.write(json.dumps(final_record) + '\n')
            
            self._operation_count = 0
            
        except Exception as e:
            raise RuntimeError(f"Failed to compact storage: {e}")
    
    def get(self, key: str) -> Optional[Any]:
        """Get value for key from in-memory store (read-optimized).
        
        Returns None if key doesn't exist.
        Idempotent: Multiple reads return same value.
        """
        with self._lock:
            return self._data.get(key)
    
    def set(self, key: str, value: Any, request_id: str = None) -> dict:
        """Set value for key in both in-memory store and persistent file.
        
        Args:
            key: The key to set
            value: The value to set
            request_id: Unique request ID for idempotency
            
        Returns:
            Dict with 'is_duplicate' flag and optional cached response
        """
        with self._lock:
            if request_id and request_id in self._processed_requests:
                # This is a duplicate request - return idempotent response
                return {'is_duplicate': True, 'current_value': self._data.get(key)}
            
            self._data[key] = value
            if request_id:
                self._processed_requests.add(request_id)
            self._persist_to_file('set', key, request_id or '', value)
            
            return {'is_duplicate': False, 'current_value': value}
    
    def keys(self) -> list:
        """Get all keys in the store.
        Idempotent: Multiple calls return same keys.
        """
        with self._lock:
            return list(self._data.keys())
    
    def delete(self, key: str, request_id: str = None) -> dict:
        """Delete key from store.
        
        Args:
            key: The key to delete
            request_id: Unique request ID for idempotency
            
        Returns:
            Dict with 'success' and 'is_duplicate' flags
        """
        with self._lock:
            if request_id and request_id in self._processed_requests:
                # This is a duplicate request - return idempotent response
                return {'success': key in self._data or key not in self._data, 'is_duplicate': True}
            
            deleted = key in self._data
            if deleted:
                del self._data[key]
                if request_id:
                    self._processed_requests.add(request_id)
                self._persist_to_file('delete', key, request_id or '')
            
            return {'success': deleted, 'is_duplicate': False}
    
    def clear(self) -> None:
        """Clear all data from store.
        Idempotent: Clearing empty store has same effect as clearing full store.
        """
        with self._lock:
            keys_to_delete = list(self._data.keys())
            self._data.clear()
            self._processed_requests.clear()
            for key in keys_to_delete:
                self._persist_to_file('delete', key, '')
