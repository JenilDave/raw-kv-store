"""Test client: Write to one server and read from another.

Prerequisites:
  Terminal 1: uv run main.py server --host localhost --port 5000 --storage data/server_5000.jsonl --server-mode replica
  Terminal 2: uv run main.py server --host localhost --port 5001 --storage data/server_5001.jsonl --server-mode primary --replica-host localhost --replica-port 5000
  Terminal 3: uv run test_cross_server.py

This setup:
  - Server 5001 (PRIMARY): Accepts client requests, syncs writes to Server 5000 (replica)
  - Server 5000 (REPLICA): Receives synced writes from primary, must acknowledge for primary to succeed
  - Client writes will succeed only if replica confirms
"""

import time
from src.kv_store.client import KVStoreClient


def test_cross_server_operations() -> None:
    """Test writing to server 5000 and reading from server 5001."""
    
    try:
        # Test 1: Write to server 1, read from server 2
        print("=" * 60)
        print("TEST 1: Write to PRIMARY (5001), Read from REPLICA (5000)")
        print("=" * 60)
        
        test_key = "test_key"
        test_value = "Hello from Primary Server"
        
        try:
            with KVStoreClient(host='localhost', port=5001) as primary:
                print(f"📝 Writing to PRIMARY (5001): {test_key} = {test_value}")
                req_id = primary.set(test_key, test_value)
                print(f"   Request ID: {req_id}")
                print(f"   ✓ Primary accepted and synced to replica")
        except Exception as e:
            print(f"✗ Failed to write to PRIMARY (5001): {e}")
            return
        
        try:
            with KVStoreClient(host='localhost', port=5000) as replica:
                print(f"📖 Reading from REPLICA (5000): {test_key}")
                value = replica.get(test_key)
                print(f"✓ Got value: {value}")
                if value == test_value:
                    print("✅ SUCCESS: Data replicated from primary to replica!")
                else:
                    print(f"⚠️  Data mismatch: Expected '{test_value}', got '{value}'")
        except KeyError as e:
            print(f"❌ FAILED: REPLICA (5000) does not have the data from PRIMARY (5001)")
            print(f"   Error: {e}")
            print("   The primary synced successfully but replica doesn't have the data.")
        except Exception as e:
            print(f"✗ Failed to read from REPLICA (5000): {e}")
            return
        
        # Test 2: Write multiple keys and verify
        print("\n" + "=" * 60)
        print("TEST 2: Multiple Keys - Write to PRIMARY (5001), Read from REPLICA (5000)")
        print("=" * 60)
        
        test_data = {
            "username": "alice",
            "email": "alice@example.com",
            "age": "30"
        }
        
        try:
            with KVStoreClient(host='localhost', port=5001) as primary:
                for key, value in test_data.items():
                    print(f"📝 Writing to PRIMARY (5001): {key} = {value}")
                    primary.set(key, value)
        except Exception as e:
            print(f"✗ Failed to write to PRIMARY (5001): {e}")
            return
        
        try:
            with KVStoreClient(host='localhost', port=5000) as replica:
                print(f"📖 Reading from REPLICA (5000)...")
                results = {}
                for key in test_data.keys():
                    try:
                        value = replica.get(key)
                        results[key] = value
                        print(f"   ✓ {key} = {value}")
                    except KeyError:
                        results[key] = None
                        print(f"   ✗ {key} = NOT FOUND")
                
                visible_count = sum(1 for v in results.values() if v is not None)
                print(f"\n   Summary: {visible_count}/{len(test_data)} keys replicated to REPLICA")
                
                if visible_count == len(test_data):
                    print("✅ SUCCESS: All data replicated from primary to replica!")
                else:
                    print(f"⚠️  Partial replication: {visible_count}/{len(test_data)} keys")
                    
        except Exception as e:
            print(f"✗ Failed to read from REPLICA (5000): {e}")
            return
        
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_cross_server_operations()
