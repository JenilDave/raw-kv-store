"""Test client: Write to one server and read from another.

Prerequisites:
  Terminal 1: uv run main.py server --host localhost --port 5000 --storage data/server_5000.jsonl
  Terminal 2: uv run main.py server --host localhost --port 5001 --storage data/server_5001.jsonl
  Terminal 3: uv run test_cross_server.py
"""

import time
from src.kv_store.client import KVStoreClient


def test_cross_server_operations() -> None:
    """Test writing to server 5000 and reading from server 5001."""
    
    try:
        # Test 1: Write to server 1, read from server 2
        print("=" * 60)
        print("TEST 1: Write to Server 5000, Read from Server 5001")
        print("=" * 60)
        
        test_key = "test_key"
        test_value = "Hello from Server 5000"
        
        try:
            with KVStoreClient(host='localhost', port=5000) as client1:
                print(f"📝 Writing to Server 5000: {test_key} = {test_value}")
                req_id = client1.set(test_key, test_value)
                print(f"   Request ID: {req_id}")
        except Exception as e:
            print(f"✗ Failed to write to Server 5000: {e}")
            return
        
        try:
            with KVStoreClient(host='localhost', port=5001) as client2:
                print(f"📖 Reading from Server 5001: {test_key}")
                value = client2.get(test_key)
                print(f"✓ Got value: {value}")
                if value == test_value:
                    print("✅ SUCCESS: Data is visible across servers!")
                else:
                    print(f"⚠️  Data mismatch: Expected '{test_value}', got '{value}'")
        except KeyError as e:
            print(f"❌ FAILED: Server 5001 cannot read the key written to Server 5000")
            print(f"   Error: {e}")
            print("   This indicates servers don't share in-memory state by default.")
        except Exception as e:
            print(f"✗ Failed to read from Server 5001: {e}")
            return
        
        # Test 2: Write multiple keys and verify
        print("\n" + "=" * 60)
        print("TEST 2: Multiple Keys - Write to 5000, Read from 5001")
        print("=" * 60)
        
        test_data = {
            "username": "alice",
            "email": "alice@example.com",
            "age": "30"
        }
        
        try:
            with KVStoreClient(host='localhost', port=5000) as client1:
                for key, value in test_data.items():
                    print(f"📝 Writing to Server 5000: {key} = {value}")
                    client1.set(key, value)
        except Exception as e:
            print(f"✗ Failed to write to Server 5000: {e}")
            return
        
        try:
            with KVStoreClient(host='localhost', port=5001) as client2:
                print(f"📖 Reading from Server 5001...")
                results = {}
                for key in test_data.keys():
                    try:
                        value = client2.get(key)
                        results[key] = value
                        print(f"   ✓ {key} = {value}")
                    except KeyError:
                        results[key] = None
                        print(f"   ✗ {key} = NOT FOUND")
                
                visible_count = sum(1 for v in results.values() if v is not None)
                print(f"\n   Summary: {visible_count}/{len(test_data)} keys visible on Server 5001")
                
                if visible_count == len(test_data):
                    print("✅ SUCCESS: All data is visible across servers!")
                else:
                    print(f"⚠️  Partial visibility: {visible_count}/{len(test_data)} keys")
                    
        except Exception as e:
            print(f"✗ Failed to read from Server 5001: {e}")
            return
        
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_cross_server_operations()
