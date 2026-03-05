import subprocess
import time
import requests
import json
import os
import signal

nodes = []

def start_nodes():
    # Node 1: Port 5001
    # Node 2: Port 5002
    # Node 3: Port 5003
    
    node_configs = [
        {"id": "node1", "port": 5001, "peers": "localhost:5002,localhost:5003"},
        {"id": "node2", "port": 5002, "peers": "localhost:5001,localhost:5003"},
        {"id": "node3", "port": 5003, "peers": "localhost:5001,localhost:5002"},
    ]
    
    for config in node_configs:
        cmd = [
            "py", "node.py",
            "--node_id", config["id"],
            "--port", str(config["port"]),
            "--peers", config["peers"],
            "--n", "3", "--r", "2", "--w", "2"
        ]
        p = subprocess.Popen(cmd)
        nodes.append(p)
        print(f"Started {config['id']} on port {config['port']}")
    
    time.sleep(3) # Wait for nodes to start

def stop_nodes():
    for p in nodes:
        p.terminate()
        p.wait()
    print("Stopped all nodes")

def test_replication():
    print("\n--- Test Use Case 1: Replication ---")
    # Add item to Node 1
    data = {"user_id": "user1", "item": "laptop", "quantity": 1}
    resp = requests.post("http://localhost:5001/cart/add", json=data)
    print(f"Add to Node 1: {resp.json()}")
    
    time.sleep(1)
    
    # Read from Node 2
    resp = requests.get("http://localhost:5002/cart/user1")
    print(f"Read from Node 2: {resp.json()}")
    
    # Read from Node 3
    resp = requests.get("http://localhost:5003/cart/user1")
    print(f"Read from Node 3: {resp.json()}")

def test_consistency_tunable():
    print("\n--- Test Use Case 2 & 3: Tunable Consistency ---")
    
    # Set N=3, W=1, R=1 (Eventual Consistency)
    for port in [5001, 5002, 5003]:
        requests.post(f"http://localhost:{port}/config", json={"r": 1, "w": 1})
    
    print("Configured W=1, R=1")
    
    # Write to Node 1
    data = {"user_id": "user2", "item": "mouse", "quantity": 1}
    requests.post("http://localhost:5001/cart/add", json=data)
    
    # Immediately read from Node 3 (might be stale if replication is slow, but local read is 5003's local)
    # Actually, in my implementation, handle_read_cart with R=1 will return local data.
    resp = requests.get("http://localhost:5003/cart/user2")
    print(f"Immediate read from Node 3 (W=1, R=1): {resp.json()}")

    # Set N=3, W=2, R=2 (Strong Consistency)
    for port in [5001, 5002, 5003]:
        requests.post(f"http://localhost:{port}/config", json={"r": 2, "w": 2})
    
    print("Configured W=2, R=2")
    data = {"user_id": "user2", "item": "mouse", "quantity": 1}
    requests.post("http://localhost:5001/cart/add", json=data)
    
    resp = requests.get("http://localhost:5003/cart/user2")
    print(f"Read from Node 3 (W=2, R=2): {resp.json()}")

def test_conflict_resolution():
    print("\n--- Test Use Case 4: Concurrent Write Conflict ---")
    
    # Set R=1, W=1 to allow fast local writes without immediate replication syncing
    for port in [5001, 5002, 5003]:
        requests.post(f"http://localhost:{port}/config", json={"r": 1, "w": 1})

    # Simulate concurrent writes by bypassing replication temporarily if possible
    # Or just write to two nodes quickly.
    # To truly simulate, I'll stop Node 2 and 3, write to Node 1, then stop Node 1, start Node 2 and write to it.
    
    # Actually, the easier way is to just send two requests to two different nodes.
    # Since they don't block each other, they will create concurrent vector clocks.
    
    # Reset user3
    data1 = {"user_id": "user3", "item": "laptop", "quantity": 1}
    requests.post("http://localhost:5001/cart/add", json=data1)
    
    data2 = {"user_id": "user3", "item": "mouse", "quantity": 1}
    requests.post("http://localhost:5002/cart/add", json=data2)
    
    time.sleep(1)
    
    # Now read from any node with R=2 or R=3 to trigger merge
    requests.post("http://localhost:5001/config", json={"r": 3, "w": 2})
    resp = requests.get("http://localhost:5001/cart/user3")
    print(f"Read after concurrent writes: {resp.json()}")

def test_checkout():
    print("\n--- Test Checkout ---")
    # Add items
    requests.post("http://localhost:5001/cart/add", json={"user_id": "user_check", "item": "laptop", "quantity": 2})
    
    # Checkout from node 2
    resp = requests.post("http://localhost:5002/cart/checkout", json={"user_id": "user_check"})
    print(f"Checkout response: {resp.json()}")
    
    # Check inventory
    resp = requests.get("http://localhost:5003/inventory")
    print(f"Inventory after checkout: {resp.json()}")

if __name__ == "__main__":
    try:
        # Cleanup old DB files
        import shutil
        for i in range(1, 4):
            if os.path.exists(f"db_node{i}"):
                shutil.rmtree(f"db_node{i}")
            if os.path.exists(f"node_node{i}.log"):
                os.remove(f"node_node{i}.log")

        start_nodes()
        test_replication()
        test_consistency_tunable()
        test_conflict_resolution()
        test_checkout()
    finally:
        stop_nodes()
