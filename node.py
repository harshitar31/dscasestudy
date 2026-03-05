import os
import json
import logging
import argparse
import requests
from flask import Flask, request, jsonify, render_template
from vector_clock import VectorClock
import threading
import time

app = Flask(__name__)

class Node:
    def __init__(self, node_id, port, peers, n=3, r=2, w=2):
        self.node_id = node_id
        self.port = port
        self.peers = peers  # List of "host:port"
        self.n = n
        self.r = r
        self.w = w
        
        self.db_dir = f"db_{node_id}"
        os.makedirs(self.db_dir, exist_ok=True)
        self.cart_db_path = os.path.join(self.db_dir, "cart_db.json")
        self.inventory_db_path = os.path.join(self.db_dir, "inventory_db.json")
        
        self.setup_logging()
        self.load_databases()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format=f'[%(asctime)s] [{self.node_id}] %(message)s',
            handlers=[
                logging.FileHandler(f"node_{self.node_id}.log"),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(self.node_id)

    def load_databases(self):
        if os.path.exists(self.cart_db_path):
            with open(self.cart_db_path, 'r') as f:
                self.cart_db = json.load(f)
        else:
            self.cart_db = {}
            self.save_cart_db()

        if os.path.exists(self.inventory_db_path):
            with open(self.inventory_db_path, 'r') as f:
                self.inventory_db = json.load(f)
        else:
            # Seed inventory if it doesn't exist
            self.inventory_db = {
                "laptop": {"stock": 25, "price": 80000, "vector_clock": {}},
                "mouse": {"stock": 100, "price": 500, "vector_clock": {}}
            }
            self.save_inventory_db()

    def save_cart_db(self):
        with open(self.cart_db_path, 'w') as f:
            json.dump(self.cart_db, f, indent=4)

    def save_inventory_db(self):
        with open(self.inventory_db_path, 'w') as f:
            json.dump(self.inventory_db, f, indent=4)

    def merge_carts(self, cart1, cart2):
        """Merges two cart states using vector clocks and conflict resolution."""
        # This is a simplified merge logic for demonstration
        # In a real system, we'd use CRDTs or more complex rules.
        # Requirement: "If vector clocks are concurrent, system must merge records."
        items1 = cart1.get("items", {})
        items2 = cart2.get("items", {})
        
        merged_items = {}
        all_items = set(items1.keys()).union(set(items2.keys()))
        for item in all_items:
            merged_items[item] = max(items1.get(item, 0), items2.get(item, 0))
            
        vc1 = VectorClock(cart1.get("vector_clock", {}))
        vc2 = VectorClock(cart2.get("vector_clock", {}))
        merged_vc = vc1.merge(vc2)
        
        return {
            "items": merged_items,
            "vector_clock": merged_vc.to_dict()
        }

    def handle_write_cart(self, user_id, item, quantity):
        self.logger.info(f"Write request received for user {user_id}: {item} x {quantity}")
        
        # 1. Update local database
        cart = self.cart_db.get(user_id, {"items": {}, "vector_clock": {}})
        vc = VectorClock(cart["vector_clock"])
        
        # 2. Increment vector clock
        vc.increment(self.node_id)
        cart["items"][item] = cart["items"].get(item, 0) + quantity
        cart["vector_clock"] = vc.to_dict()
        
        self.cart_db[user_id] = cart
        self.save_cart_db()
        self.logger.info(f"Local write successful. Vector clock: {cart['vector_clock']}")

        # 3. Send replication request to peer nodes
        acks = 1  # Standard Dynamo says coordinator is one of the replicas
        
        def replicate_to_peer(peer, results):
            try:
                url = f"http://{peer}/replicate/write_cart"
                response = requests.post(url, json={"user_id": user_id, "cart": cart}, timeout=2)
                if response.status_code == 200:
                    results.append(True)
            except Exception as e:
                self.logger.error(f"Failed to replicate to {peer}: {e}")

        threads = []
        replication_results = []
        for peer in self.peers:
            t = threading.Thread(target=replicate_to_peer, args=(peer, replication_results))
            threads.append(t)
            t.start()

        # 4. Wait for W-1 acknowledgements (since we already did local write)
        # In a real system we might use a timeout.
        start_time = time.time()
        while len(replication_results) < self.w - 1 and (time.time() - start_time) < 5:
            time.sleep(0.1)

        acks += len(replication_results)
        
        if acks >= self.w:
            self.logger.info(f"Quorum satisfied ({acks}/{self.w} acks). Confirming successfully to client.")
            return True, cart
        else:
            self.logger.warning(f"Quorum not satisfied ({acks}/{self.w} acks).")
            return False, cart

    def handle_read_cart(self, user_id):
        self.logger.info(f"Read request received for user {user_id}")
        
        # 1. Coordinator queries R replicas (including local)
        versions = [self.cart_db.get(user_id, {"items": {}, "vector_clock": {}})]
        
        def fetch_from_peer(peer, results):
            try:
                url = f"http://{peer}/replicate/read_cart"
                response = requests.get(url, params={"user_id": user_id}, timeout=2)
                if response.status_code == 200:
                    results.append(response.json().get("cart"))
            except Exception as e:
                self.logger.error(f"Failed to read from {peer}: {e}")

        threads = []
        peer_results = []
        for peer in self.peers:
            t = threading.Thread(target=fetch_from_peer, args=(peer, peer_results))
            threads.append(t)
            t.start()

        start_time = time.time()
        while len(peer_results) < self.r - 1 and (time.time() - start_time) < 5:
            time.sleep(0.1)

        versions.extend(peer_results)
        
        if len(versions) < self.r:
            self.logger.error(f"Failed to reach read quorum ({len(versions)}/{self.r})")
            return None

        # 2. Compare vector clocks and detect conflicts
        latest = versions[0]
        stale_indices = []
        conflict = False
        
        for i in range(1, len(versions)):
            vc_latest = VectorClock(latest["vector_clock"])
            vc_current = VectorClock(versions[i]["vector_clock"])
            cmp = vc_latest.compare(vc_current)
            
            if cmp == -1: # latest is stale
                stale_indices.append(0) # previous 'latest' was stale
                latest = versions[i]
            elif cmp == 1: # current is stale
                stale_indices.append(i)
            elif cmp == 0:
                pass # identical
            else: # concurrent / conflict
                self.logger.info("Conflict detected! Merging records.")
                latest = self.merge_carts(latest, versions[i])
                conflict = True

        # 3. Perform read repair if stale replica detected
        if len(stale_indices) > 0 or conflict:
            self.logger.info("Read repair triggered for stale/conflicting replicas.")
            self.trigger_read_repair(user_id, latest)

        return latest

    def trigger_read_repair(self, user_id, latest_cart):
        def repair_peer(peer):
            try:
                url = f"http://{peer}/replicate/write_cart"
                requests.post(url, json={"user_id": user_id, "cart": latest_cart}, timeout=1)
            except:
                pass
        
        # Also repair local if it was stale (though handled by logic above, let's just make sure)
        self.cart_db[user_id] = latest_cart
        self.save_cart_db()

        for peer in self.peers:
            threading.Thread(target=repair_peer, args=(peer,)).start()

    # Replicate Write Handlers
    def receive_replicate_write(self, user_id, remote_cart):
        local_cart = self.cart_db.get(user_id, {"items": {}, "vector_clock": {}})
        vc_local = VectorClock(local_cart["vector_clock"])
        vc_remote = VectorClock(remote_cart["vector_clock"])
        
        cmp = vc_local.compare(vc_remote)
        if cmp == -1: # local is stale
            self.cart_db[user_id] = remote_cart
            self.save_cart_db()
            self.logger.info(f"Updated replica for user {user_id} (remote was newer)")
        elif cmp == None: # conflict
            self.logger.info(f"Conflict detected during replication for {user_id}. Merging.")
            merged = self.merge_carts(local_cart, remote_cart)
            self.cart_db[user_id] = merged
            self.save_cart_db()
        else:
            self.logger.info(f"Replication received but local is already up to date or newer for {user_id}")
        
        return True

    # Inventory Operations
    def handle_checkout(self, user_id):
        self.logger.info(f"Checkout request received for user {user_id}")
        
        # 1. Get latest cart with quorum
        cart = self.handle_read_cart(user_id)
        if not cart or not cart.get("items"):
            return False, "Cart is empty or could not be read"
        
        items_to_buy = cart["items"]
        
        # 2. Validate inventory (Simplified: check local inventory)
        # In a real system, inventory would also be quorum-read
        for item, qty in items_to_buy.items():
            current_stock = self.inventory_db.get(item, {}).get("stock", 0)
            if current_stock < qty:
                return False, f"Insufficient stock for {item}"
        
        # 3. Update inventory locally and replicate
        for item, qty in items_to_buy.items():
            self.inventory_db[item]["stock"] -= qty
            # Increment inventory vector clock
            vc = VectorClock(self.inventory_db[item].get("vector_clock", {}))
            vc.increment(self.node_id)
            self.inventory_db[item]["vector_clock"] = vc.to_dict()
        
        self.save_inventory_db()
        
        # Replicate inventory updates
        for peer in self.peers:
            def replicate_inv(p):
                try:
                    url = f"http://{p}/replicate/inventory"
                    requests.post(url, json={"inventory": self.inventory_db}, timeout=2)
                except:
                    pass
            threading.Thread(target=replicate_inv, args=(peer,)).start()

        # 4. Clear cart and replicate
        self.cart_db[user_id] = {"items": {}, "vector_clock": cart["vector_clock"]}
        # Increment VC for cart clearing
        vc_cart = VectorClock(self.cart_db[user_id]["vector_clock"])
        vc_cart.increment(self.node_id)
        self.cart_db[user_id]["vector_clock"] = vc_cart.to_dict()
        self.save_cart_db()
        
        self.trigger_read_repair(user_id, self.cart_db[user_id])
        
        return True, "Checkout successful"

    def receive_replicate_inventory(self, remote_inventory):
        for item, remote_data in remote_inventory.items():
            if item not in self.inventory_db:
                self.inventory_db[item] = remote_data
                continue
            
            vc_local = VectorClock(self.inventory_db[item].get("vector_clock", {}))
            vc_remote = VectorClock(remote_data.get("vector_clock", {}))
            
            if vc_local.compare(vc_remote) == -1: # local is stale
                self.inventory_db[item] = remote_data
        
        self.save_inventory_db()
        return True

node = None

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/state', methods=['GET'])
def get_state():
    user_id = request.args.get('user_id', 'user1')
    return jsonify({
        "node_id": node.node_id,
        "port": node.port,
        "n": node.n,
        "r": node.r,
        "w": node.w,
        "cart": node.cart_db.get(user_id, {"items": {}, "vector_clock": {}}),
        "inventory": node.inventory_db
    }), 200

@app.route('/cart/add', methods=['POST'])
def add_to_cart():
    data = request.json
    user_id = data.get('user_id')
    item = data.get('item')
    quantity = data.get('quantity', 1)
    
    success, cart = node.handle_write_cart(user_id, item, quantity)
    if success:
        return jsonify({"status": "success", "cart": cart}), 200
    else:
        return jsonify({"status": "quorum_failed", "cart": cart}), 503

@app.route('/cart/<user_id>', methods=['GET'])
def get_cart(user_id):
    cart = node.handle_read_cart(user_id)
    if cart:
        return jsonify({"status": "success", "cart": cart}), 200
    else:
        # Fallback to local if quorum fails (Eventual Consistency)
        local_cart = node.cart_db.get(user_id, {"items": {}, "vector_clock": {}})
        return jsonify({"status": "quorum_failed", "cart": local_cart}), 200

@app.route('/cart/checkout', methods=['POST'])
def checkout():
    data = request.json
    user_id = data.get('user_id')
    success, message = node.handle_checkout(user_id)
    if success:
        return jsonify({"status": "success", "message": message}), 200
    else:
        return jsonify({"status": "error", "message": message}), 400

@app.route('/inventory', methods=['GET'])
def get_inventory():
    return jsonify(node.inventory_db), 200

@app.route('/replicate/write_cart', methods=['POST'])
def replicate_write_cart():
    data = request.json
    user_id = data.get('user_id')
    cart = data.get('cart')
    node.receive_replicate_write(user_id, cart)
    return jsonify({"status": "ok"}), 200

@app.route('/replicate/read_cart', methods=['GET'])
def replicate_read_cart():
    user_id = request.args.get('user_id')
    cart = node.cart_db.get(user_id, {"items": {}, "vector_clock": {}})
    return jsonify({"cart": cart}), 200

@app.route('/replicate/inventory', methods=['POST'])
def replicate_inventory():
    data = request.json
    inventory = data.get('inventory')
    node.receive_replicate_inventory(inventory)
    return jsonify({"status": "ok"}), 200

@app.route('/config', methods=['POST'])
def update_config():
    data = request.json
    if 'r' in data: node.r = data['r']
    if 'w' in data: node.w = data['w']
    node.logger.info(f"Updated Quorum Configuration: R={node.r}, W={node.w}")
    return jsonify({"status": "ok", "r": node.r, "w": node.w}), 200

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--node_id", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--peers", help="Comma separated list of peer host:port")
    parser.add_argument("--n", type=int, default=3)
    parser.add_argument("--r", type=int, default=2)
    parser.add_argument("--w", type=int, default=2)
    
    args = parser.parse_args()
    
    peers = args.peers.split(",") if args.peers else []
    node = Node(args.node_id, args.port, peers, n=args.n, r=args.r, w=args.w)
    
    app.run(host='0.0.0.0', port=args.port)
