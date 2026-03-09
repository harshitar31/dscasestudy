import os
import json
import logging
import argparse
import requests
from flask import Flask, request, jsonify, render_template
from vector_clock import VectorClock
import threading
import time
from pyngrok import ngrok

app = Flask(__name__)

class Node:
    def __init__(self, node_id, port, peers, n=3, r=2, w=2, public_url=None):
        self.node_id = node_id
        self.port = port
        self.peers = peers  # List of "host:port" or "ngrok-url"
        self.n = n
        self.r = r
        self.w = w
        self.public_url = public_url
        
        self.db_dir = f"db_{node_id}"
        os.makedirs(self.db_dir, exist_ok=True)
        self.cart_db_path = os.path.join(self.db_dir, "cart_db.json")
        self.inventory_db_path = os.path.join(self.db_dir, "inventory_db.json")
        
        # Distributed locking state
        self.item_locks = {}  # item_id -> coordinator_node_id
        
        self.setup_logging()
        self.load_databases()
        
        # Start background sync with peers
        threading.Thread(target=self.sync_inventory_with_peers, daemon=True).start()

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
            self.inventory_db = {}

        # DEFAULT CATALOG: Always ensure these items exist
        defaults = {
            "laptop": {"stock": 25, "price": 80000, "vector_clock": {}},
            "mouse": {"stock": 100, "price": 500, "vector_clock": {}},
            "keyboard": {"stock": 50, "price": 1500, "vector_clock": {}},
            "monitor": {"stock": 30, "price": 12000, "vector_clock": {}},
            "phone": {"stock": 40, "price": 50000, "vector_clock": {}},
            "tablet": {"stock": 20, "price": 30000, "vector_clock": {}}
        }
        
        updated = False
        for item, data in defaults.items():
            if item not in self.inventory_db:
                self.inventory_db[item] = data
                updated = True
        
        if updated or not os.path.exists(self.inventory_db_path):
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
                url = self.get_peer_url(peer, "replicate/write_cart")
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
                url = self.get_peer_url(peer, "replicate/read_cart")
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
                url = self.get_peer_url(peer, "replicate/write_cart")
                requests.post(url, json={"user_id": user_id, "cart": latest_cart}, timeout=1)
            except:
                pass
        
        # Also repair local if it was stale (though handled by logic above, let's just make sure)
        self.cart_db[user_id] = latest_cart
        self.save_cart_db()

        for peer in self.peers:
            threading.Thread(target=repair_peer, args=(peer,)).start()

    def get_peer_url(self, peer_addr, path):
        if peer_addr.startswith("http"):
            return f"{peer_addr.rstrip('/')}/{path.lstrip('/')}"
        return f"http://{peer_addr}/{path.lstrip('/')}"

    def add_peer(self, peer_addr, announce_back=True):
        # Normalize and avoid self-peering
        if not peer_addr: return False
        
        # Don't add if already exists or is self
        if peer_addr in self.peers: return False
        if peer_addr == self.public_url: return False
        if peer_addr.split(':')[-1] == str(self.port) and ('localhost' in peer_addr or '127.0.0.1' in peer_addr):
            return False

        self.peers.append(peer_addr)
        self.logger.info(f"Dynamically added new peer: {peer_addr}")
        
        # Tell them to add us back if they don't know us
        if announce_back and self.public_url:
            def announce_to_new_peer():
                try:
                    url = self.get_peer_url(peer_addr, "peers/add")
                    requests.post(url, json={"peer_url": self.public_url, "announce_back": False}, timeout=2)
                except:
                    pass
            threading.Thread(target=announce_to_new_peer, daemon=True).start()

        # Immediately sync with the new peer
        threading.Thread(target=self.sync_with_specific_peer, args=(peer_addr,), daemon=True).start()
        return True

    def sync_with_specific_peer(self, peer):
        self.logger.info(f"Catching up with new peer: {peer}")
        try:
             # Sync inventory
             inv_url = self.get_peer_url(peer, "inventory")
             resp = requests.get(inv_url, timeout=3)
             if resp.status_code == 200:
                 self.receive_replicate_inventory(resp.json())
             
             # Sync config
             state_url = self.get_peer_url(peer, "state")
             resp = requests.get(state_url, timeout=3)
             if resp.status_code == 200:
                 data = resp.json()
                 self.r = data.get('r', self.r)
                 self.w = data.get('w', self.w)
             self.logger.info(f"Successfully caught up with {peer}")
        except Exception as e:
            self.logger.error(f"Failed to catch up with {peer}: {e}")

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
        
        items_to_buy = list(cart["items"].keys())
        if not items_to_buy:
            return False, "Cart is empty"

        # 2. Acquire Distributed Locks from Majority
        self.logger.info(f"Attempting to acquire distributed locks for {items_to_buy}")
        locked_nodes = []
        
        def try_lock(peer, results):
            try:
                url = self.get_peer_url(peer, "inventory/lock")
                resp = requests.post(url, json={"node_id": self.node_id, "items": items_to_buy}, timeout=2)
                if resp.status_code == 200:
                    results.append(peer)
            except:
                pass

        # Try local first
        local_locked = False
        # Simplified local lock check (index.html will call /inventory/lock on server)
        # But handle_checkout is internal, so we check node state directly
        conflict = False
        for item in items_to_buy:
            if item in self.item_locks and self.item_locks[item] != self.node_id:
                conflict = True
                break
        
        if not conflict:
            for item in items_to_buy:
                self.item_locks[item] = self.node_id
            locked_nodes.append("local")
            local_locked = True
        else:
            self.logger.warning("Local lock conflict detected")

        # Try peers
        threads = []
        peer_results = []
        for peer in self.peers:
            t = threading.Thread(target=try_lock, args=(peer, peer_results))
            threads.append(t)
            t.start()
        
        # Wait for responses
        start_time = time.time()
        # Majority is ceil((N+1)/2). If N=3 peers + 1 local = 4 nodes, majority = 3.
        # If N=2 peers + 1 local = 3 nodes, majority = 2.
        total_nodes = len(self.peers) + 1
        majority = (total_nodes // 2) + 1
        
        while (len(peer_results) + (1 if local_locked else 0)) < majority and (time.time() - start_time) < 3:
            time.sleep(0.1)
        
        total_locked = len(peer_results) + (1 if local_locked else 0)
        all_contacted_nodes = peer_results + (["local"] if local_locked else [])

        def release_locks():
            self.logger.info(f"Releasing locks for {items_to_buy}")
            # Local
            for item in items_to_buy:
                if self.item_locks.get(item) == self.node_id:
                    del self.item_locks[item]
            # Peers
            for peer in self.peers:
                def do_unlock(p):
                    try:
                        url = self.get_peer_url(p, "inventory/unlock")
                        requests.post(url, json={"node_id": self.node_id, "items": items_to_buy}, timeout=1)
                    except: pass
                threading.Thread(target=do_unlock, args=(peer,)).start()

        if total_locked < majority:
            self.logger.warning(f"Failed to acquire majority locks ({total_locked}/{majority}). Aborting checkout.")
            release_locks()
            return False, "System busy (lock conflict), please try again in a moment."

        self.logger.info(f"Majority locks acquired ({total_locked}/{majority}). Proceeding with checkout.")

        try:
            # 3. Validate inventory (Quorum Read for inventory)
            # ... rest of the existing logic ...
            # I will keep the existing logic but wrapped in this lock block
            
            # Fetch current inventory from R replicas to ensure consistency
            self.logger.info("Performing Quorum Read for Inventory validation")
            peer_inv_results = []
            threads = []
            for peer in self.peers:
                def fetch_inv_from_peer(p, results):
                    try:
                        url = self.get_peer_url(p, "inventory")
                        response = requests.get(url, timeout=2)
                        if response.status_code == 200:
                            results.append(response.json())
                    except: pass
                t = threading.Thread(target=fetch_inv_from_peer, args=(peer, peer_inv_results))
                threads.append(t)
                t.start()
            
            start_time = time.time()
            while len(peer_inv_results) < self.r - 1 and (time.time() - start_time) < 3:
                time.sleep(0.1)
                
            all_inv_versions = [self.inventory_db] + peer_inv_results
            
            latest_inventory = all_inv_versions[0]
            for i in range(1, len(all_inv_versions)):
                remote_inv = all_inv_versions[i]
                for item in remote_inv:
                    if item not in latest_inventory:
                        latest_inventory[item] = remote_inv[item]
                        continue
                    
                    vc_local = VectorClock(latest_inventory[item].get("vector_clock", {}))
                    vc_remote = VectorClock(remote_inv[item].get("vector_clock", {}))
                    cmp = vc_local.compare(vc_remote)
                    
                    if cmp == -1: 
                        latest_inventory[item] = remote_inv[item]
                    elif cmp == None: 
                        latest_inventory[item]["stock"] = max(latest_inventory[item]["stock"], remote_inv[item]["stock"])
                        latest_inventory[item]["vector_clock"] = vc_local.merge(vc_remote).to_dict()
            
            # Validate against the "Latest/Agreed" inventory
            for item, qty in cart["items"].items():
                current_stock = latest_inventory.get(item, {}).get("stock", 0)
                if current_stock < qty:
                    release_locks()
                    return False, f"Insufficient stock for {item}"
            
            self.inventory_db = latest_inventory

            # 4. Update inventory locally and replicate
            for item, qty in cart["items"].items():
                self.inventory_db[item]["stock"] -= qty
                vc = VectorClock(self.inventory_db[item].get("vector_clock", {}))
                vc.increment(self.node_id)
                self.inventory_db[item]["vector_clock"] = vc.to_dict()
            
            self.save_inventory_db()
            
            # Replicate inventory updates
            for peer in self.peers:
                def replicate_inv(p):
                    try:
                        url = self.get_peer_url(p, "replicate/inventory")
                        requests.post(url, json={"inventory": self.inventory_db}, timeout=2)
                    except: pass
                threading.Thread(target=replicate_inv, args=(peer,)).start()

            # 5. Clear cart and replicate
            self.cart_db[user_id] = {"items": {}, "vector_clock": cart["vector_clock"]}
            vc_cart = VectorClock(self.cart_db[user_id]["vector_clock"])
            vc_cart.increment(self.node_id)
            self.cart_db[user_id]["vector_clock"] = vc_cart.to_dict()
            self.save_cart_db()
            
            self.trigger_read_repair(user_id, self.cart_db[user_id])
            
            release_locks()
            return True, "Checkout successful"
            
        except Exception as e:
            self.logger.error(f"Error during checkout: {e}")
            release_locks()
            return False, f"Internal server error during checkout: {str(e)}"

    def receive_replicate_inventory(self, remote_inventory):
        for item, remote_data in remote_inventory.items():
            if item not in self.inventory_db:
                self.inventory_db[item] = remote_data
                continue
            
            vc_local = VectorClock(self.inventory_db[item].get("vector_clock", {}))
            vc_remote = VectorClock(remote_data.get("vector_clock", {}))
            
            cmp = vc_local.compare(vc_remote)
            if cmp == -1: # local is stale
                self.inventory_db[item] = remote_data
            elif cmp == None: # Conflict, we take the one with the higher stock for safety or merge VCs
                self.inventory_db[item]["stock"] = max(self.inventory_db[item]["stock"], remote_data["stock"])
                self.inventory_db[item]["vector_clock"] = vc_local.merge(vc_remote).to_dict()
        
        self.save_inventory_db()
        return True

    def sync_inventory_with_peers(self):
        """On startup, fetch inventory and quorum config from peers to catch up."""
        self.logger.info("Initializing startup peer-sync for inventory and configuration...")
        # Give a small delay for other nodes to potentially start up if launched in batch
        time.sleep(2)
        
        for peer in self.peers:
            try:
                self.logger.info(f"Syncing with peer {peer}")
                # 1. Sync Inventory
                inv_url = self.get_peer_url(peer, "inventory")
                inv_response = requests.get(inv_url, timeout=3)
                if inv_response.status_code == 200:
                    self.receive_replicate_inventory(inv_response.json())
                    self.logger.info(f"Successfully synced inventory with {peer}")

                # 2. Sync Quorum Configuration (R, W)
                state_url = self.get_peer_url(peer, "state")
                state_response = requests.get(state_url, timeout=3)
                if state_response.status_code == 200:
                    data = state_response.json()
                    new_r = data.get('r')
                    new_w = data.get('w')
                    if new_r and new_w and (new_r != self.r or new_w != self.w):
                        self.logger.info(f"Updating quorum config from peer {peer}: R={new_r}, W={new_w} (was R={self.r}, W={self.w})")
                        self.r = new_r
                        self.w = new_w
                        
            except Exception as e:
                self.logger.debug(f"Could not sync with {peer} during startup: {e}")

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
        "public_url": node.public_url,
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

@app.route('/cart/clear', methods=['POST'])
def clear_cart():
    data = request.json
    user_id = data.get('user_id')
    
    # Update local
    local_cart = node.cart_db.get(user_id, {"items": {}, "vector_clock": {}})
    vc = VectorClock(local_cart["vector_clock"])
    vc.increment(node.node_id)
    new_cart = {"items": {}, "vector_clock": vc.to_dict()}
    
    node.cart_db[user_id] = new_cart
    node.save_cart_db()
    
    # Replicate (using read repair logic)
    node.trigger_read_repair(user_id, new_cart)
    
    return jsonify({"status": "success", "message": "Cart cleared"}), 200

@app.route('/inventory', methods=['GET'])
def get_inventory():
    return jsonify(node.inventory_db), 200

@app.route('/inventory/manage', methods=['POST'])
def manage_inventory():
    """Allows UI to forcefully update or clear inventory stock levels and replicate."""
    data = request.json
    updates = data.get('updates', {}) # e.g. {"laptop": 50, "mouse": 200}
    
    for item, new_stock in updates.items():
        if item not in node.inventory_db:
             node.inventory_db[item] = {"stock": new_stock, "price": 1000, "vector_clock": {}}
        else:
             node.inventory_db[item]["stock"] = new_stock
        
        vc = VectorClock(node.inventory_db[item].get("vector_clock", {}))
        vc.increment(node.node_id)
        node.inventory_db[item]["vector_clock"] = vc.to_dict()
        
    node.save_inventory_db()
    
    # Replicate explicitly 
    for peer in node.peers:
        def replicate_inv(p):
            try:
                url = node.get_peer_url(p, "replicate/inventory")
                requests.post(url, json={"inventory": node.inventory_db}, timeout=2)
            except:
                pass
        threading.Thread(target=replicate_inv, args=(peer,)).start()
        
    return jsonify({"status": "success", "message": "Inventory updated"}), 200

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

@app.route('/peers/add', methods=['POST'])
def add_peer_endpoint():
    data = request.json
    peer_url = data.get('peer_url')
    announce_back = data.get('announce_back', True)
    if peer_url:
        added = node.add_peer(peer_url, announce_back=announce_back)
        return jsonify({"status": "ok", "added": added}), 200
    return jsonify({"status": "error", "message": "Missing peer_url"}), 400

@app.route('/inventory/lock', methods=['POST'])
def lock_items():
    data = request.json
    items = data.get('items', [])
    requester = data.get('node_id')
    
    # Check if any item is already locked by someone else
    for item in items:
        if item in node.item_locks and node.item_locks[item] != requester:
            return jsonify({"status": "locked", "item": item}), 409
            
    # Grant locks
    for item in items:
        node.item_locks[item] = requester
    node.logger.info(f"Locked items {items} for {requester}")
    return jsonify({"status": "ok"}), 200

@app.route('/inventory/unlock', methods=['POST'])
def unlock_items():
    data = request.json
    items = data.get('items', [])
    requester = data.get('node_id')
    
    for item in items:
        if node.item_locks.get(item) == requester:
            del node.item_locks[item]
            
    node.logger.info(f"Unlocked items {items} for {requester}")
    return jsonify({"status": "ok"}), 200

@app.route('/config', methods=['POST'])
def update_config():
    data = request.json
    if 'r' in data: node.r = data['r']
    if 'w' in data: node.w = data['w']
    node.logger.info(f"Updated Quorum Configuration: R={node.r}, W={node.w}")
    
    # Propagate to all other peers if 'propagate' is not explicitly False
    if data.get('propagate', True):
        for peer in node.peers:
            def replicate_config(p):
                try:
                    url = node.get_peer_url(p, "config")
                    # Set propagate=False to prevent infinite loops
                    requests.post(url, json={"r": node.r, "w": node.w, "propagate": False}, timeout=2)
                except Exception as e:
                    node.logger.error(f"Failed to propagate config to {p}: {e}")
            threading.Thread(target=replicate_config, args=(peer,)).start()
            
    return jsonify({"status": "ok", "r": node.r, "w": node.w}), 200

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--node_id", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--peers", default="", help="Comma separated list of peer host:port or ngrok-vols")
    parser.add_argument("--n", type=int, default=3)
    parser.add_argument("--r", type=int, default=2)
    parser.add_argument("--w", type=int, default=2)
    parser.add_argument("--use_ngrok", action="store_true", help="Start an ngrok tunnel")
    parser.add_argument("--ngrok_token", help="Ngrok auth token")
    
    args = parser.parse_args()
    
    public_url = None
    if args.use_ngrok:
        if args.ngrok_token:
            ngrok.set_auth_token(args.ngrok_token)
        tunnel = ngrok.connect(args.port)
        public_url = tunnel.public_url
        print(f" * Ngrok Tunnel established: {public_url}")

    peers = args.peers.split(",") if args.peers else []
    node = Node(args.node_id, args.port, peers, n=args.n, r=args.r, w=args.w, public_url=public_url)
    
    app.run(host='0.0.0.0', port=args.port)
