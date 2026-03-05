# Deployment Guide: 4-Laptop Physical Distributed Network

To demonstrate this system running on 4 distinct computers, follow these instructions to launch a node on each machine.

## Prerequisites
1. Ensure all 4 laptops are connected to the **same Wi-Fi network**.
2. Find the IP Address of each laptop.
    - **Windows**: Open Command Prompt and run `ipconfig`. Look for "IPv4 Address" (e.g., `192.168.1.10`).
    - **Mac/Linux**: Open Terminal and run `ifconfig` or `ip a`.
3. Install Python 3 on all machines.
4. Copy the project folder (`dscasestudy` containing `node.py`, `vector_clock.py`, and the `templates/` folder) to all 4 laptops.
5. Install the required dependency on all laptops: `pip install flask requests`.

## Network Configuration Example
Assume the following IP addresses for your 4 laptops:
- **Laptop A**: `192.168.1.100` (Node 1)
- **Laptop B**: `192.168.1.101` (Node 2)
- **Laptop C**: `192.168.1.102` (Node 3)
- **Laptop D**: `192.168.1.103` (Node 4)

We will use port `5000` consistently across all machines.

## Launching the Nodes
Open a terminal inside the project directory on each respective laptop and run the following commands:

### On Laptop A
```bash
python node.py --node_id node1 --port 5000 --peers 192.168.1.101:5000,192.168.1.102:5000,192.168.1.103:5000 --n 4 --w 3 --r 2
```

### On Laptop B
```bash
python node.py --node_id node2 --port 5000 --peers 192.168.1.100:5000,192.168.1.102:5000,192.168.1.103:5000 --n 4 --w 3 --r 2
```

### On Laptop C
```bash
python node.py --node_id node3 --port 5000 --peers 192.168.1.100:5000,192.168.1.101:5000,192.168.1.103:5000 --n 4 --w 3 --r 2
```

### On Laptop D
```bash
python node.py --node_id node4 --port 5000 --peers 192.168.1.100:5000,192.168.1.101:5000,192.168.1.102:5000 --n 4 --w 3 --r 2
```

## Accessing the Dashboard UI
Once the nodes are running, you can open a web browser on **ANY** of the computers (or even a mobile phone connected to the same Wi-Fi) to view the dashboards for each node visually:

- **Node 1 Dashboard**: `http://192.168.1.100:5000`
- **Node 2 Dashboard**: `http://192.168.1.101:5000`
- **Node 3 Dashboard**: `http://192.168.1.102:5000`
- **Node 4 Dashboard**: `http://192.168.1.103:5000`

### Demonstration Workflow
1. Open the Node 1 and Node 2 dashboards side-by-side.
2. Use the "Add to Cart" form on the Node 1 dashboard. Wait less than a second.
3. Observe the cart and vector clocks automatically update on the Node 2 dashboard.
4. Experiment with changing `r` and `w` values dynamically from the dashboard.
5. Optional: Shut down Laptop D and demonstrate writes continuing reliably using the Quorum mechanism!
