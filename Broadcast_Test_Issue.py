import socket
import threading
import pickle
import time

class Node:
    def __init__(self, node_id, num_nodes):
        self.node_id = node_id
        self.num_nodes = num_nodes
        self.broadcast_vector_clock = [0] * num_nodes
        self.broadcast_message_queue = []

        self.lock = threading.Lock()

        self.broadcast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.broadcast_sock.bind(("localhost", 10000 + node_id * 2))

        self.broadcast_thread = threading.Thread(target=self.receive_broadcast, daemon=True)

    def receive_broadcast(self):
        while True:
            data, _ = self.broadcast_sock.recvfrom(1024)
            packet = pickle.loads(data)
            if isinstance(packet, tuple):
                sender_id, sender_clock, message = packet
                # print(f"Node {self.node_id} received broadcast message from Node {sender_id}: {message}")
                if message == "Fine !":
                    # Handle the delay message in a separate thread
                    threading.Thread(target=self.handle_delay_message, args=(message, sender_id, sender_clock)).start()
                else:
                    # Process other messages immediately
                    self.receive_message_broadcast(message, sender_id, sender_clock)

    def broadcast_message(self, message):
        with self.lock:
            self.broadcast_vector_clock[self.node_id] += 1
            packet = (self.node_id, self.broadcast_vector_clock[:], message)
            print(f"Node {self.node_id} broadcasting message: {message}")
            for i in range(self.num_nodes):
                if i != self.node_id:
                    self.broadcast_sock.sendto(pickle.dumps(packet), ("localhost", 10000 + i * 2))

    def handle_delay_message(self, message, sender_id, sender_clock):
        time.sleep(5)
        print(f"Node {self.node_id} processed delay from Node {sender_id}")
        self.broadcast_message_queue.append((message, sender_id, sender_clock))
        self.process_broadcast_message_queue()

    def receive_message_broadcast(self, message, sender_id, sender_clock):
        print(f"Node {self.node_id} received broadcast message from Node {sender_id}: {message}")
        self.broadcast_message_queue.append((message, sender_id, sender_clock))
        self.process_broadcast_message_queue()

    def process_broadcast_message_queue(self):
        delivered = False
        while not delivered:
            time.sleep(1)
            for msg, sender, clock in list(self.broadcast_message_queue):
                if self.can_deliver_message(clock, sender, self.broadcast_vector_clock):
                    self.deliver_broadcast_message(msg, sender, clock)
                    self.broadcast_message_queue.remove((msg, sender, clock))
                    delivered = True

    def can_deliver_message(self, sender_clock, sender_id, vector_clock):
        for i in range(self.num_nodes):
            if i != sender_id and sender_clock[i] > vector_clock[i]:
                return False
            if i == sender_id and sender_clock[i] > max(vector_clock) + 1:
                return False
        return True

    def deliver_broadcast_message(self, message, sender_id, sender_clock):
        print(f"Node {self.node_id} delivering broadcast message from Node {sender_id}: {message}")
        self.broadcast_vector_clock[self.node_id] += 1
        for i in range(self.num_nodes):
            self.broadcast_vector_clock[i] = max(self.broadcast_vector_clock[i], sender_clock[i])

    def start(self):
        self.broadcast_thread.start()

    def print_vector_clocks(self):
        print(f"Node {self.node_id} broadcast vector clock: {self.broadcast_vector_clock}")

if __name__ == "__main__":
    num_nodes = 3
    
    # Create nodes with specified delays
    nodes = [Node(i, num_nodes) for i in range(num_nodes)]
    
    # Start all nodes
    for node in nodes:
        node.start()

    # Delay to let the nodes start receiving messages
    time.sleep(1)

    # Define the functions for sending messages with delays
    def broadcast_function(node, message, delay=0):
        if delay > 0:
            print(f"Node {node.node_id}: Sleeping for {delay} seconds before broadcasting '{message}'")
        time.sleep(delay)
        node.broadcast_message(message)

    # Start threads for different message actions with specified delays
    threading.Thread(target=broadcast_function, args=(nodes[0], "How are you ?", 0)).start()
    time.sleep(3)
    threading.Thread(target=broadcast_function, args=(nodes[1], "Fine !", 0)).start()  # Delay message
    time.sleep(1)
    threading.Thread(target=broadcast_function, args=(nodes[1], "And you ?", 0)).start()  # Delay 2 seconds to ensure it comes after "delay"
    time.sleep(1)
    # threading.Thread(target=broadcast_function, args=(nodes[2], "Hello from Node 2", 0)).start()
    # time.sleep(1)
    # threading.Thread(target=broadcast_function, args=(nodes[3], "Hello from Node 3", 0)).start()

    # Give enough time for all messages to be processed
    time.sleep(20)

    # Print final vector clocks
    for node in nodes:
        node.print_vector_clocks()
