import socket
import threading
import pickle
import time

class Node:
    def __init__(self, node_id, num_nodes):
        self.node_id = node_id
        self.num_nodes = num_nodes
        self.private_vector_clock = [0] * num_nodes
        self.private_message_queue = []

        self.lock = threading.Lock()

        self.private_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.private_sock.bind(("localhost", 10000 + node_id * 2 + 1))

        self.private_thread = threading.Thread(target=self.receive_private, daemon=True)

    def receive_private(self):
        while True:
            data, _ = self.private_sock.recvfrom(1024)
            packet = pickle.loads(data)
            if isinstance(packet, tuple):
                sender_id, sender_clock, message = packet
                if message == "Fine !":
                    # Handle the delay message in a separate thread
                    threading.Thread(target=self.handle_delay_message, args=(message, sender_id, sender_clock)).start()
                else :
                    # Process other messages immediately
                    self.receive_message_private(message, sender_id, sender_clock)

    def send_private_message(self, message, recipient_id):
        with self.lock:
            self.private_vector_clock[self.node_id] += 1
            packet = (self.node_id, self.private_vector_clock[:], message)
            print(f"Node {self.node_id} sending private message to Node {recipient_id}: {message}")
            self.private_sock.sendto(pickle.dumps(packet), ("localhost", 10000 + recipient_id * 2 + 1))

    def handle_delay_message(self, message, sender_id, sender_clock):
        time.sleep(5)
        print(f"Node {self.node_id} processed delay from Node {sender_id}")
        self.private_message_queue.append((message, sender_id, sender_clock))
        self.process_private_message_queue()

    def receive_message_private(self, message, sender_id, sender_clock):
        print(f"Node {self.node_id} received private message from Node {sender_id}: {message}")
        with self.lock:
            self.private_message_queue.append((message, sender_id, sender_clock))
            self.process_private_message_queue()

    def process_private_message_queue(self):
        delivered = False
        while not delivered:
            time.sleep(1)
            for msg, sender, clock in list(self.private_message_queue):
                if self.can_deliver_message(clock, sender, self.private_vector_clock):
                    self.deliver_private_message(msg, sender, clock)
                    self.private_message_queue.remove((msg, sender, clock))
                    delivered = True

    def can_deliver_message(self, sender_clock, sender_id, vector_clock):
        # print(f"Node {self.node_id} checking if message can be delivered: {sender_clock}, {sender_id}, {vector_clock}")
        for i in range(self.num_nodes):
            if i != sender_id and sender_clock[i] > vector_clock[i]:
                return False
            if i == sender_id and sender_clock[i] > max(vector_clock) +1:
                return False
        return True

    def deliver_private_message(self, message, sender_id, sender_clock):
        print(f"Node {self.node_id} delivering private message from Node {sender_id}: {message}")
        self.private_vector_clock[self.node_id] += 1
        for i in range(self.num_nodes):
            self.private_vector_clock[i] = max(self.private_vector_clock[i], sender_clock[i])
        print(f"Node {self.node_id} private vector clock after delivering: {self.private_vector_clock}")

    def start(self):
        self.private_thread.start()

    def print_vector_clocks(self):
        print(f"Node {self.node_id} private vector clock: {self.private_vector_clock}")

if __name__ == "__main__":
    num_nodes = 2
    
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

    def private_message_function(node, message, recipient_id, delay=0):
        if delay > 0:
            print(f"Node {node.node_id}: Sleeping for {delay} seconds before sending private message to Node {recipient_id}")
        time.sleep(delay)
        node.send_private_message(message, recipient_id)

    # Start threads for different message actions with specified delays
    threading.Thread(target=private_message_function, args=(nodes[0], "How are you ?", 1)).start()
    time.sleep(3)
    threading.Thread(target=private_message_function, args=(nodes[1], "Fine !", 0)).start()  # Delay message
    time.sleep(1)
    threading.Thread(target=private_message_function, args=(nodes[1], "And you ?", 0)).start()  # Delay 2 seconds to ensure it comes after "delay"
    time.sleep(6)
    threading.Thread(target=private_message_function, args=(nodes[0], "Fine, thanks", 1)).start()
    time.sleep(2)

    # Give enough time for all messages to be processed
    time.sleep(20)

    # Print final vector clocks
    for node in nodes:
        node.print_vector_clocks()
