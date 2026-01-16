from kubernetes import client, config
import grpc
import os
import socket
from concurrent import futures
import gossip_pb2
import gossip_pb2_grpc
import json
import time
import sqlite3

# Inspired from k8sv2
class Node(gossip_pb2_grpc.GossipServiceServicer):

    def __init__(self, service_name):
        self.hostname = socket.gethostname()
        self.host = socket.gethostbyname(self.hostname)
        self.port = '5050'
        self.service_name = service_name
        self.app_name = 'bcgossip'
        # List to keep track of IPs of neighboring nodes
        self.susceptible_nodes = []
        # Set to keep track of messages that have been received to prevent loops
        self.received_message = ""
        # self.gossip_initiated = False

    def get_neighbors(self):

        try:
            # Connecting to sqlite
            conn = sqlite3.connect('ned.db')

            # SELECT table
            # cursor = conn.execute("SELECT pod_ip from NEIGHBORS")
            # SELECT both pod_ip and weight from NEIGHBORS table
            cursor = conn.execute("SELECT pod_ip, weight from NEIGHBORS")

            # Clear the existing list to refresh it
            self.susceptible_nodes = []

            for row in cursor:
                # self.susceptible_nodes.append(row[0])
                self.susceptible_nodes.append((row[0],row[1]))
            # print(f"self.susceptible_nodes: {self.susceptible_nodes}",flush=True)
            conn.close()

        # except sqlite3.Error as e:
        #     print(f"SQLite error: {e}")
        except sqlite3.Error as e:
            print(f"SQLite error in get_neighbors: {e}", flush=True)
        except Exception as e:
            print(f"Unexpected error in get_neighbors: {str(e)}", flush=True)

    def SendMessage(self, request, context):

        """
        Receiving message from other nodes
        and distribute it to others (multi rounds gossip)
        """
        message = request.message
        sender_id = request.sender_id
        received_timestamp = time.time_ns()

        # Get latency info of each gossip
        # 0.00ms for self initiated message
        # Depends on the latency of the current neighbor latency info
        received_latency = request.latency_ms

        # For initiating acknowledgment only
        if sender_id == self.host:
            self.received_message = message
            log_message = (f"Gossip initiated by {self.hostname} ({self.host}) at "
                           f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(received_timestamp / 1e9))}"
                           f"with no latency: {received_latency} ms")
            self._log_event(message, sender_id, received_timestamp, None,
                            received_latency,'initiate',log_message)
            self.gossip_message(message, sender_id)
            return gossip_pb2.Acknowledgment(details=f"Done propagate! {self.host} received: '{message}'")

        # Check whether the message is already received ot not
        # Notify whether accept it or ignore it
        elif self.received_message == message:
            log_message = f"{self.host} ignoring duplicate message: {message} from {sender_id}"
            self._log_event(message, sender_id, received_timestamp, None,
                            received_latency,'duplicate', log_message)
            return gossip_pb2.Acknowledgment(details=f"Duplicate message ignored by ({self.host})")
        else:
            self.received_message = message
            propagation_time = (received_timestamp - request.timestamp) / 1e6
            log_message = (f"({self.hostname}({self.host}) received: '{message}' from {sender_id}"
                           f" in {propagation_time:.2f} ms ")
            self._log_event(message, sender_id, received_timestamp, propagation_time,
                            received_latency,'received', log_message)
            # Start gossip only when the node is the gossip initiator itself
            # therefore, only one iteration is required

            # gossip to its neighbor (if this pod is not the initiator)
            self.gossip_message(message, sender_id)

            return gossip_pb2.Acknowledgment(details=f"{self.host} received: '{message}'")

    def gossip_message(self, message, sender_ip):
        """
        Propagates the message to susceptible (neighboring) nodes.
        Excludes the sender from forwarding.
        """
        # This function objective is to send message to all neighbor nodes.
        # In real environment, suppose we should get latency from
        # networking tools such as iperf. But it will be included in
        # future work. For the sake of this simulation, we will get
        # neighbor latency based by providing delay using the pre-defined
        # latency value. Formula: time.sleep(latency_ms/1000)
        # For now, the weight is latency

        # Refresh list of neighbors before gossiping to capture any changes
        # Ensure neighbors are loaded if the list is empty at this point
        if not self.susceptible_nodes:
            self.get_neighbors()
            print(f"self.susceptible_nodes: {self.susceptible_nodes}", flush=True)

        # Loop through (peer_ip, peer_weight) tuples
        for peer_ip, peer_weight in self.susceptible_nodes:
            # Exclude the sender from the list of nodes to forward the message to
            if peer_ip != sender_ip:

                # Record the send timestamp
                send_timestamp = time.time_ns()

                # Introduce latency here
                time.sleep(int(peer_weight) / 1000)

                try:
                    # Establish gRPC channel to the peer
                    # The weight (peer_weight) is not part of the message payload as per current proto
                    with grpc.insecure_channel(f"{peer_ip}:5050") as channel:
                        stub = gossip_pb2_grpc.GossipServiceStub(channel)
                        stub.SendMessage(gossip_pb2.GossipMessage(
                            message=message,
                            sender_id=self.host,  # This node's IP is the sender for the next hop
                            timestamp=send_timestamp,
                            latency_ms=peer_weight  # Pass the weight of the link as latency_ms (in milisecond)
                        ))
                except grpc.RpcError as e:
                    print(f"Failed to send message: '{message}' to {peer_ip}: {e.code()} - {e.details()}", flush=True)
                except Exception as e:
                    print(f"Unexpected error when sending message to {peer_ip}: {str(e)}", flush=True)

    def _log_event(self, message, sender_id, received_timestamp, propagation_time, latency_ms, event_type, log_message):
        """Logs the gossip event as structured JSON data."""
        event_data = {
            'message': message,
            'sender_id': sender_id,
            'receiver_id': self.host,
            'received_timestamp': received_timestamp,
            'propagation_time': propagation_time,
            'latency_ms': latency_ms,
            'event_type': event_type,
            'detail': log_message
        }

        # Print both the log message and the JSON data to the console
        print(json.dumps(event_data), flush=True)

    def start_server(self):
        """ Initiating server """
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        gossip_pb2_grpc.add_GossipServiceServicer_to_server(self, server)
        server.add_insecure_port(f'[::]:{self.port}')
        print(f"{self.hostname}({self.host}) listening on port {self.port}", flush=True)
        server.start()
        server.wait_for_termination()

def run_server():
    service_name = os.getenv('SERVICE_NAME', 'bcgossip-svc')
    node = Node(service_name)
    node.start_server()

if __name__ == '__main__':
    run_server()