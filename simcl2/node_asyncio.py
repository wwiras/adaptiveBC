# ==============================================================================
# node_asyncio.py
# ==============================================================================

import asyncio
import grpc.aio
import os
import socket
from concurrent import futures
import gossip_pb2
import gossip_pb2_grpc
import json
import time
import sqlite3
from google.protobuf.empty_pb2 import Empty


# Define a constant for max concurrent outbound gRPC calls
# MAX_CONCURRENT_SENDS = 10 # v1
# MAX_CONCURRENT_SENDS = 20 # v2
# MAX_CONCURRENT_SENDS = 30 # v3
# MAX_CONCURRENT_SENDS = 40 # v4
# MAX_CONCURRENT_SENDS = 50 # v5
# MAX_CONCURRENT_SENDS = 60 # v6
# MAX_CONCURRENT_SENDS = 70 # v7
# MAX_CONCURRENT_SENDS = 80 # v8
# MAX_CONCURRENT_SENDS = 90 # v9
# MAX_CONCURRENT_SENDS = 100 # v10
# MAX_CONCURRENT_SENDS = 110 # # v11
# MAX_CONCURRENT_SENDS = 120 # v12
# MAX_CONCURRENT_SENDS = 130 # v13
MAX_CONCURRENT_SENDS = 125 # v14


class Node(gossip_pb2_grpc.GossipServiceServicer):

    def __init__(self, service_name):
        self.hostname = socket.gethostname()
        self.host = socket.gethostbyname(self.hostname)
        self.port = '5050'
        self.service_name = service_name
        self.app_name = 'bcgossip'
        self.susceptible_nodes = []
        self.received_message_ids = set()
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_SENDS)

        self.get_neighbors()

    def get_neighbors(self):
        try:
            conn = sqlite3.connect('ned.db')
            cursor = conn.execute("SELECT pod_ip, weight from NEIGHBORS")
            self.susceptible_nodes = []
            for row in cursor:
                self.susceptible_nodes.append((row[0], row[1]))
            conn.close()
            print(f"Neighbors list refreshed from ned.db. Found {len(self.susceptible_nodes)} neighbors.", flush=True)
        except Exception as e:
            print(f"Error in get_neighbors: {e}", flush=True)

    async def UpdateNeighbors(self, request, context):
        print("Received UpdateNeighbors signal. Refreshing state...", flush=True)
        self.get_neighbors()
        self.received_message_ids.clear()
        print(f"Message cache cleared. New topology active.", flush=True)
        return gossip_pb2.Acknowledgment(details="Neighbors list and message cache have been updated.")

    async def SendMessage(self, request, context):
        """
        Receiving message from other nodes and distributing it.
        """
        message = request.message
        sender_id = request.sender_id
        received_timestamp = time.time_ns()
        incoming_link_latency = request.latency_ms
        incoming_round_count = request.round_count  # <-- ADDED

        if sender_id == self.host:
            self.received_message_ids.add(message)
            log_message = (f"Gossip initiated by {self.hostname} ({self.host}) at "
                           f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(received_timestamp / 1e9))}")
            self._log_event(message, sender_id, received_timestamp, None,
                            None, 'initiate', log_message, 0)  # <-- ADDED
            await self.gossip_message(message, sender_id, 0)
            return gossip_pb2.Acknowledgment(details=f"Done propagate! {self.host} received: '{message}'")
        
        elif message in self.received_message_ids:
            log_message = f"{self.host} ignoring duplicate message: {message} from {sender_id}"
            self._log_event(message, sender_id, received_timestamp, None,
                            incoming_link_latency, 'duplicate', log_message, incoming_round_count) # <-- ADDED
            return gossip_pb2.Acknowledgment(details=f"Duplicate message ignored by ({self.host})")
        else:
            self.received_message_ids.add(message)
            propagation_time = (received_timestamp - request.timestamp) / 1e6
            log_message = (f"({self.hostname}({self.host}) received: '{message}' from {sender_id}"
                           f" in {propagation_time:.2f} ms. Incoming link latency: {incoming_link_latency:.2f} ms")
            self._log_event(message, sender_id, received_timestamp, propagation_time,
                            incoming_link_latency, 'received', log_message, incoming_round_count) # <-- ADDED

            new_round_count = incoming_round_count + 1
            
            await self.gossip_message(message, sender_id, new_round_count)
            
            return gossip_pb2.Acknowledgment(details=f"{self.host} received: '{message}'")

    async def _send_gossip_to_peer(self, message, sender_id, peer_ip, peer_weight, round_count): # <-- ADDED
        send_timestamp = time.time_ns()
        async with self.semaphore:
            try:
                await asyncio.sleep(int(peer_weight) / 1000)
                async with grpc.aio.insecure_channel(f"{peer_ip}:5050") as channel:
                    stub = gossip_pb2_grpc.GossipServiceStub(channel)
                    await stub.SendMessage(gossip_pb2.GossipMessage(
                        message=message,
                        sender_id=self.host,
                        timestamp=send_timestamp,
                        latency_ms=peer_weight,
                        round_count=round_count # <-- ADDED
                    ))
            except Exception as e:
                print(f"Failed to send message: '{message}' to {peer_ip}: {str(e)}", flush=True)
                
    async def gossip_message(self, message, sender_id, round_count=0): # <-- ADDED
        if not self.susceptible_nodes:
            self.get_neighbors()
        
        tasks = []
        for peer_ip, peer_weight in self.susceptible_nodes:
            if peer_ip != sender_id:
                task = asyncio.create_task(self._send_gossip_to_peer(message, sender_id, peer_ip, peer_weight, round_count)) # <-- ADDED
                tasks.append(task)
        
        await asyncio.gather(*tasks, return_exceptions=True)

    def _log_event(self, message, sender_id, received_timestamp, propagation_time, incoming_link_latency, event_type,
                   log_message, round_count): # <-- ADDED
        event_data = {
            'message': message,
            'sender_id': sender_id,
            'receiver_id': self.host,
            'received_timestamp': received_timestamp,
            'propagation_time': propagation_time,
            'incoming_link_latency': incoming_link_latency,
            'round_count': round_count, # <-- ADDED
            'event_type': event_type,
            'detail': log_message
        }
        print(json.dumps(event_data), flush=True)

    async def start_server(self):
        server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))
        gossip_pb2_grpc.add_GossipServiceServicer_to_server(self, server)
        server.add_insecure_port(f'[::]:{self.port}')
        print(f"{self.hostname}({self.host}) listening on port {self.port}", flush=True)
        await server.start()
        await server.wait_for_termination()


async def run_server_async():
    service_name = os.getenv('SERVICE_NAME', 'bcgossip-svc')
    node = Node(service_name)
    await node.start_server()


if __name__ == '__main__':
    asyncio.run(run_server_async())