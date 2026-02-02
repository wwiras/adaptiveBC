import asyncio
import grpc.aio
import os
import socket
import time
import sqlite3
import json
from concurrent import futures
import gossip_pb2
import gossip_pb2_grpc

MAX_CONCURRENT_SENDS = 125

class Node(gossip_pb2_grpc.GossipServiceServicer):
    
    def __init__(self, service_name):
        self.hostname = socket.gethostname()
        self.host = socket.gethostbyname(self.hostname)
        self.port = '5050'
        self.service_name = service_name
        self.app_name = 'bcgossip'  # Restored
        self.susceptible_nodes = []
        self.received_message_ids = set()
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_SENDS)
        self.get_neighbors_from_db()

    def get_neighbors_from_db(self):
        """Initial load from DB on startup."""
        try:
            if os.path.exists('ned.db'):
                conn = sqlite3.connect('ned.db')
                cursor = conn.execute("SELECT pod_ip, weight from NEIGHBORS")
                self.susceptible_nodes = [(row[0], row[1]) for row in cursor]
                conn.close()
                print(f"Loaded {len(self.susceptible_nodes)} neighbors from DB.", flush=True)
        except Exception as e:
            print(f"Error in get_neighbors: {e}", flush=True)

    async def UpdateNeighbors(self, request, context):
        """ONE-STEP UPDATE: Updates memory, clears cache, and saves to DB."""
        try:
            self.susceptible_nodes = [(n.pod_ip, n.weight) for n in request.neighbors]
            self.received_message_ids.clear()
            
            with sqlite3.connect('ned.db') as conn:
                conn.execute('BEGIN TRANSACTION')
                conn.execute('DROP TABLE IF EXISTS NEIGHBORS')
                conn.execute('CREATE TABLE NEIGHBORS (pod_ip TEXT PRIMARY KEY, weight REAL)')
                conn.executemany('INSERT INTO NEIGHBORS VALUES (?, ?)', self.susceptible_nodes)
                conn.commit()

            print(f"Neighbors list refreshed. Found {len(self.susceptible_nodes)} neighbors.", flush=True)
            print(f"Message cache cleared. New topology active.", flush=True)
            return gossip_pb2.Acknowledgment(details="Neighbors list and message cache have been updated.")
        except Exception as e:
            print(f"Update Error: {e}", flush=True)
            return gossip_pb2.Acknowledgment(details=f"Error: {str(e)}")

    async def SendMessage(self, request, context):
        """Receiving message from other nodes and distributing it with logging."""
        message = request.message
        sender_id = request.sender_id
        received_timestamp = time.time_ns()
        incoming_link_latency = request.latency_ms
        incoming_round_count = request.round_count

        # Case 1: Initial message (node sending to itself to start gossip)
        if sender_id == self.host:
            self.received_message_ids.add(message)
            log_msg = (f"Gossip initiated by {self.hostname} ({self.host}) at "
                       f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(received_timestamp / 1e9))}")
            self._log_event(message, sender_id, received_timestamp, None, None, 'initiate', log_msg, 0)
            asyncio.create_task(self.gossip_message(message, sender_id, 0))
            return gossip_pb2.Acknowledgment(details=f"Done propagate! {self.host} received: '{message}'")
        
        # Case 2: Duplicate message
        elif message in self.received_message_ids:
            log_msg = f"{self.host} ignoring duplicate message: {message} from {sender_id}"
            self._log_event(message, sender_id, received_timestamp, None, incoming_link_latency, 'duplicate', log_msg, incoming_round_count)
            return gossip_pb2.Acknowledgment(details=f"Duplicate message ignored by ({self.host})")
        
        # Case 3: New message
        else:
            self.received_message_ids.add(message)
            propagation_time = (received_timestamp - request.timestamp) / 1e6
            log_msg = (f"({self.hostname}({self.host}) received: '{message}' from {sender_id}"
                       f" in {propagation_time:.2f} ms. Incoming link latency: {incoming_link_latency:.2f} ms")
            self._log_event(message, sender_id, received_timestamp, propagation_time, incoming_link_latency, 'received', log_msg, incoming_round_count)

            new_round_count = incoming_round_count + 1
            asyncio.create_task(self.gossip_message(message, sender_id, new_round_count))
            return gossip_pb2.Acknowledgment(details=f"{self.host} received: '{message}'")

    async def _send_gossip_to_peer(self, message, sender_id, peer_ip, peer_weight, round_count):
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
                        round_count=round_count
                    ))
            except Exception as e:
                print(f"Failed to send message: '{message}' to {peer_ip}: {str(e)}", flush=True)

    async def gossip_message(self, message, sender_id, round_count=0):
        tasks = []
        for peer_ip, peer_weight in self.susceptible_nodes:
            if peer_ip != sender_id:
                task = asyncio.create_task(self._send_gossip_to_peer(message, sender_id, peer_ip, peer_weight, round_count))
                tasks.append(task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _log_event(self, message, sender_id, received_timestamp, propagation_time, incoming_link_latency, event_type, log_message, round_count):
        """Restored structured JSON logging."""
        event_data = {
            'message': message,
            'sender_id': sender_id,
            'receiver_id': self.host,
            'received_timestamp': received_timestamp,
            'propagation_time': propagation_time,
            'incoming_link_latency': incoming_link_latency,
            'round_count': round_count,
            'event_type': event_type,
            'detail': log_message
        }
        print(json.dumps(event_data), flush=True)

    async def start_server(self):
        server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=100))
        gossip_pb2_grpc.add_GossipServiceServicer_to_server(self, server)
        server.add_insecure_port(f'[::]:{self.port}')
        print(f"{self.hostname}({self.host}) listening on port {self.port}", flush=True)
        await server.start()
        await server.wait_for_termination()

if __name__ == '__main__':
    asyncio.run(Node(os.getenv('SERVICE_NAME', 'bcgossip')).start_server())