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

MAX_CONCURRENT_SENDS = 125

class Node(gossip_pb2_grpc.GossipServiceServicer):
    def __init__(self, service_name):
        self.hostname = socket.gethostname()
        self.host = socket.gethostbyname(self.hostname)
        self.port = '5050'
        self.service_name = service_name
        self.susceptible_nodes = []
        self.received_message_ids = set()
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_SENDS)
        # We don't necessarily need to call get_neighbors here 
        # because lazy load will handle it, but it's fine as a fallback.
        self.get_neighbors()

    def get_neighbors(self):
        """Refreshes neighbor list from SQLite. Handles missing table gracefully."""
        try:
            conn = sqlite3.connect('ned.db', timeout=10)
            cursor = conn.execute("SELECT pod_ip, weight FROM NEIGHBORS")
            self.susceptible_nodes = [(row[0], row[1]) for row in cursor]
            conn.close()
            print(f"Refreshed: {len(self.susceptible_nodes)} neighbors found.", flush=True)
        except Exception as e:
            # Table might not exist yet if prepare.py hasn't run
            self.susceptible_nodes = []
            print(f"get_neighbors info: {e}", flush=True)

    async def UpdateNeighbors(self, request, context):
        """Force a manual refresh and clear message cache for new experiments."""
        print("Received UpdateNeighbors signal. Refreshing state...", flush=True)
        self.get_neighbors()
        self.received_message_ids.clear()
        return gossip_pb2.Acknowledgment(details="State refreshed.")

    async def SendMessage(self, request, context):
        message = request.message
        sender_id = request.sender_id
        received_timestamp = time.time_ns()
        
        # LAZY LOAD TRIGGER: If we have no neighbors, try to load them now
        if not self.susceptible_nodes:
            self.get_neighbors()

        if sender_id == self.host:
            self.received_message_ids.add(message)
            self._log_event(message, sender_id, received_timestamp, None, None, 'initiate', "Gossip Start", 0)
            await self.gossip_message(message, sender_id, 0)
            return gossip_pb2.Acknowledgment(details="Initiated")
        
        elif message in self.received_message_ids:
            self._log_event(message, sender_id, received_timestamp, None, request.latency_ms, 'duplicate', "Ignored", request.round_count)
            return gossip_pb2.Acknowledgment(details="Duplicate")
        
        else:
            self.received_message_ids.add(message)
            prop_time = (received_timestamp - request.timestamp) / 1e6
            self._log_event(message, sender_id, received_timestamp, prop_time, request.latency_ms, 'received', "New Message", request.round_count)
            await self.gossip_message(message, sender_id, request.round_count + 1)
            return gossip_pb2.Acknowledgment(details="Propagated")

    async def gossip_message(self, message, sender_id, round_count):
        # Final safety check
        if not self.susceptible_nodes:
            print("❌ No neighbors found in DB. Stopping propagation.", flush=True)
            return

        tasks = []
        for peer_ip, peer_weight in self.susceptible_nodes:
            if peer_ip != sender_id:
                tasks.append(asyncio.create_task(self._send_gossip_to_peer(message, peer_ip, peer_weight, round_count)))
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_gossip_to_peer(self, message, peer_ip, peer_weight, round_count):
        async with self.semaphore:
            try:
                await asyncio.sleep(float(peer_weight) / 1000)
                async with grpc.aio.insecure_channel(f"{peer_ip}:5050") as channel:
                    stub = gossip_pb2_grpc.GossipServiceStub(channel)
                    await stub.SendMessage(gossip_pb2.GossipMessage(
                        message=message, sender_id=self.host, timestamp=time.time_ns(),
                        latency_ms=peer_weight, round_count=round_count
                    ))
            except Exception as e:
                print(f"Error sending to {peer_ip}: {e}", flush=True)

    def _log_event(self, message, sender_id, received_timestamp, propagation_time, incoming_link_latency, event_type, detail, round_count):
        event_data = {
            'message': message, 'sender_id': sender_id, 'receiver_id': self.host,
            'received_timestamp': received_timestamp, 'propagation_time': propagation_time,
            'incoming_link_latency': incoming_link_latency, 'round_count': round_count,
            'event_type': event_type, 'detail': detail
        }
        print(json.dumps(event_data), flush=True)

    async def start_server(self):
        server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))
        gossip_pb2_grpc.add_GossipServiceServicer_to_server(self, server)
        server.add_insecure_port(f'[::]:{self.port}')
        print(f"Node listening on {self.port}", flush=True)
        await server.start()
        await server.wait_for_termination()

if __name__ == '__main__':
    asyncio.run(Node('bcgossip-svc').start_server())