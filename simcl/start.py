# ==============================================================================
# start.py
# ==============================================================================

import grpc
import argparse
import gossip_pb2
import gossip_pb2_grpc
import socket
import time

def send_message_to_self(message):
    """Sends a message to the current pod (itself)."""
    host_ip = socket.gethostbyname(socket.gethostname())
    print(f"host_ip={host_ip}", flush=True)
    target = f"{host_ip}:5050"
    print(f"target={target}", flush=True)
    target_latency = 0.00

    with grpc.insecure_channel(target) as channel:
        stub = gossip_pb2_grpc.GossipServiceStub(channel)
        print(f"Sending message to self ({host_ip}): '{message}' with latency={target_latency} ms",
              flush=True)
        response = stub.SendMessage(gossip_pb2.GossipMessage(
            message=message,
            sender_id=host_ip,
            timestamp=time.time_ns(),
            latency_ms=target_latency,
            round_count=0  # <-- ADDED
        ))
        print(f"Received acknowledgment: {response.details}", flush=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Initiate gossip protocol by sending a message to self.")
    parser.add_argument('--message', required=True, help="Message to send to self")
    args = parser.parse_args()
    send_message_to_self(args.message)