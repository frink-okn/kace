# Copyright Onai Inc.
import argparse
import socket
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization


def generate_keys():
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend())
    public_key = private_key.public_key()
    return public_key, private_key


def send_graph_committment(commit_id, graph_name, committer_name):
    cname = committer_name.decode('utf-8')
    private_key = committer_keys[cname]["sk"]
    public_key = committer_keys[cname]["pk"]

    signature = private_key.sign(
        commit_id,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH),
        hashes.SHA256())

    public_key_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )

    # Create a TCP socket
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Connect to the server
    client_socket.connect((SERVER_HOST, SERVER_PORT))
    print(f"[*] Connected to {SERVER_HOST}:{SERVER_PORT}")

    # Send data to the server
    message = b", ".join([commit_id, graph_name, signature, public_key_pem, committer_name])
    client_socket.sendall(message)

    # Receive data from the server
    data = client_socket.recv(1024)
    print(f"[*] Received from server: {data}")

    # Close the connection
    client_socket.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit-id", help="Commit id")
    parser.add_argument("--graph-name", help="graph name")
    parser.add_argument("--committer-name", help="committer name")
    parser.add_argument("--ip", help="spider server")
    parser.add_argument("--port", help="spider server")

    args = parser.parse_args()

    commit_id = args.commit_id.encode("utf-8")
    graph_name = args.graph_name.encode("utf-8")
    committer_name = args.committer_name.encode("utf-8")

    # Define server address and port
    SERVER_HOST = args.ip
    SERVER_PORT = int(args.port)

    committer_keys = {"admin": {"pk": "", "sk": ""}}

    pk, sk = generate_keys()
    committer_keys["admin"]["pk"] = pk
    committer_keys["admin"]["sk"] = sk

    send_graph_committment(commit_id,
                           graph_name,
                           committer_name)