import argparse
import http.server
import os
import sys

class MockNodeHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Override to log cleanly to stdout/stderr which gets captured in the log file
        sys.stderr.write(f"[{node_id}] {format % args}\n")

    def do_GET(self):
        if self.path.startswith("/keys/"):
            key = self.path[6:]
            filepath = os.path.join(data_dir, f"{key}.db")
            if os.path.exists(filepath):
                with open(filepath, "r") as f:
                    val = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(val.encode("utf-8"))
            else:
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"null")
        else:
            self.send_response(404)
            self.end_headers()

    def do_PUT(self):
        if self.path.startswith("/keys/"):
            key = self.path[6:]
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length)
            
            filepath = os.path.join(data_dir, f"{key}.db")
            with open(filepath, "w") as f:
                f.write(body.decode("utf-8"))
                
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--proxy", required=True)
    args = parser.parse_args()

    node_id = args.id
    port = args.port
    
    # We will use the current directory or create a subfolder for node storage
    # The runner creates the node data_dir and executes from the workspace root
    # So we can use the data directory: ".failforge/data/{run_id}/{node_id}"
    # Wait, how does node know its data_dir?
    # We can infer it or we can pass a datadir arg, but since we run from our specific target datadir or can just create it relative:
    data_dir = f".failforge/data/local/{node_id}"
    # To support dynamic datadir passed from failforge:
    # Actually, failforge YAML command specifies: "python3 node.py --id {node_id} --port {port} --proxy {proxy_url}"
    # So let's write to a folder based on node_id:
    data_dir = os.path.join(".failforge", "data", "store", node_id)
    os.makedirs(data_dir, exist_ok=True)

    sys.stderr.write(f"Starting mock node {node_id} on port {port}\n")
    server = http.server.HTTPServer(("127.0.0.1", port), MockNodeHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    sys.stderr.write(f"Stopping mock node {node_id}\n")
