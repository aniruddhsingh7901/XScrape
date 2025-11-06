#!/usr/bin/env python3
"""
public_http_server.py

Simple static file HTTP server for publishing snapshots.
- Serves files from directory specified by environment variable DIR (default: scripts/public_db)
- Listens on port specified by environment variable PORT (default: 8000)
- Binds to 0.0.0.0 so PM2 can expose locally

Usage (PM2 sets env automatically via pm2.config.js):
  PORT=8000 DIR=scripts/public_db python scripts/public_http_server.py
"""

import os
import sys
import socket
from http.server import SimpleHTTPRequestHandler
from socketserver import TCPServer

# Read configuration from environment
PORT = int(os.getenv("PORT", "8000"))
SERVE_DIR = os.getenv("DIR", "scripts/public_db")

if not os.path.isdir(SERVE_DIR):
    print(f"[ERROR] Serve directory does not exist: {SERVE_DIR}", flush=True)
    sys.exit(1)

# Change working directory so SimpleHTTPRequestHandler serves from SERVE_DIR
os.chdir(SERVE_DIR)

class QuietHandler(SimpleHTTPRequestHandler):
    # Optional: reduce default logging noise
    def log_message(self, format, *args):
        sys.stdout.write("[HTTP] " + (format % args) + "\n")
        sys.stdout.flush()

# Prefer reuse port to avoid TIME_WAIT issues on restarts
class ReusableTCPServer(TCPServer):
    allow_reuse_address = True

def main():
    addr = ("0.0.0.0", PORT)
    try:
        with ReusableTCPServer(addr, QuietHandler) as httpd:
            httpd.timeout = 2
            sa = httpd.socket.getsockname()
