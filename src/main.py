"""
Main entry point for the LSM-Tree Key/Value storage system.
Starts the HTTP API server with all required endpoints.
"""

import sys
import signal
import argparse
from server.api_server import KVAPIServer
from config import DEFAULT_HOST, DEFAULT_PORT, DATA_DIR


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    print("\nReceived shutdown signal. Stopping server...")
    if 'server' in globals():
        server.stop()
    sys.exit(0)


def main():
    """Main function to start the server"""
    parser = argparse.ArgumentParser(description='LSM-Tree Key/Value Storage System')
    parser.add_argument('--host', default=DEFAULT_HOST, help=f'Host to bind to (default: {DEFAULT_HOST})')
    parser.add_argument('--port', type=int, default=DEFAULT_PORT, help=f'Port to bind to (default: {DEFAULT_PORT})')
    parser.add_argument('--data-dir', default=DATA_DIR, help=f'Data directory (default: {DATA_DIR})')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and start server
    global server
    server = KVAPIServer(args.host, args.port, args.data_dir)
    
    try:
        server.start()
        
        # Keep main thread alive
        print("Server is running. Press Ctrl+C to stop.")
        while True:
            import time
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        server.stop()


if __name__ == '__main__':
    main()
