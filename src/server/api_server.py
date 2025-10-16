"""
HTTP/REST API server for the LSM-Tree Key/Value storage system.
Provides RESTful endpoints for all required operations.
"""

import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from typing import Dict, Any
from storage.lsm_engine import LSMEngine
from storage.compaction import CompactionManager
from config import DEFAULT_HOST, DEFAULT_PORT, MAX_REQUEST_SIZE, DATA_DIR


class KVAPIHandler(BaseHTTPRequestHandler):
    """HTTP request handler for Key/Value API"""
    
    def __init__(self, engine: LSMEngine, compaction_manager: CompactionManager, *args, **kwargs):
        self.engine = engine
        self.compaction_manager = compaction_manager
        super().__init__(*args, **kwargs)
    
    def do_PUT(self) -> None:
        """Handle PUT requests for key-value pairs"""
        try:
            parsed_url = urlparse(self.path)
            
            if parsed_url.path == '/kv/put':
                self._handle_put()
            else:
                self._send_error_response(404, "Not found")
                
        except Exception as e:
            self._send_error_response(500, f"Internal server error: {str(e)}")
    
    def _handle_put(self) -> None:
        """Handle PUT operation"""
        # Read request body
        content_length = int(self.headers.get('Content-Length', 0))
        if content_length > MAX_REQUEST_SIZE:
            self._send_error_response(413, "Request too large")
            return
        
        body = self.rfile.read(content_length)
        
        try:
            data = json.loads(body.decode('utf-8'))
        except json.JSONDecodeError:
            self._send_error_response(400, "Invalid JSON")
            return
        
        if 'key' not in data or 'value' not in data:
            self._send_error_response(400, "key and value are required")
            return
        
        # Convert to bytes
        key_text = data['key']
        value_text = data['value']
        key = key_text.encode('utf-8')
        value = value_text.encode('utf-8')
        
        # Store key-value pair
        self.engine.put(key, value)
        
        # Send success response
        self._send_json_response(200, {"status": "success", "message": "Key stored successfully"})
    
    def do_GET(self) -> None:
        """Handle GET requests for key lookups and range scans"""
        try:
            parsed_url = urlparse(self.path)
            
            if parsed_url.path == '/kv/get':
                # Single key lookup
                self._handle_key_lookup()
            elif parsed_url.path == '/kv/range':
                # Range scan
                self._handle_range_scan()
            elif parsed_url.path == '/health':
                # Health check
                self._handle_health_check()
            elif parsed_url.path == '/stats':
                # Statistics
                self._handle_stats()
            else:
                self._send_error_response(404, "Not found")
                
        except Exception as e:
            self._send_error_response(500, f"Internal server error: {str(e)}")
    
    def do_POST(self) -> None:
        """Handle POST requests for batch operations"""
        try:
            if self.path == '/kv/batch':
                self._handle_batch_put()
            else:
                self._send_error_response(404, "Not found")
                
        except Exception as e:
            self._send_error_response(500, f"Internal server error: {str(e)}")
    
    def do_DELETE(self) -> None:
        """Handle DELETE requests for key deletion"""
        try:
            parsed_url = urlparse(self.path)
            
            if parsed_url.path == '/kv/delete':
                self._handle_delete()
            else:
                self._send_error_response(404, "Not found")
                
        except Exception as e:
            self._send_error_response(500, f"Internal server error: {str(e)}")
    
    def _handle_delete(self) -> None:
        """Handle DELETE operation"""
        # Read request body
        content_length = int(self.headers.get('Content-Length', 0))
        if content_length > MAX_REQUEST_SIZE:
            self._send_error_response(413, "Request too large")
            return
        
        body = self.rfile.read(content_length)
        
        try:
            data = json.loads(body.decode('utf-8'))
        except json.JSONDecodeError:
            self._send_error_response(400, "Invalid JSON")
            return
        
        if 'key' not in data:
            self._send_error_response(400, "key is required")
            return
        
        # Convert to bytes
        key_text = data['key']
        key = key_text.encode('utf-8')
        
        # Delete key
        deleted = self.engine.delete(key)
        
        if deleted:
            self._send_json_response(200, {"status": "success", "message": "Key deleted successfully"})
        else:
            self._send_json_response(404, {"status": "not_found", "message": "Key not found"})
    
    def _handle_key_lookup(self) -> None:
        """Handle single key lookup"""
        # Read request body
        content_length = int(self.headers.get('Content-Length', 0))
        if content_length > MAX_REQUEST_SIZE:
            self._send_error_response(413, "Request too large")
            return
        
        body = self.rfile.read(content_length)
        
        try:
            data = json.loads(body.decode('utf-8'))
        except json.JSONDecodeError:
            self._send_error_response(400, "Invalid JSON")
            return
        
        if 'key' not in data:
            self._send_error_response(400, "key is required")
            return
        
        # Convert to bytes
        key_text = data['key']
        key = key_text.encode('utf-8')
        
        # Lookup key
        value = self.engine.get(key)
        
        if value is not None:
            # Decode value as text for JSON response
            value_text = value.decode('utf-8')
            self._send_json_response(200, {
                "status": "success",
                "key": key_text,
                "value": value_text
            })
        else:
            self._send_json_response(404, {
                "status": "not_found",
                "key": key_text,
                "message": "Key not found"
            })
    
    def _handle_range_scan(self) -> None:
        """Handle range scan requests"""
        # Read request body
        content_length = int(self.headers.get('Content-Length', 0))
        if content_length > MAX_REQUEST_SIZE:
            self._send_error_response(413, "Request too large")
            return
        
        body = self.rfile.read(content_length)
        
        try:
            data = json.loads(body.decode('utf-8'))
        except json.JSONDecodeError:
            self._send_error_response(400, "Invalid JSON")
            return
        
        if 'start' not in data or 'end' not in data:
            self._send_error_response(400, "start and end keys are required")
            return
        
        # Convert to bytes
        start_key_text = data['start']
        end_key_text = data['end']
        start_key = start_key_text.encode('utf-8')
        end_key = end_key_text.encode('utf-8')
        
        # Perform range scan
        results = []
        for key, value in self.engine.range_scan(start_key, end_key):
            key_text = key.decode('utf-8')
            if value is not None:
                value_text = value.decode('utf-8')
                results.append({"key": key_text, "value": value_text})
            # Skip tombstones (deleted keys) - don't include them in results
        
        self._send_json_response(200, {
            "status": "success",
            "count": len(results),
            "results": results
        })
    
    def _handle_batch_put(self) -> None:
        """Handle batch put requests"""
        # Read request body
        content_length = int(self.headers.get('Content-Length', 0))
        if content_length > MAX_REQUEST_SIZE:
            self._send_error_response(413, "Request too large")
            return
        
        body = self.rfile.read(content_length)
        
        try:
            data = json.loads(body.decode('utf-8'))
        except json.JSONDecodeError:
            self._send_error_response(400, "Invalid JSON")
            return
        
        if 'keys' not in data or 'values' not in data:
            self._send_error_response(400, "keys and values arrays are required")
            return
        
        keys_data = data['keys']
        values_data = data['values']
        
        if len(keys_data) != len(values_data):
            self._send_error_response(400, "keys and values arrays must have the same length")
            return
        
        # Convert keys and values to bytes (assume they are text)
        try:
            keys = [k.encode('utf-8') for k in keys_data]
            values = [v.encode('utf-8') for v in values_data]
        except UnicodeEncodeError:
            self._send_error_response(400, "Invalid UTF-8 text in keys or values")
            return
        
        # Perform batch put
        self.engine.batch_put(keys, values)
        
        self._send_json_response(200, {
            "status": "success",
            "message": f"Batch put completed for {len(keys)} items"
        })
    
    def _handle_health_check(self) -> None:
        """Handle health check requests"""
        self._send_json_response(200, {
            "status": "healthy",
            "service": "LSM Key/Value Store"
        })
    
    def _handle_stats(self) -> None:
        """Handle statistics requests"""
        engine_stats = self.engine.get_stats()
        compaction_stats = self.compaction_manager.get_stats()
        
        self._send_json_response(200, {
            "status": "success",
            "engine": engine_stats,
            "compaction": compaction_stats
        })
    
    def _send_json_response(self, status_code: int, data: Dict[str, Any]) -> None:
        """Send JSON response"""
        response_body = json.dumps(data, indent=2).encode('utf-8')
        
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(response_body)))
        self.end_headers()
        self.wfile.write(response_body)
    
    def _send_error_response(self, status_code: int, message: str) -> None:
        """Send error response"""
        error_data = {
            "status": "error",
            "code": status_code,
            "message": message
        }
        self._send_json_response(status_code, error_data)
    
    def log_message(self, format, *args):
        """Override to reduce log verbosity"""
        pass


class KVAPIServer:
    """HTTP server for Key/Value API"""
    
    def __init__(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT, data_dir: str = None):
        """
        Initialize API server.
        
        Args:
            host: Host to bind to
            port: Port to bind to
            data_dir: Directory for data storage
        """
        self.host = host
        self.port = port
        self.data_dir = data_dir or DATA_DIR
        
        # Initialize storage engine
        self.engine = LSMEngine(self.data_dir)
        self.compaction_manager = CompactionManager(self.data_dir, self.engine.sstables)
        
        # Create server
        self.server = None
        self.server_thread = None
        self._running = False
    
    def start(self) -> None:
        """Start the API server"""
        if self._running:
            return
        
        # Start compaction manager
        self.compaction_manager.start()
        
        # Create handler factory
        def handler_factory(*args, **kwargs):
            return KVAPIHandler(self.engine, self.compaction_manager, *args, **kwargs)
        
        # Create and start server
        self.server = HTTPServer((self.host, self.port), handler_factory)
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        
        self._running = True
        print(f"LSM Key/Value API server started on http://{self.host}:{self.port}")
        print("Available endpoints:")
        print("  PUT /kv/put - Store key-value pair (JSON body: {\"key\": \"...\", \"value\": \"...\"})")
        print("  GET /kv/get - Retrieve value for key (JSON body: {\"key\": \"...\"})")
        print("  GET /kv/range - Range scan (JSON body: {\"start\": \"...\", \"end\": \"...\"})")
        print("  POST /kv/batch - Batch put multiple key-value pairs")
        print("  DELETE /kv/delete - Delete key (JSON body: {\"key\": \"...\"})")
        print("  GET /health - Health check")
        print("  GET /stats - Get statistics")
    
    def stop(self) -> None:
        """Stop the API server"""
        if not self._running:
            return
        
        # Stop server
        if self.server:
            self.server.shutdown()
            self.server.server_close()
        
        # Stop compaction manager
        self.compaction_manager.stop()
        
        # Close engine
        self.engine.close()
        
        self._running = False
        print("LSM Key/Value API server stopped")
    
    def __enter__(self):
        """Context manager entry"""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop()
