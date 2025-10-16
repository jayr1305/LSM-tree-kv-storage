"""
Integration tests for HTTP API server.
"""

import unittest
import requests
from server.api_server import KVAPIServer
import tempfile
import shutil

class TestKVAPI(unittest.TestCase):
    """Test cases for Key/Value API"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.server = KVAPIServer('localhost', 0, self.temp_dir)  # Port 0 = random available port
        self.server.start()
        self.port = self.server.server.server_address[1]
        self.base_url = f'http://localhost:{self.port}'
    
    def tearDown(self):
        """Clean up test fixtures"""
        self.server.stop()
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    
    def test_put_and_get(self):
        """Test PUT and GET operations"""
        key = 'test_key'
        value = 'test_value'
        
        # PUT request
        response = requests.put(f'{self.base_url}/kv/put', json={'key': key, 'value': value})
        self.assertEqual(response.status_code, 200)
        
        # GET request
        response = requests.get(f'{self.base_url}/kv/get', json={'key': key})
        self.assertEqual(response.status_code, 200)
        
        data = response.json()
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['value'], value)
    
    def test_get_nonexistent_key(self):
        """Test GET for non-existent key"""
        response = requests.get(f'{self.base_url}/kv/get', json={'key': 'nonexistent_key'})
        self.assertEqual(response.status_code, 404)
        
        data = response.json()
        self.assertEqual(data['status'], 'not_found')
    
    def test_delete(self):
        """Test DELETE operation"""
        key = 'delete_test_key'
        value = 'delete_test_value'
        
        # PUT key
        response = requests.put(f'{self.base_url}/kv/put', json={'key': key, 'value': value})
        self.assertEqual(response.status_code, 200)
        
        # Verify key exists
        response = requests.get(f'{self.base_url}/kv/get', json={'key': key})
        self.assertEqual(response.status_code, 200)
        
        # DELETE key
        response = requests.delete(f'{self.base_url}/kv/delete', json={'key': key})
        self.assertEqual(response.status_code, 200)
        
        # Verify key is deleted
        response = requests.get(f'{self.base_url}/kv/get', json={'key': key})
        self.assertEqual(response.status_code, 404)
    
    def test_delete_nonexistent_key(self):
        """Test DELETE for non-existent key"""
        response = requests.delete(f'{self.base_url}/kv/delete', json={'key': 'nonexistent_key'})
        self.assertEqual(response.status_code, 404)
        
        data = response.json()
        self.assertEqual(data['status'], 'not_found')
    
    def test_range_scan(self):
        """Test range scan operation"""
        # Insert test data
        test_data = [
            ('key1', 'value1'),
            ('key2', 'value2'),
            ('key3', 'value3'),
            ('key4', 'value4'),
            ('key5', 'value5')
        ]
        
        for key, value in test_data:
            response = requests.put(f'{self.base_url}/kv/put', json={'key': key, 'value': value})
            self.assertEqual(response.status_code, 200)
        
        # Range scan
        response = requests.get(f'{self.base_url}/kv/range', json={'start': 'key2', 'end': 'key4'})
        self.assertEqual(response.status_code, 200)
        
        data = response.json()
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['count'], 2)
        
        # Verify results
        results = data['results']
        self.assertEqual(len(results), 2)
        
        # Check that results are sorted by key
        keys = [result['key'] for result in results]
        self.assertEqual(keys, ['key2', 'key3'])
    
    def test_batch_put(self):
        """Test batch put operation"""
        keys = ['batch_key1', 'batch_key2', 'batch_key3']
        values = ['batch_value1', 'batch_value2', 'batch_value3']
        
        # Batch PUT request
        payload = {
            'keys': keys,
            'values': values
        }
        
        response = requests.post(f'{self.base_url}/kv/batch', json=payload)
        self.assertEqual(response.status_code, 200)
        
        data = response.json()
        self.assertEqual(data['status'], 'success')
        self.assertIn('3 items', data['message'])
        
        # Verify all keys were stored
        for key in keys:
            response = requests.get(f'{self.base_url}/kv/get', json={'key': key})
            self.assertEqual(response.status_code, 200)
    
    def test_batch_put_mismatched_lengths(self):
        """Test batch put with mismatched key/value lengths"""
        payload = {
            'keys': ['key1', 'key2'],
            'values': ['value1']  # Mismatched length
        }
        
        response = requests.post(f'{self.base_url}/kv/batch', json=payload)
        self.assertEqual(response.status_code, 400)
    
    def test_health_check(self):
        """Test health check endpoint"""
        response = requests.get(f'{self.base_url}/health')
        self.assertEqual(response.status_code, 200)
        
        data = response.json()
        self.assertEqual(data['status'], 'healthy')
        self.assertEqual(data['service'], 'LSM Key/Value Store')
    
    def test_stats(self):
        """Test statistics endpoint"""
        # Perform some operations
        requests.put(f'{self.base_url}/kv/put', json={'key': 'stats_test_key', 'value': 'stats_test_value'})
        requests.get(f'{self.base_url}/kv/get', json={'key': 'stats_test_key'})
        
        # Get stats
        response = requests.get(f'{self.base_url}/stats')
        self.assertEqual(response.status_code, 200)
        
        data = response.json()
        self.assertEqual(data['status'], 'success')
        self.assertIn('engine', data)
        self.assertIn('compaction', data)
        
        # Check engine stats
        engine_stats = data['engine']
        self.assertGreater(engine_stats['puts'], 0)
        self.assertGreater(engine_stats['gets'], 0)
    
    def test_invalid_paths(self):
        """Test invalid API paths"""
        # Invalid path
        response = requests.get(f'{self.base_url}/invalid')
        self.assertEqual(response.status_code, 404)
        
        # Invalid PUT path
        response = requests.put(f'{self.base_url}/invalid', json={'key': 'test', 'value': 'test'})
        self.assertEqual(response.status_code, 404)
    
    def test_invalid_json(self):
        """Test invalid JSON in request body"""
        # Invalid JSON in PUT
        response = requests.put(f'{self.base_url}/kv/put', data='invalid json')
        self.assertEqual(response.status_code, 400)
        
        # Invalid JSON in GET
        response = requests.get(f'{self.base_url}/kv/get', data='invalid json')
        self.assertEqual(response.status_code, 400)
    
    def test_missing_parameters(self):
        """Test missing required parameters"""
        # Missing key for GET
        response = requests.get(f'{self.base_url}/kv/get', json={})
        self.assertEqual(response.status_code, 400)
        
        # Missing start/end for range scan
        response = requests.get(f'{self.base_url}/kv/range', json={})
        self.assertEqual(response.status_code, 400)
        
        # Missing keys/values for batch put
        response = requests.post(f'{self.base_url}/kv/batch', json={})
        self.assertEqual(response.status_code, 400)


if __name__ == '__main__':
    unittest.main()
