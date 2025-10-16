"""
Unit tests for MemTable implementation.
"""

import unittest
from storage.memtable import MemTable


class TestMemTable(unittest.TestCase):
    """Test cases for MemTable"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.memtable = MemTable()
    
    def test_put_and_get(self):
        """Test basic put and get operations"""
        key = b'test_key'
        value = b'test_value'
        
        # Put key-value pair
        self.memtable.put(key, value)
        
        # Get value
        retrieved_value = self.memtable.get(key)
        self.assertEqual(retrieved_value, value)
    
    def test_put_overwrite(self):
        """Test overwriting existing key"""
        key = b'test_key'
        value1 = b'value1'
        value2 = b'value2'
        
        # Put first value
        self.memtable.put(key, value1)
        self.assertEqual(self.memtable.get(key), value1)
        
        # Overwrite with second value
        self.memtable.put(key, value2)
        self.assertEqual(self.memtable.get(key), value2)
    
    def test_get_nonexistent_key(self):
        """Test getting non-existent key"""
        key = b'nonexistent_key'
        value = self.memtable.get(key)
        self.assertIsNone(value)
    
    def test_delete(self):
        """Test delete operation"""
        key = b'test_key'
        value = b'test_value'
        
        # Put key-value pair
        self.memtable.put(key, value)
        self.assertEqual(self.memtable.get(key), value)
        
        # Delete key
        deleted = self.memtable.delete(key)
        self.assertTrue(deleted)
        self.assertIsNone(self.memtable.get(key))
    
    def test_delete_nonexistent_key(self):
        """Test deleting non-existent key"""
        key = b'nonexistent_key'
        deleted = self.memtable.delete(key)
        self.assertFalse(deleted)
    
    def test_range_scan(self):
        """Test range scan operation"""
        # Insert test data
        test_data = [
            (b'key1', b'value1'),
            (b'key2', b'value2'),
            (b'key3', b'value3'),
            (b'key4', b'value4'),
            (b'key5', b'value5')
        ]
        
        for key, value in test_data:
            self.memtable.put(key, value)
        
        # Test range scan
        results = list(self.memtable.range_scan(b'key2', b'key4'))
        expected = [(b'key2', b'value2'), (b'key3', b'value3')]
        self.assertEqual(results, expected)
    
    def test_range_scan_empty(self):
        """Test range scan with no results"""
        results = list(self.memtable.range_scan(b'key1', b'key2'))
        self.assertEqual(results, [])
    
    def test_get_all(self):
        """Test get all operation"""
        # Insert test data
        test_data = [
            (b'key3', b'value3'),
            (b'key1', b'value1'),
            (b'key2', b'value2')
        ]
        
        for key, value in test_data:
            self.memtable.put(key, value)
        
        # Get all data
        results = list(self.memtable.get_all())
        
        # Should be sorted by key
        expected = [
            (b'key1', b'value1'),
            (b'key2', b'value2'),
            (b'key3', b'value3')
        ]
        self.assertEqual(results, expected)
    
    def test_size_and_memory_usage(self):
        """Test size and memory usage tracking"""
        # Initially empty
        self.assertEqual(self.memtable.get_size(), 0)
        self.assertEqual(self.memtable.get_memory_usage(), 0)
        self.assertTrue(self.memtable.is_empty())
        
        # Add some data
        self.memtable.put(b'key1', b'value1')
        self.memtable.put(b'key2', b'value2')
        
        self.assertEqual(self.memtable.get_size(), 2)
        self.assertGreater(self.memtable.get_memory_usage(), 0)
        self.assertFalse(self.memtable.is_empty())
    
    def test_clear(self):
        """Test clear operation"""
        # Add some data
        self.memtable.put(b'key1', b'value1')
        self.memtable.put(b'key2', b'value2')
        
        self.assertEqual(self.memtable.get_size(), 2)
        
        # Clear memtable
        self.memtable.clear()
        
        self.assertEqual(self.memtable.get_size(), 0)
        self.assertEqual(self.memtable.get_memory_usage(), 0)
        self.assertTrue(self.memtable.is_empty())
    
    def test_concurrent_operations(self):
        """Test concurrent operations (basic thread safety)"""
        import threading
        import time
        
        def writer():
            for i in range(100):
                self.memtable.put(f'key_{i}'.encode(), f'value_{i}'.encode())
                time.sleep(0.001)
        
        def reader():
            for i in range(100):
                self.memtable.get(f'key_{i}'.encode())
                time.sleep(0.001)
        
        # Start threads
        writer_thread = threading.Thread(target=writer)
        reader_thread = threading.Thread(target=reader)
        
        writer_thread.start()
        reader_thread.start()
        
        # Wait for completion
        writer_thread.join()
        reader_thread.join()
        
        # Verify some data was written
        self.assertGreater(self.memtable.get_size(), 0)


if __name__ == '__main__':
    unittest.main()
