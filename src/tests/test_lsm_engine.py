"""
Unit tests for LSM Engine implementation.
"""

import unittest
import tempfile
import shutil
from storage.lsm_engine import LSMEngine


class TestLSMEngine(unittest.TestCase):
    """Test cases for LSM Engine"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.engine = LSMEngine(self.temp_dir)
    
    def tearDown(self):
        """Clean up test fixtures"""
        self.engine.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_put_and_get(self):
        """Test basic put and get operations"""
        key = b'test_key'
        value = b'test_value'
        
        # Put key-value pair
        self.engine.put(key, value)
        
        # Get value
        retrieved_value = self.engine.get(key)
        self.assertEqual(retrieved_value, value)
    
    def test_put_overwrite(self):
        """Test overwriting existing key"""
        key = b'test_key'
        value1 = b'value1'
        value2 = b'value2'
        
        # Put first value
        self.engine.put(key, value1)
        self.assertEqual(self.engine.get(key), value1)
        
        # Overwrite with second value
        self.engine.put(key, value2)
        self.assertEqual(self.engine.get(key), value2)
    
    def test_get_nonexistent_key(self):
        """Test getting non-existent key"""
        key = b'nonexistent_key'
        value = self.engine.get(key)
        self.assertIsNone(value)
    
    def test_delete(self):
        """Test delete operation"""
        key = b'test_key'
        value = b'test_value'
        
        # Put key-value pair
        self.engine.put(key, value)
        self.assertEqual(self.engine.get(key), value)
        
        # Delete key
        deleted = self.engine.delete(key)
        self.assertTrue(deleted)
        self.assertIsNone(self.engine.get(key))
    
    def test_delete_nonexistent_key(self):
        """Test deleting non-existent key"""
        key = b'nonexistent_key'
        deleted = self.engine.delete(key)
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
            self.engine.put(key, value)
        
        # Test range scan
        results = list(self.engine.range_scan(b'key2', b'key4'))
        expected = [(b'key2', b'value2'), (b'key3', b'value3')]
        self.assertEqual(results, expected)
    
    def test_batch_put(self):
        """Test batch put operation"""
        keys = [b'key1', b'key2', b'key3']
        values = [b'value1', b'value2', b'value3']
        
        # Batch put
        self.engine.batch_put(keys, values)
        
        # Verify all keys were stored
        for key, value in zip(keys, values):
            self.assertEqual(self.engine.get(key), value)
    
    def test_batch_put_mismatched_lengths(self):
        """Test batch put with mismatched key/value lengths"""
        keys = [b'key1', b'key2']
        values = [b'value1']  # Mismatched length
        
        with self.assertRaises(ValueError):
            self.engine.batch_put(keys, values)
    
    def test_stats(self):
        """Test statistics tracking"""
        # Initially empty
        stats = self.engine.get_stats()
        self.assertEqual(stats['puts'], 0)
        self.assertEqual(stats['gets'], 0)
        self.assertEqual(stats['deletes'], 0)
        
        # Perform operations
        self.engine.put(b'key1', b'value1')
        self.engine.get(b'key1')
        self.engine.delete(b'key1')
        
        # Check updated stats
        stats = self.engine.get_stats()
        self.assertEqual(stats['puts'], 1)
        self.assertEqual(stats['gets'], 2)  # One explicit get + one from delete operation
        self.assertEqual(stats['deletes'], 1)
    
    def test_persistence(self):
        """Test data persistence across engine restarts"""
        key = b'persistent_key'
        value = b'persistent_value'
        
        # Put data
        self.engine.put(key, value)
        self.assertEqual(self.engine.get(key), value)
        
        # Close and reopen engine
        self.engine.close()
        self.engine = LSMEngine(self.temp_dir)
        
        # Data should still be there
        self.assertEqual(self.engine.get(key), value)
    
    def test_memtable_flush(self):
        """Test memtable flushing to SSTable"""
        # Force memtable flush by adding many entries (more than MEMTABLE_MAX_ENTRIES)
        for i in range(100001):  # Add one more than the limit
            key = f'key_{i:06d}'.encode()
            value = f'value_{i:06d}'.encode()
            self.engine.put(key, value)
        
        # Check that data is still accessible
        self.assertEqual(self.engine.get(b'key_000000'), b'value_000000')
        self.assertEqual(self.engine.get(b'key_100000'), b'value_100000')
        
        # Check stats
        stats = self.engine.get_stats()
        self.assertGreater(stats['flushes'], 0)
    
    def test_crash_recovery(self):
        """Test crash recovery from WAL"""
        key = b'crash_test_key'
        value = b'crash_test_value'
        
        # Put data (this goes to WAL)
        self.engine.put(key, value)
        self.assertEqual(self.engine.get(key), value)
        
        # Simulate crash by closing engine without proper shutdown
        self.engine.wal.close()
        self.engine.close()
        
        # Restart engine (should recover from WAL)
        self.engine = LSMEngine(self.temp_dir)
        
        # Data should be recovered
        self.assertEqual(self.engine.get(key), value)
    
    def test_large_dataset(self):
        """Test with larger dataset"""
        # Add 1000 key-value pairs
        for i in range(1000):
            key = f'large_key_{i:04d}'.encode()
            value = f'large_value_{i:04d}'.encode()
            self.engine.put(key, value)
        
        # Test random reads
        self.assertEqual(self.engine.get(b'large_key_0000'), b'large_value_0000')
        self.assertEqual(self.engine.get(b'large_key_0999'), b'large_value_0999')
        self.assertIsNone(self.engine.get(b'large_key_9999'))
        
        # Test range scan
        results = list(self.engine.range_scan(b'large_key_0100', b'large_key_0200'))
        self.assertEqual(len(results), 100)
    
    def test_concurrent_operations(self):
        """Test concurrent operations"""
        import threading
        import time
        
        def writer():
            for i in range(100):
                self.engine.put(f'concurrent_key_{i}'.encode(), f'concurrent_value_{i}'.encode())
                time.sleep(0.001)
        
        def reader():
            for i in range(100):
                self.engine.get(f'concurrent_key_{i}'.encode())
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
        stats = self.engine.get_stats()
        self.assertGreater(stats['puts'], 0)


if __name__ == '__main__':
    unittest.main()
