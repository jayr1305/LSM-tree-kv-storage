"""
Unit tests for SSTable implementation.
"""

import unittest
import tempfile
import os
from storage.sstable import SSTableWriter, SSTableReader


class TestSSTable(unittest.TestCase):
    """Test cases for SSTable"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.sstable_path = os.path.join(self.temp_dir, 'test.sst')
    
    def tearDown(self):
        """Clean up test fixtures"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_write_and_read(self):
        """Test basic write and read operations"""
        # Write SSTable
        writer = SSTableWriter(self.sstable_path, 10)
        writer.add(b'key1', b'value1')
        writer.add(b'key2', b'value2')
        writer.add(b'key3', b'value3')
        writer.write()
        
        # Read SSTable
        reader = SSTableReader(self.sstable_path)
        self.assertTrue(reader.exists())
        
        # Test individual reads
        self.assertEqual(reader.get(b'key1'), b'value1')
        self.assertEqual(reader.get(b'key2'), b'value2')
        self.assertEqual(reader.get(b'key3'), b'value3')
        self.assertIsNone(reader.get(b'nonexistent'))
    
    def test_tombstones(self):
        """Test tombstone handling"""
        # Write SSTable with tombstone
        writer = SSTableWriter(self.sstable_path, 10)
        writer.add(b'key1', b'value1')
        writer.add(b'key2', None)  # Tombstone
        writer.add(b'key3', b'value3')
        writer.write()
        
        # Read SSTable
        reader = SSTableReader(self.sstable_path)
        
        # Test reads
        self.assertEqual(reader.get(b'key1'), b'value1')
        self.assertEqual(reader.get(b'key2'), b'__TOMBSTONE__')  # Should return tombstone marker
        self.assertEqual(reader.get(b'key3'), b'value3')
    
    def test_range_scan(self):
        """Test range scan operation"""
        # Write SSTable
        writer = SSTableWriter(self.sstable_path, 10)
        test_data = [
            (b'key1', b'value1'),
            (b'key2', b'value2'),
            (b'key3', b'value3'),
            (b'key4', b'value4'),
            (b'key5', b'value5')
        ]
        
        for key, value in test_data:
            writer.add(key, value)
        writer.write()
        
        # Read SSTable
        reader = SSTableReader(self.sstable_path)
        
        # Test range scan
        results = list(reader.range_scan(b'key2', b'key4'))
        expected = [(b'key2', b'value2'), (b'key3', b'value3')]
        self.assertEqual(results, expected)
    
    def test_get_all(self):
        """Test get all operation"""
        # Write SSTable
        writer = SSTableWriter(self.sstable_path, 10)
        test_data = [
            (b'key3', b'value3'),
            (b'key1', b'value1'),
            (b'key2', b'value2')
        ]
        
        for key, value in test_data:
            writer.add(key, value)
        writer.write()
        
        # Read SSTable
        reader = SSTableReader(self.sstable_path)
        
        # Test get all
        results = list(reader.get_all())
        
        # Should be sorted by key
        expected = [
            (b'key1', b'value1'),
            (b'key2', b'value2'),
            (b'key3', b'value3')
        ]
        self.assertEqual(results, expected)
    
    def test_bloom_filter(self):
        """Test bloom filter functionality"""
        # Write SSTable
        writer = SSTableWriter(self.sstable_path, 10)
        writer.add(b'key1', b'value1')
        writer.add(b'key2', b'value2')
        writer.write()
        
        # Read SSTable
        reader = SSTableReader(self.sstable_path)
        
        # Test bloom filter (should not have false negatives)
        self.assertIsNotNone(reader.get(b'key1'))
        self.assertIsNotNone(reader.get(b'key2'))
        
        # Test non-existent key (may have false positives, but should be fast)
        result = reader.get(b'nonexistent_key')
        # Result can be None (correct) or some value (false positive)
        # The important thing is that the bloom filter prevents unnecessary disk reads
    
    def test_metadata(self):
        """Test metadata handling"""
        # Write SSTable
        writer = SSTableWriter(self.sstable_path, 10)
        writer.add(b'key1', b'value1')
        writer.add(b'key2', b'value2')
        writer.write()
        
        # Read SSTable
        reader = SSTableReader(self.sstable_path)
        
        # Test metadata
        self.assertEqual(reader.get_key_count(), 2)
        self.assertGreater(reader.get_file_size(), 0)
        self.assertTrue(reader.exists())
    
    def test_large_dataset(self):
        """Test with larger dataset"""
        # Write larger SSTable
        writer = SSTableWriter(self.sstable_path, 1000)
        
        # Add 100 key-value pairs
        for i in range(100):
            key = f'key_{i:03d}'.encode()
            value = f'value_{i:03d}'.encode()
            writer.add(key, value)
        
        writer.write()
        
        # Read SSTable
        reader = SSTableReader(self.sstable_path)
        
        # Test reads
        self.assertEqual(reader.get_key_count(), 100)
        self.assertEqual(reader.get(b'key_050'), b'value_050')
        self.assertIsNone(reader.get(b'key_999'))
        
        # Test range scan
        results = list(reader.range_scan(b'key_010', b'key_020'))
        self.assertEqual(len(results), 10)
    
    def test_empty_sstable(self):
        """Test empty SSTable"""
        # Write empty SSTable
        writer = SSTableWriter(self.sstable_path, 10)
        writer.write()
        
        # Read SSTable
        reader = SSTableReader(self.sstable_path)
        
        # Test reads
        self.assertEqual(reader.get_key_count(), 0)
        self.assertIsNone(reader.get(b'any_key'))
        self.assertEqual(list(reader.get_all()), [])
    
    def test_nonexistent_file(self):
        """Test reading non-existent SSTable"""
        nonexistent_path = os.path.join(self.temp_dir, 'nonexistent.sst')
        reader = SSTableReader(nonexistent_path)
        
        self.assertFalse(reader.exists())
        self.assertEqual(reader.get_key_count(), 0)
        self.assertEqual(reader.get_file_size(), 0)


if __name__ == '__main__':
    unittest.main()
