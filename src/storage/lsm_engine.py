"""
LSM-Tree storage engine that integrates memtable, WAL, and SSTables.
Provides the main interface for key-value operations with durability and performance.
"""

import os
import threading
import time
from typing import Optional, Iterator, Tuple, List
from .memtable import MemTable
from .wal import WAL
from .sstable import SSTableWriter, SSTableReader
from config import (
    DATA_DIR, MEMTABLE_MAX_SIZE, MEMTABLE_MAX_ENTRIES,
    MAX_LEVELS
)


class LSMEngine:
    """
    LSM-Tree storage engine with memtable, WAL, and multi-level SSTables.
    Provides ACID properties and high performance for key-value operations.
    """
    
    def __init__(self, data_dir: str = DATA_DIR):
        """
        Initialize LSM engine.
        
        Args:
            data_dir: Directory for storing data files
        """
        self.data_dir = data_dir
        self.wal_file = os.path.join(data_dir, 'wal.log')
        
        # Ensure data directory exists
        os.makedirs(data_dir, exist_ok=True)
        
        # Initialize components
        self.memtable = MemTable()
        self.wal = WAL(self.wal_file)
        self.sstables: List[List[SSTableReader]] = [[] for _ in range(MAX_LEVELS)]
        
        # Thread safety
        self._lock = threading.RLock()
        self._flush_lock = threading.Lock()
        
        # Statistics
        self._stats = {
            'puts': 0,
            'gets': 0,
            'deletes': 0,
            'range_scans': 0,
            'flushes': 0,
            'compactions': 0
        }
        
        # Load existing SSTables and recover from WAL
        self._load_sstables()
        self._recover_from_wal()
        
        # Open WAL for new operations
        self.wal.open()
    
    def _load_sstables(self) -> None:
        """Load existing SSTable files from disk"""
        for level in range(MAX_LEVELS):
            level_dir = os.path.join(self.data_dir, f'level_{level}')
            if not os.path.exists(level_dir):
                continue
            
            sstable_files = sorted([f for f in os.listdir(level_dir) if f.endswith('.sst')])
            for sstable_file in sstable_files:
                sstable_path = os.path.join(level_dir, sstable_file)
                reader = SSTableReader(sstable_path)
                if reader.exists():
                    self.sstables[level].append(reader)
    
    def _recover_from_wal(self) -> None:
        """Recover state from WAL after crash"""
        if not os.path.exists(self.wal_file):
            return
        
        print("Recovering from WAL...")
        recovered_entries = 0
        
        for entry in self.wal.replay():
            try:
                if entry.operation == 'PUT':
                    self.memtable.put(entry.key, entry.value)
                elif entry.operation == 'DELETE':
                    self.memtable.delete(entry.key)
                recovered_entries += 1
            except Exception as e:
                print(f"Error recovering entry: {e}")
                continue
        
        print(f"Recovered {recovered_entries} entries from WAL")
        
        # Clear WAL after successful recovery
        self.wal.clear()
    
    def put(self, key: bytes, value: bytes) -> None:
        """
        Put key-value pair.
        
        Args:
            key: Key bytes
            value: Value bytes
        """
        with self._lock:
            # Write to WAL first for durability
            self.wal.put(key, value)
            
            # Write to memtable
            self.memtable.put(key, value)
            
            # Check if memtable needs flushing
            if self._should_flush_memtable():
                self._flush_memtable()
            
            self._stats['puts'] += 1
    
    def get(self, key: bytes) -> Optional[bytes]:
        """
        Get value for key.
        
        Args:
            key: Key to lookup
            
        Returns:
            Value bytes if found, None otherwise
        """
        with self._lock:
            # First check memtable - need to check if key exists (even with None value)
            for mem_key, mem_value in self.memtable.get_all():
                if mem_key == key:
                    self._stats['gets'] += 1
                    return mem_value  # Could be None for deleted keys
            
            # Check SSTables from newest to oldest (level 0 to max level)
            for level in range(MAX_LEVELS):
                for sstable in reversed(self.sstables[level]):  # Newest first within level
                    value = sstable.get(key)
                    if value is not None:
                        self._stats['gets'] += 1
                        # Return None if tombstone, otherwise return the value
                        if value == b'__TOMBSTONE__':
                            return None
                        return value
            
            self._stats['gets'] += 1
            return None
    
    def delete(self, key: bytes) -> bool:
        """
        Delete key (mark as deleted with tombstone).
        
        Args:
            key: Key to delete
            
        Returns:
            True if key was found and deleted, False otherwise
        """
        with self._lock:
            # Check if key exists (in memtable or SSTables)
            exists = self.get(key) is not None
            
            if exists:
                # Write delete to WAL
                self.wal.delete(key)
                
                # Add tombstone to memtable (overwrite any existing value)
                # Use put(key, None) to ensure tombstone is added even if key not in memtable
                self.memtable.put(key, None)
                
                # Check if memtable needs flushing
                if self._should_flush_memtable():
                    self._flush_memtable()
                
                self._stats['deletes'] += 1
                return True
            
            return False
    
    def range_scan(self, start_key: bytes, end_key: bytes) -> Iterator[Tuple[bytes, Optional[bytes]]]:
        """
        Get all key-value pairs in range [start_key, end_key).
        
        Args:
            start_key: Inclusive start key
            end_key: Exclusive end key
            
        Yields:
            Tuple of (key, value) for each pair in range
        """
        with self._lock:
            # Collect all key-value pairs from memtable and SSTables
            all_pairs = []
            
            # Get from memtable
            for key, value in self.memtable.range_scan(start_key, end_key):
                all_pairs.append((key, value))
            
            # Get from SSTables (newest to oldest)
            for level in range(MAX_LEVELS):
                for sstable in reversed(self.sstables[level]):
                    for key, value in sstable.range_scan(start_key, end_key):
                        all_pairs.append((key, value))
            
            # Sort by key and remove duplicates (newest wins)
            all_pairs.sort(key=lambda x: x[0])
            
            # Yield unique keys (newest value wins), include tombstones
            seen_keys = set()
            for key, value in all_pairs:
                if key not in seen_keys:
                    seen_keys.add(key)
                    # Include tombstones (deleted keys) as None values
                    if value == b'__TOMBSTONE__':
                        yield key, None
                    else:
                        yield key, value
            
            self._stats['range_scans'] += 1
    
    def batch_put(self, keys: List[bytes], values: List[bytes]) -> None:
        """
        Batch put multiple key-value pairs.
        
        Args:
            keys: List of key bytes
            values: List of value bytes
        """
        if len(keys) != len(values):
            raise ValueError("Keys and values lists must have the same length")
        
        with self._lock:
            # Write all to WAL first
            for key, value in zip(keys, values):
                self.wal.put(key, value)
            
            # Write all to memtable
            for key, value in zip(keys, values):
                self.memtable.put(key, value)
            
            # Check if memtable needs flushing
            if self._should_flush_memtable():
                self._flush_memtable()
            
            self._stats['puts'] += len(keys)
    
    def _should_flush_memtable(self) -> bool:
        """Check if memtable should be flushed to SSTable"""
        return (self.memtable.get_memory_usage() > MEMTABLE_MAX_SIZE or
                self.memtable.get_size() > MEMTABLE_MAX_ENTRIES)
    
    def _flush_memtable(self) -> None:
        """Flush memtable to SSTable"""
        with self._flush_lock:
            if self.memtable.is_empty():
                return
            
            # Create SSTable file
            level_0_dir = os.path.join(self.data_dir, 'level_0')
            os.makedirs(level_0_dir, exist_ok=True)
            
            timestamp = int(time.time() * 1000000)  # Microseconds
            sstable_path = os.path.join(level_0_dir, f'{timestamp}.sst')
            
            # Write SSTable
            writer = SSTableWriter(sstable_path, self.memtable.get_size())
            for key, value in self.memtable.get_all():
                writer.add(key, value)
            writer.write()
            
            # Add to level 0 SSTables
            reader = SSTableReader(sstable_path)
            self.sstables[0].append(reader)
            
            # Clear memtable and WAL
            self.memtable.clear()
            self.wal.clear()
            
            self._stats['flushes'] += 1
            print(f"Flushed memtable to {sstable_path}")
    
    def get_stats(self) -> dict:
        """Get engine statistics"""
        with self._lock:
            stats = self._stats.copy()
            stats['memtable_size'] = self.memtable.get_size()
            stats['memtable_memory'] = self.memtable.get_memory_usage()
            stats['wal_size'] = self.wal.get_size()
            stats['sstable_counts'] = [len(level) for level in self.sstables]
            return stats
    
    def close(self) -> None:
        """Close engine and flush memtable"""
        with self._lock:
            if not self.memtable.is_empty():
                self._flush_memtable()
            self.wal.close()
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
