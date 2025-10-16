"""
In-memory sorted data structure for fast writes and range queries.
Uses a skip list for O(log n) operations and efficient range scans.
"""

import threading
from typing import Optional, Iterator, Tuple
import random


class SkipListNode:
    """Node in skip list with multiple forward pointers"""
    
    def __init__(self, key: bytes, value: Optional[bytes], level: int):
        self.key = key
        self.value = value  # None for deleted entries
        self.forward = [None] * (level + 1)
        self.level = level


class MemTable:
    """
    In-memory sorted key-value store using skip list.
    Provides O(log n) insert, delete, and lookup operations.
    Supports range queries and ordered iteration.
    """
    
    def __init__(self, max_level: int = 16):
        """
        Initialize memtable with skip list.
        
        Args:
            max_level: Maximum level for skip list (affects memory vs performance trade-off)
        """
        self.max_level = max_level
        self.level = 0
        self.size = 0
        
        # Create header node with maximum level
        self.header = SkipListNode(b'', None, max_level)
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Track memory usage (approximate)
        self._memory_usage = 0
    
    def _random_level(self) -> int:
        """Generate random level for new node (geometric distribution)"""
        level = 0
        while random.random() < 0.5 and level < self.max_level:
            level += 1
        return level
    
    def put(self, key: bytes, value: bytes) -> None:
        """
        Insert or update key-value pair.
        
        Args:
            key: Key bytes
            value: Value bytes
        """
        with self._lock:
            # Find insertion point and update path
            update = [None] * (self.max_level + 1)
            current = self.header
            
            # Traverse from top level to bottom
            for i in range(self.level, -1, -1):
                while current.forward[i] and current.forward[i].key < key:
                    current = current.forward[i]
                update[i] = current
            
            # Move to next node at level 0
            current = current.forward[0]
            
            # If key exists, update value
            if current and current.key == key:
                old_size = len(current.value) if current.value else 0
                current.value = value
                new_size = len(value) if value else 0
                self._memory_usage += new_size - old_size
                return
            
            # Generate random level for new node
            new_level = self._random_level()
            
            # If new level is higher than current level, update header
            if new_level > self.level:
                for i in range(self.level + 1, new_level + 1):
                    update[i] = self.header
                self.level = new_level
            
            # Create new node
            new_node = SkipListNode(key, value, new_level)
            
            # Update forward pointers
            for i in range(new_level + 1):
                new_node.forward[i] = update[i].forward[i]
                update[i].forward[i] = new_node
            
            self.size += 1
            value_size = len(value) if value else 0
            self._memory_usage += len(key) + value_size + (new_level + 1) * 8  # Approximate pointer overhead
    
    def get(self, key: bytes) -> Optional[bytes]:
        """
        Get value for key.
        
        Args:
            key: Key to lookup
            
        Returns:
            Value bytes if found, None otherwise
        """
        with self._lock:
            current = self.header
            
            # Traverse from top level to bottom
            for i in range(self.level, -1, -1):
                while current.forward[i] and current.forward[i].key < key:
                    current = current.forward[i]
            
            # Move to next node at level 0
            current = current.forward[0]
            
            if current and current.key == key:
                return current.value
            
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
            # Find deletion point and update path
            update = [None] * (self.max_level + 1)
            current = self.header
            
            # Traverse from top level to bottom
            for i in range(self.level, -1, -1):
                while current.forward[i] and current.forward[i].key < key:
                    current = current.forward[i]
                update[i] = current
            
            # Move to next node at level 0
            current = current.forward[0]
            
            if current and current.key == key:
                # Mark as deleted (tombstone)
                current.value = None
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
            # Find starting position
            current = self.header
            
            # Traverse from top level to bottom to find start_key
            for i in range(self.level, -1, -1):
                while current.forward[i] and current.forward[i].key < start_key:
                    current = current.forward[i]
            
            # Move to first node >= start_key
            current = current.forward[0]
            
            # Iterate through range
            while current and current.key < end_key:
                yield current.key, current.value
                current = current.forward[0]
    
    def get_all(self) -> Iterator[Tuple[bytes, Optional[bytes]]]:
        """
        Get all key-value pairs in sorted order.
        
        Yields:
            Tuple of (key, value) for each pair
        """
        with self._lock:
            current = self.header.forward[0]
            while current:
                yield current.key, current.value
                current = current.forward[0]
    
    def get_memory_usage(self) -> int:
        """
        Get approximate memory usage in bytes.
        
        Returns:
            Approximate memory usage
        """
        with self._lock:
            return self._memory_usage
    
    def get_size(self) -> int:
        """
        Get number of entries in memtable.
        
        Returns:
            Number of entries
        """
        with self._lock:
            return self.size
    
    def clear(self) -> None:
        """Clear all entries from memtable"""
        with self._lock:
            self.level = 0
            self.size = 0
            self._memory_usage = 0
            
            # Reset header forward pointers
            for i in range(self.max_level + 1):
                self.header.forward[i] = None
    
    def is_empty(self) -> bool:
        """
        Check if memtable is empty.
        
        Returns:
            True if empty, False otherwise
        """
        with self._lock:
            return self.size == 0
