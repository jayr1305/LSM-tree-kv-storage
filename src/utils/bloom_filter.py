"""
Bloom Filter implementation for fast negative lookups in SSTables.
Uses multiple hash functions to reduce false positive rate.
"""

import hashlib
import struct
import math


class BloomFilter:
    """
    Space-efficient probabilistic data structure for membership testing.
    False positives are possible, but false negatives are not.
    """
    
    def __init__(self, expected_items: int, false_positive_rate: float = 0.01):
        """
        Initialize bloom filter.
        
        Args:
            expected_items: Expected number of items to be inserted
            false_positive_rate: Desired false positive rate (0.01 = 1%)
        """
        self.expected_items = expected_items
        self.false_positive_rate = false_positive_rate
        
        # Calculate optimal bit array size and number of hash functions
        self.bit_array_size = self._calculate_bit_array_size(expected_items, false_positive_rate)
        self.num_hash_functions = self._calculate_num_hash_functions(expected_items, self.bit_array_size)
        
        # Initialize bit array (using bytes for memory efficiency)
        self.bit_array = bytearray((self.bit_array_size + 7) // 8)
        self.items_added = 0
    
    def _calculate_bit_array_size(self, n: int, p: float) -> int:
        """Calculate optimal bit array size using formula: m = -(n * ln(p)) / (ln(2)^2)"""
        return int(-(n * math.log(p)) / (math.log(2) ** 2))
    
    def _calculate_num_hash_functions(self, n: int, m: int) -> int:
        """Calculate optimal number of hash functions: k = (m/n) * ln(2)"""
        return max(1, int((m / n) * math.log(2)))
    
    def _hash(self, item: bytes, seed: int) -> int:
        """Generate hash for item with given seed"""
        hasher = hashlib.sha256()
        hasher.update(item)
        hasher.update(seed.to_bytes(4, 'big'))
        return int.from_bytes(hasher.digest()[:8], 'big') % self.bit_array_size
    
    def add(self, item: bytes) -> None:
        """Add item to bloom filter"""
        for i in range(self.num_hash_functions):
            bit_index = self._hash(item, i)
            byte_index = bit_index // 8
            bit_offset = bit_index % 8
            self.bit_array[byte_index] |= (1 << bit_offset)
        self.items_added += 1
    
    def contains(self, item: bytes) -> bool:
        """
        Check if item might be in the set.
        Returns True if item might be present (could be false positive)
        Returns False if item is definitely not present
        """
        for i in range(self.num_hash_functions):
            bit_index = self._hash(item, i)
            byte_index = bit_index // 8
            bit_offset = bit_index % 8
            if not (self.bit_array[byte_index] & (1 << bit_offset)):
                return False
        return True
    
    def serialize(self) -> bytes:
        """Serialize bloom filter to bytes for storage"""
        # Pack metadata: expected_items, false_positive_rate, bit_array_size, num_hash_functions, items_added
        metadata = struct.pack('>QfIIQ', 
                             self.expected_items,
                             self.false_positive_rate,
                             self.bit_array_size,
                             self.num_hash_functions,
                             self.items_added)
        return metadata + bytes(self.bit_array)
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'BloomFilter':
        """Deserialize bloom filter from bytes"""
        # Unpack metadata
        metadata_size = 8 + 4 + 4 + 4 + 8  # Q + f + I + I + Q
        metadata = struct.unpack('>QfIIQ', data[:metadata_size])
        
        expected_items, false_positive_rate, bit_array_size, num_hash_functions, items_added = metadata
        
        # Create bloom filter instance
        bf = cls(expected_items, false_positive_rate)
        bf.bit_array_size = bit_array_size
        bf.num_hash_functions = num_hash_functions
        bf.items_added = items_added
        
        # Restore bit array
        bf.bit_array = bytearray(data[metadata_size:])
        
        return bf
    
    def get_false_positive_rate(self) -> float:
        """Calculate current false positive rate based on items added"""
        if self.items_added == 0:
            return 0.0
        
        import math
        # Formula: (1 - e^(-k*n/m))^k
        k = self.num_hash_functions
        n = self.items_added
        m = self.bit_array_size
        
        return (1 - math.exp(-k * n / m)) ** k
