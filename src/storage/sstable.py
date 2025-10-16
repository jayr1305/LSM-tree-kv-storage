"""
SSTable (Sorted String Table) implementation for persistent storage.
Provides efficient disk-based storage with bloom filters and sparse indexing.
"""

import os
import struct
from typing import Iterator, Tuple, Optional, List
from utils.bloom_filter import BloomFilter
from utils.serialization import (
    encode_key_value, decode_key_value, encode_metadata, decode_metadata,
    encode_index_entry, decode_index_entry, decode_varint
)


class SSTableWriter:
    """Writer for creating SSTable files"""
    
    def __init__(self, file_path: str, expected_items: int = 10000, false_positive_rate: float = 0.01):
        """
        Initialize SSTable writer.
        
        Args:
            file_path: Path to SSTable file
            expected_items: Expected number of items for bloom filter sizing
            false_positive_rate: Desired false positive rate for bloom filter
        """
        self.file_path = file_path
        self.expected_items = expected_items
        self.false_positive_rate = false_positive_rate
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Initialize components
        self.bloom_filter = BloomFilter(max(1, expected_items), false_positive_rate)
        self.index_entries: List[Tuple[bytes, int]] = []
        self.data_blocks: List[bytes] = []
        
        # Track current position
        self.current_offset = 0
        self.key_count = 0
    
    def add(self, key: bytes, value: Optional[bytes]) -> None:
        """
        Add key-value pair to SSTable.
        
        Args:
            key: Key bytes
            value: Value bytes (None for tombstones)
        """
        # Add to bloom filter
        self.bloom_filter.add(key)
        
        # Encode key-value pair
        if value is None:
            # Tombstone - use special marker
            encoded = encode_key_value(key, b'__TOMBSTONE__')
        else:
            encoded = encode_key_value(key, value)
        
        # Add to data blocks
        self.data_blocks.append(encoded)
        
        # Add to sparse index (every 16th key)
        if self.key_count % 16 == 0:
            self.index_entries.append((key, self.current_offset))
        
        self.current_offset += len(encoded)
        self.key_count += 1
    
    def write(self) -> None:
        """Write SSTable to disk"""
        with open(self.file_path, 'wb') as f:
            # Sort data blocks by key before writing
            # Extract keys from data blocks for sorting
            key_value_pairs = []
            for block in self.data_blocks:
                key, value, _ = decode_key_value(block)
                key_value_pairs.append((key, value))
            
            # Sort by key
            key_value_pairs.sort(key=lambda x: x[0])
            
            # Re-encode sorted data
            sorted_blocks = []
            for key, value in key_value_pairs:
                if value is None:
                    encoded = encode_key_value(key, b'__TOMBSTONE__')
                else:
                    encoded = encode_key_value(key, value)
                sorted_blocks.append(encoded)
            
            # Write sorted data blocks
            data_start = f.tell()
            for block in sorted_blocks:
                f.write(block)
            data_end = f.tell()
            
            # Write index block
            index_start = f.tell()
            # Rebuild index entries for sorted data
            current_offset = 0
            for i, block in enumerate(sorted_blocks):
                if i % 16 == 0:  # Create index entry every 16th key
                    key, _, _ = decode_key_value(block)
                    index_entry = encode_index_entry(key, current_offset)
                    f.write(index_entry)
                current_offset += len(block)
            index_end = f.tell()
            
            # Write bloom filter
            bloom_start = f.tell()
            bloom_data = self.bloom_filter.serialize()
            f.write(bloom_data)
            bloom_end = f.tell()
            
            # Calculate key range for metadata
            min_key = b''
            max_key = b''
            if key_value_pairs:
                min_key = key_value_pairs[0][0]  # First key (after sorting)
                max_key = key_value_pairs[-1][0]  # Last key (after sorting)
            
            # Write metadata
            metadata_start = f.tell()
            metadata = encode_metadata(self.key_count, index_start, bloom_start, min_key, max_key)
            f.write(metadata)
            metadata_end = f.tell()
            
            # Write footer with checksums (now includes metadata offset)
            footer = struct.pack('>QQQQQ', data_start, data_end, index_start, index_end, metadata_start)
            f.write(footer)


class SSTableReader:
    """Reader for SSTable files"""
    
    def __init__(self, file_path: str):
        """
        Initialize SSTable reader.
        
        Args:
            file_path: Path to SSTable file
        """
        self.file_path = file_path
        self._metadata = None
        self._bloom_filter = None
        self._index_entries = None
        self._file_size = 0
        self._key_range = None  # Cache for key range
        
        if os.path.exists(file_path):
            self._load_metadata()
    
    def _load_metadata(self) -> None:
        """Load metadata from SSTable file"""
        with open(self.file_path, 'rb') as f:
            # Get file size
            f.seek(0, 2)
            self._file_size = f.tell()
            f.seek(0)
            
            # Check if file is too small to contain valid SSTable
            if self._file_size < 40:  # 40 (footer) + 32 (minimum metadata)
                self._metadata = (0, 0, 0, b'', b'')
                self._bloom_filter = None
                self._index_entries = []
                self._key_range = (b'', b'')
                return
            
            # Read footer (last 40 bytes)
            f.seek(self._file_size - 40)
            footer = f.read(40)
            
            if len(footer) < 40:
                self._metadata = (0, 0, 0, b'', b'')
                self._bloom_filter = None
                self._index_entries = []
                self._key_range = (b'', b'')
                return
            
            try:
                data_start, data_end, index_start, index_end, metadata_start = struct.unpack('>QQQQQ', footer)
            except struct.error:
                self._metadata = (0, 0, 0, b'', b'')
                self._bloom_filter = None
                self._index_entries = []
                self._key_range = (b'', b'')
                return
            
            # Read metadata
            metadata_size = self._file_size - 40 - metadata_start  # 40 bytes for new footer
            f.seek(metadata_start)
            metadata_data = f.read(metadata_size)
            
            if len(metadata_data) < 24:
                self._metadata = (0, 0, 0, b'', b'')
                self._bloom_filter = None
                self._index_entries = []
                self._key_range = (b'', b'')
                return
            
            self._metadata = decode_metadata(metadata_data)
            
            # Load bloom filter
            key_count, index_offset, bloom_filter_offset, min_key, max_key = self._metadata
            self._key_range = (min_key, max_key)
            
            if bloom_filter_offset > index_offset and bloom_filter_offset <= self._file_size:
                f.seek(bloom_filter_offset)
                bloom_data = f.read(metadata_start - bloom_filter_offset)  # Read until metadata
                if bloom_data:
                    try:
                        self._bloom_filter = BloomFilter.deserialize(bloom_data)
                    except Exception:
                        self._bloom_filter = None
                else:
                    self._bloom_filter = None
            else:
                self._bloom_filter = None
            
            # Load index entries
            if index_offset < bloom_filter_offset and index_offset <= self._file_size:
                f.seek(index_offset)
                index_data = f.read(bloom_filter_offset - index_offset)
                self._index_entries = self._parse_index_entries(index_data)
            else:
                self._index_entries = []
    
    def _parse_index_entries(self, index_data: bytes) -> List[Tuple[bytes, int]]:
        """Parse index entries from index data"""
        entries = []
        offset = 0
        
        while offset < len(index_data):
            try:
                key, data_offset, new_offset = decode_index_entry(index_data, offset)
                entries.append((key, data_offset))
                offset = new_offset
            except Exception:
                break
        
        return entries
    
    def get(self, key: bytes) -> Optional[bytes]:
        """
        Get value for key.
        
        Args:
            key: Key to lookup
            
        Returns:
            Value bytes if found, None otherwise
        """
        if not self._bloom_filter or not self._bloom_filter.contains(key):
            return None
        
        # Find appropriate index entry
        index_entry = None
        for idx_key, data_offset in self._index_entries:
            if idx_key <= key:
                index_entry = (idx_key, data_offset)
            else:
                break
        
        if not index_entry:
            return None
        
        # Read from data section
        with open(self.file_path, 'rb') as f:
            # Read footer to get data section bounds
            f.seek(self._file_size - 40)
            footer = f.read(40)
            data_start, data_end, _, _, _ = struct.unpack('>QQQQQ', footer)
            
            # Start reading from index entry offset
            f.seek(data_start + index_entry[1])
            
            # Read until we find the key or exhaust the data section
            while f.tell() < data_end:
                # Read key-value pair using varint decoding
                try:
                    # Read key length (varint)
                    key_len_data = f.read(1)
                    if len(key_len_data) < 1:
                        break
                    
                    # Read more bytes if needed for varint
                    varint_bytes = key_len_data
                    while (varint_bytes[-1] & 0x80) != 0 and len(varint_bytes) < 5:
                        next_byte = f.read(1)
                        if len(next_byte) < 1:
                            break
                        varint_bytes += next_byte
                    
                    key_len, _ = decode_varint(varint_bytes, 0)
                    if key_len > 1024 * 1024:  # Sanity check
                        break
                    
                    # Read key
                    key_data = f.read(key_len)
                    if len(key_data) < key_len:
                        break
                    
                    # Read value length (varint)
                    value_len_data = f.read(1)
                    if len(value_len_data) < 1:
                        break
                    
                    # Read more bytes if needed for varint
                    varint_bytes = value_len_data
                    while (varint_bytes[-1] & 0x80) != 0 and len(varint_bytes) < 5:
                        next_byte = f.read(1)
                        if len(next_byte) < 1:
                            break
                        varint_bytes += next_byte
                    
                    value_len, _ = decode_varint(varint_bytes, 0)
                    if value_len > 100 * 1024 * 1024:  # Sanity check
                        break
                    
                    # Read value
                    value_data = f.read(value_len)
                    if len(value_data) < value_len:
                        break
                    
                    # Check if this is our key
                    if key_data == key:
                        return value_data  # Return as-is, including tombstones
                    
                    # If we've passed our key, it's not in this SSTable
                    if key_data > key:
                        break
                        
                except Exception:
                    break
        
        return None
    
    def range_scan(self, start_key: bytes, end_key: bytes) -> Iterator[Tuple[bytes, Optional[bytes]]]:
        """
        Get all key-value pairs in range [start_key, end_key).
        
        Args:
            start_key: Inclusive start key
            end_key: Exclusive end key
            
        Yields:
            Tuple of (key, value) for each pair in range
        """
        with open(self.file_path, 'rb') as f:
            # Read footer to get data section bounds
            f.seek(self._file_size - 40)
            footer = f.read(40)
            data_start, data_end, _, _, _ = struct.unpack('>QQQQQ', footer)
            
            # Start from beginning of data section
            f.seek(data_start)
            
            # Read all key-value pairs using varint decoding
            while f.tell() < data_end:
                try:
                    # Read key length (varint)
                    key_len_data = f.read(1)
                    if len(key_len_data) < 1:
                        break
                    
                    # Read more bytes if needed for varint
                    varint_bytes = key_len_data
                    while (varint_bytes[-1] & 0x80) != 0 and len(varint_bytes) < 5:
                        next_byte = f.read(1)
                        if len(next_byte) < 1:
                            break
                        varint_bytes += next_byte
                    
                    key_len, _ = decode_varint(varint_bytes, 0)
                    if key_len > 1024 * 1024:  # Sanity check
                        break
                    
                    # Read key
                    key_data = f.read(key_len)
                    if len(key_data) < key_len:
                        break
                    
                    # Read value length (varint)
                    value_len_data = f.read(1)
                    if len(value_len_data) < 1:
                        break
                    
                    # Read more bytes if needed for varint
                    varint_bytes = value_len_data
                    while (varint_bytes[-1] & 0x80) != 0 and len(varint_bytes) < 5:
                        next_byte = f.read(1)
                        if len(next_byte) < 1:
                            break
                        varint_bytes += next_byte
                    
                    value_len, _ = decode_varint(varint_bytes, 0)
                    if value_len > 100 * 1024 * 1024:  # Sanity check
                        break
                    
                    # Read value
                    value_data = f.read(value_len)
                    if len(value_data) < value_len:
                        break
                    
                    # Check if key is in range
                    if start_key <= key_data < end_key:
                        # Yield tombstones as-is for compaction to handle
                        yield key_data, value_data
                    
                    # If we've passed end_key, we're done
                    if key_data >= end_key:
                        break
                        
                except Exception:
                    break
    
    def get_all(self) -> Iterator[Tuple[bytes, Optional[bytes]]]:
        """
        Get all key-value pairs in sorted order.
        
        Yields:
            Tuple of (key, value) for each pair
        """
        return self.range_scan(b'', b'\xff' * 1000)  # Very large end key
    
    def get_key_count(self) -> int:
        """Get number of keys in SSTable"""
        if self._metadata:
            return self._metadata[0]
        return 0
    
    def get_file_size(self) -> int:
        """Get SSTable file size in bytes"""
        return self._file_size
    
    def exists(self) -> bool:
        """Check if SSTable file exists"""
        return os.path.exists(self.file_path)
    
    def get_key_range(self) -> Tuple[bytes, bytes]:
        """
        Get the key range (min_key, max_key) of this SSTable.
        
        Returns:
            Tuple of (min_key, max_key). Returns (b'', b'') if SSTable is empty.
        """
        # Return cached key range from metadata
        if self._key_range is not None:
            return self._key_range
        
        # If no key range available, return empty range
        return (b'', b'')