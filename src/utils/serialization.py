"""
Serialization utilities for key-value pairs and metadata.
Handles variable-length encoding for efficient storage.
"""

import struct
from typing import Tuple, Iterator


def encode_varint(value: int) -> bytes:
    """
    Encode integer using variable-length encoding (similar to protobuf varint).
    Uses 7 bits per byte, with MSB indicating continuation.
    
    Args:
        value: Integer to encode
        
    Returns:
        Encoded bytes
    """
    if value < 0:
        raise ValueError("Varint encoding only supports non-negative integers")
    
    result = bytearray()
    while value >= 0x80:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value & 0x7F)
    return bytes(result)


def decode_varint(data: bytes, offset: int = 0) -> Tuple[int, int]:
    """
    Decode variable-length encoded integer.
    
    Args:
        data: Data containing varint
        offset: Starting offset in data
        
    Returns:
        Tuple of (decoded_value, new_offset)
    """
    result = 0
    shift = 0
    pos = offset
    
    while pos < len(data):
        byte = data[pos]
        result |= (byte & 0x7F) << shift
        pos += 1
        
        if (byte & 0x80) == 0:
            break
        shift += 7
    
    return result, pos


def encode_key_value(key: bytes, value: bytes) -> bytes:
    """
    Encode key-value pair with length prefixes.
    
    Args:
        key: Key bytes
        value: Value bytes
        
    Returns:
        Encoded key-value pair
    """
    key_len = encode_varint(len(key))
    value_len = encode_varint(len(value))
    return key_len + key + value_len + value


def decode_key_value(data: bytes, offset: int = 0) -> Tuple[bytes, bytes, int]:
    """
    Decode key-value pair from data.
    
    Args:
        data: Data containing encoded key-value pair
        offset: Starting offset in data
        
    Returns:
        Tuple of (key, value, new_offset)
    """
    # Decode key length
    key_len, pos = decode_varint(data, offset)
    
    # Extract key
    key = data[pos:pos + key_len]
    pos += key_len
    
    # Decode value length
    value_len, pos = decode_varint(data, pos)
    
    # Extract value
    value = data[pos:pos + value_len]
    pos += value_len
    
    return key, value, pos


def encode_metadata(key_count: int, index_offset: int, bloom_filter_offset: int, min_key: bytes = b'', max_key: bytes = b'') -> bytes:
    """
    Encode SSTable metadata.
    
    Args:
        key_count: Number of key-value pairs
        index_offset: Offset to index block
        bloom_filter_offset: Offset to bloom filter block
        min_key: Minimum key in SSTable
        max_key: Maximum key in SSTable
        
    Returns:
        Encoded metadata
    """
    # Format: [key_count][index_offset][bloom_filter_offset][min_key_len][min_key][max_key_len][max_key]
    min_key_len = len(min_key)
    max_key_len = len(max_key)
    
    return struct.pack('>QQQII', key_count, index_offset, bloom_filter_offset, min_key_len, max_key_len) + min_key + max_key


def decode_metadata(data: bytes) -> Tuple[int, int, int, bytes, bytes]:
    """
    Decode SSTable metadata.
    
    Args:
        data: Data containing metadata
        
    Returns:
        Tuple of (key_count, index_offset, bloom_filter_offset, min_key, max_key)
    """
    if len(data) < 32:  # Minimum size for new format
        return 0, 0, 0, b'', b''
    
    try:
        key_count, index_offset, bloom_filter_offset, min_key_len, max_key_len = struct.unpack('>QQQII', data[:32])
        
        if len(data) >= 32 + min_key_len + max_key_len:
            min_key = data[32:32 + min_key_len]
            max_key = data[32 + min_key_len:32 + min_key_len + max_key_len]
            return key_count, index_offset, bloom_filter_offset, min_key, max_key
        else:
            return 0, 0, 0, b'', b''
    except struct.error:
        return 0, 0, 0, b'', b''


def encode_index_entry(key: bytes, offset: int) -> bytes:
    """
    Encode index entry (key -> offset mapping).
    
    Args:
        key: Key bytes
        offset: Offset in SSTable
        
    Returns:
        Encoded index entry
    """
    key_len = encode_varint(len(key))
    offset_bytes = struct.pack('>Q', offset)
    return key_len + key + offset_bytes


def decode_index_entry(data: bytes, offset: int = 0) -> Tuple[bytes, int, int]:
    """
    Decode index entry from data.
    
    Args:
        data: Data containing index entry
        offset: Starting offset in data
        
    Returns:
        Tuple of (key, data_offset, new_offset)
    """
    # Decode key length
    key_len, pos = decode_varint(data, offset)
    
    # Extract key
    key = data[pos:pos + key_len]
    pos += key_len
    
    # Decode data offset
    data_offset = struct.unpack('>Q', data[pos:pos + 8])[0]
    pos += 8
    
    return key, data_offset, pos


def iterate_key_values(data: bytes) -> Iterator[Tuple[bytes, bytes]]:
    """
    Iterate over key-value pairs in encoded data.
    
    Args:
        data: Data containing encoded key-value pairs
        
    Yields:
        Tuple of (key, value) for each pair
    """
    offset = 0
    while offset < len(data):
        key, value, offset = decode_key_value(data, offset)
        yield key, value
