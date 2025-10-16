"""
Checksum utilities for data integrity validation.
Uses CRC32 for fast checksum calculation.
"""

import zlib
import struct
from typing import Union


def calculate_crc32(data: Union[bytes, bytearray]) -> int:
    """
    Calculate CRC32 checksum for data.
    
    Args:
        data: Data to calculate checksum for
        
    Returns:
        CRC32 checksum as 32-bit unsigned integer
    """
    return zlib.crc32(data) & 0xffffffff


def verify_crc32(data: Union[bytes, bytearray], expected_checksum: int) -> bool:
    """
    Verify data integrity using CRC32 checksum.
    
    Args:
        data: Data to verify
        expected_checksum: Expected CRC32 checksum
        
    Returns:
        True if checksum matches, False otherwise
    """
    actual_checksum = calculate_crc32(data)
    return actual_checksum == expected_checksum


def pack_with_checksum(data: Union[bytes, bytearray]) -> bytes:
    """
    Pack data with its CRC32 checksum.
    
    Args:
        data: Data to pack
        
    Returns:
        Packed data with checksum (4 bytes checksum + data)
    """
    checksum = calculate_crc32(data)
    return struct.pack('>I', checksum) + bytes(data)


def unpack_with_checksum(packed_data: bytes) -> tuple[bytes, bool]:
    """
    Unpack data and verify its CRC32 checksum.
    
    Args:
        packed_data: Packed data with checksum
        
    Returns:
        Tuple of (unpacked_data, is_valid)
    """
    if len(packed_data) < 4:
        return b'', False
    
    # Extract checksum and data
    checksum = struct.unpack('>I', packed_data[:4])[0]
    data = packed_data[4:]
    
    # Verify checksum
    is_valid = verify_crc32(data, checksum)
    
    return data, is_valid
