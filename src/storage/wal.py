"""
Write-Ahead Log (WAL) for crash recovery and durability.
All writes are logged before being applied to memtable.
"""

import os
import struct
import threading
from typing import Iterator, Optional
from utils.checksum import calculate_crc32, unpack_with_checksum


class WALEntry:
    """Represents a single WAL entry"""
    
    def __init__(self, operation: str, key: bytes, value: Optional[bytes] = None):
        self.operation = operation  # 'PUT', 'DELETE'
        self.key = key
        self.value = value
        self.timestamp = self._get_timestamp()
    
    def _get_timestamp(self) -> int:
        """Get current timestamp in microseconds"""
        import time
        return int(time.time() * 1_000_000)
    
    def serialize(self) -> bytes:
        """Serialize WAL entry to bytes"""
        # Format: [operation_len][operation][key_len][key][value_len][value][timestamp]
        operation_bytes = self.operation.encode('utf-8')
        key_len = len(self.key)
        value_len = len(self.value) if self.value else 0
        
        data = struct.pack('>I', len(operation_bytes))
        data += operation_bytes
        data += struct.pack('>I', key_len)
        data += self.key
        data += struct.pack('>I', value_len)
        if self.value:
            data += self.value
        data += struct.pack('>Q', self.timestamp)
        
        return data
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'WALEntry':
        """Deserialize WAL entry from bytes"""
        offset = 0
        
        # Read operation
        op_len = struct.unpack('>I', data[offset:offset + 4])[0]
        offset += 4
        operation = data[offset:offset + op_len].decode('utf-8')
        offset += op_len
        
        # Read key
        key_len = struct.unpack('>I', data[offset:offset + 4])[0]
        offset += 4
        key = data[offset:offset + key_len]
        offset += key_len
        
        # Read value
        value_len = struct.unpack('>I', data[offset:offset + 4])[0]
        offset += 4
        value = data[offset:offset + value_len] if value_len > 0 else None
        offset += value_len
        
        # Read timestamp
        timestamp = struct.unpack('>Q', data[offset:offset + 8])[0]
        offset += 8
        
        entry = cls(operation, key, value)
        entry.timestamp = timestamp
        return entry


class WAL:
    """
    Write-Ahead Log for durability and crash recovery.
    All operations are logged before being applied to memtable.
    """
    
    def __init__(self, wal_file_path: str, sync_on_write: bool = True):
        """
        Initialize WAL.
        
        Args:
            wal_file_path: Path to WAL file
            sync_on_write: Whether to fsync after each write (for durability)
        """
        self.wal_file_path = wal_file_path
        self.sync_on_write = sync_on_write
        self._lock = threading.Lock()
        self._file = None
        self._is_open = False
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(wal_file_path), exist_ok=True)
    
    def open(self) -> None:
        """Open WAL file for writing"""
        with self._lock:
            self._open_unlocked()
    
    def _open_unlocked(self) -> None:
        """Open WAL file for writing without acquiring lock (internal use)"""
        if self._is_open:
            return
        
        # Open file in append mode
        self._file = open(self.wal_file_path, 'ab')
        self._is_open = True
    
    def close(self) -> None:
        """Close WAL file"""
        with self._lock:
            self._close_unlocked()
    
    def _close_unlocked(self) -> None:
        """Close WAL file without acquiring lock (internal use)"""
        if not self._is_open:
            return
        
        if self._file:
            self._file.close()
            self._file = None
        self._is_open = False
    
    def append(self, entry: WALEntry) -> None:
        """
        Append entry to WAL.
        
        Args:
            entry: WAL entry to append
        """
        with self._lock:
            if not self._is_open:
                raise RuntimeError("WAL is not open")
            
            # Serialize entry
            entry_data = entry.serialize()
            
            # Pack with checksum and length
            checksum = calculate_crc32(entry_data)
            length = len(entry_data)
            
            # Write checksum (4 bytes), length (4 bytes), then data
            self._file.write(struct.pack('>I', checksum))
            self._file.write(struct.pack('>I', length))
            self._file.write(entry_data)
            
            # Sync for durability if enabled
            if self.sync_on_write:
                self._file.flush()
                os.fsync(self._file.fileno())
    
    def put(self, key: bytes, value: bytes) -> None:
        """
        Log PUT operation.
        
        Args:
            key: Key bytes
            value: Value bytes
        """
        entry = WALEntry('PUT', key, value)
        self.append(entry)
    
    def delete(self, key: bytes) -> None:
        """
        Log DELETE operation.
        
        Args:
            key: Key bytes
        """
        entry = WALEntry('DELETE', key)
        self.append(entry)
    
    def replay(self) -> Iterator[WALEntry]:
        """
        Replay all entries from WAL file.
        Used for crash recovery.
        
        Yields:
            WALEntry objects in order
        """
        if not os.path.exists(self.wal_file_path):
            print("WAL: File does not exist")
            return
        
        with open(self.wal_file_path, 'rb') as f:
            while True:
                try:
                    # Read checksum (4 bytes)
                    checksum_data = f.read(4)
                    if len(checksum_data) < 4:
                        break
                    
                    # Read entry length (4 bytes)
                    length_data = f.read(4)
                    if len(length_data) < 4:
                        break
                    
                    entry_length = struct.unpack('>I', length_data)[0]
                    
                    # Read entry data
                    entry_data = f.read(entry_length)
                    if len(entry_data) < entry_length:
                        break
                    
                    # Verify checksum
                    packed_data = checksum_data + length_data + entry_data
                    data, is_valid = unpack_with_checksum(packed_data)
                    
                    if not is_valid:
                        # Skip corrupted entry
                        continue
                    
                    try:
                        entry = WALEntry.deserialize(data)
                        yield entry
                    except Exception:
                        # Skip malformed entry
                        continue
                except Exception:
                    break
    
    def clear(self) -> None:
        """Clear WAL file (used after successful flush to SSTable)"""
        with self._lock:
            self._close_unlocked()
            
            # Remove file if it exists
            if os.path.exists(self.wal_file_path):
                os.remove(self.wal_file_path)
            
            # Reopen for new entries
            self._open_unlocked()
    
    def get_size(self) -> int:
        """
        Get WAL file size in bytes.
        
        Returns:
            File size in bytes
        """
        if os.path.exists(self.wal_file_path):
            return os.path.getsize(self.wal_file_path)
        return 0
    
    def __enter__(self):
        """Context manager entry"""
        self.open()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
