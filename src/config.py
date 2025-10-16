"""
Configuration parameters for the LSM-Tree Key/Value storage system.
"""

import os

# Storage paths
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
WAL_FILE = os.path.join(DATA_DIR, 'wal.log')

# Memtable configuration
MEMTABLE_MAX_SIZE = 5 * 1024 * 1024  # 5MB
MEMTABLE_MAX_ENTRIES = 100000

# SSTable configuration
SSTABLE_BLOCK_SIZE = 64 * 1024  # 64KB
SSTABLE_INDEX_INTERVAL = 16  # Create index entry every 16 keys
SSTABLE_BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.01  # 1%

# LSM-Tree levels configuration
MAX_LEVELS = 7
LEVEL_SIZE_MULTIPLIER = 10  # Each level is 10x larger than previous

# Compaction configuration
COMPACTION_THREAD_COUNT = 1
COMPACTION_BATCH_SIZE = 1000

# WAL configuration
WAL_SYNC_ON_WRITE = True
WAL_MAX_SIZE = 100 * 1024 * 1024  # 100MB

# Performance tuning
READ_BUFFER_SIZE = 64 * 1024  # 64KB
WRITE_BUFFER_SIZE = 64 * 1024  # 64KB

# Network configuration
DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 8080
MAX_REQUEST_SIZE = 10 * 1024 * 1024  # 10MB

# Logging configuration
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
