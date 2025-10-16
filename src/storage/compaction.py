"""
Background compaction for LSM-Tree to merge SSTables across levels.
Implements size-tiered compaction strategy for optimal performance.
"""

import os
import threading
import time
from typing import List, Optional, Tuple
from .sstable import SSTableReader, SSTableWriter
from config import MAX_LEVELS, LEVEL_SIZE_MULTIPLIER, COMPACTION_THREAD_COUNT


class CompactionManager:
    """
    Manages background compaction of SSTables across LSM-Tree levels.
    Uses size-tiered compaction strategy for optimal write amplification.
    """
    
    def __init__(self, data_dir: str, sstables: List[List[SSTableReader]]):
        """
        Initialize compaction manager.
        
        Args:
            data_dir: Directory for storing data files
            sstables: Reference to SSTables list (shared with LSM engine)
        """
        self.data_dir = data_dir
        self.sstables = sstables
        self._running = False
        self._compaction_threads = []
        self._lock = threading.Lock()
        
        # Compaction statistics
        self._stats = {
            'compactions_completed': 0,
            'sstables_merged': 0,
            'bytes_compacted': 0,
            'last_compaction_time': 0
        }
    
    def start(self) -> None:
        """Start background compaction threads"""
        with self._lock:
            if self._running:
                return
            
            self._running = True
            
            # Start compaction threads
            for i in range(COMPACTION_THREAD_COUNT):
                thread = threading.Thread(target=self._compaction_worker, daemon=True)
                thread.start()
                self._compaction_threads.append(thread)
            
            print(f"Started {COMPACTION_THREAD_COUNT} compaction threads")
    
    def stop(self) -> None:
        """Stop background compaction threads"""
        with self._lock:
            if not self._running:
                return
            
            self._running = False
            
            # Wait for threads to finish
            for thread in self._compaction_threads:
                thread.join(timeout=5.0)
            
            self._compaction_threads.clear()
            print("Stopped compaction threads")
    
    def _compaction_worker(self) -> None:
        """Worker thread for background compaction"""
        while self._running:
            try:
                # Check if compaction is needed
                level_to_compact = self._find_level_to_compact()
                if level_to_compact is not None:
                    self._compact_level(level_to_compact)
                else:
                    # No compaction needed, sleep briefly
                    time.sleep(1.0)
            except Exception as e:
                print(f"Compaction error: {e}")
                time.sleep(5.0)  # Wait longer on error
    
    def _find_level_to_compact(self) -> Optional[int]:
        """
        Find the level that needs compaction.
        Uses size-tiered strategy: compact when level has too many SSTables.
        
        Returns:
            Level number to compact, or None if no compaction needed
        """
        with self._lock:
            # Level 0: compact when we have more than 4 SSTables
            if len(self.sstables[0]) > 4:
                return 0
            
            # Other levels: compact when level size exceeds threshold
            for level in range(1, MAX_LEVELS):
                if len(self.sstables[level]) > 0:
                    # Calculate total size of this level
                    level_size = sum(sstable.get_file_size() for sstable in self.sstables[level])
                    threshold = LEVEL_SIZE_MULTIPLIER ** level * 10 * 1024 * 1024  # 10MB base
                    
                    if level_size > threshold:
                        return level
            
            return None
    
    def _compact_level(self, level: int) -> None:
        """
        Compact SSTables at the specified level.
        
        Args:
            level: Level to compact
        """
        print(f"Starting compaction of level {level}")
        start_time = time.time()
        
        with self._lock:
            if level == 0:
                # Level 0: merge all SSTables into level 1
                # Sort SSTables by creation time (filename timestamp) to ensure chronological order
                sstables_to_compact = sorted(self.sstables[0], key=lambda s: int(os.path.basename(s.file_path).split('.')[0]))
                target_level = 1
            else:
                # Other levels: merge some SSTables into next level
                sstables_to_compact = self._select_sstables_for_compaction(level)
                target_level = level + 1
            
            if not sstables_to_compact:
                return
            
            # Ensure target level directory exists
            target_dir = os.path.join(self.data_dir, f'level_{target_level}')
            os.makedirs(target_dir, exist_ok=True)
            
            # Create merged SSTable
            timestamp = int(time.time() * 1000000)
            merged_sstable_path = os.path.join(target_dir, f'{timestamp}.sst')
            
            # Merge SSTables
            merged_reader = self._merge_sstables(sstables_to_compact, merged_sstable_path)
            
            if merged_reader:
                # Remove old SSTables from source level
                for sstable in sstables_to_compact:
                    if sstable in self.sstables[level]:
                        self.sstables[level].remove(sstable)
                        # Delete old SSTable file
                        if os.path.exists(sstable.file_path):
                            os.remove(sstable.file_path)
                
                # Remove overlapping SSTables from target level
                if target_level < MAX_LEVELS:
                    overlapping_sstables = self._find_overlapping_sstables(merged_reader, target_level)
                    for sstable in overlapping_sstables:
                        if sstable in self.sstables[target_level]:
                            self.sstables[target_level].remove(sstable)
                            # Delete overlapping SSTable file
                            if os.path.exists(sstable.file_path):
                                os.remove(sstable.file_path)
                    
                    # Add merged SSTable to target level
                    self.sstables[target_level].append(merged_reader)
                    
                    if overlapping_sstables:
                        print(f"Removed {len(overlapping_sstables)} overlapping SSTables from level {target_level}")
                
                # Update statistics
                self._stats['compactions_completed'] += 1
                self._stats['sstables_merged'] += len(sstables_to_compact)
                self._stats['bytes_compacted'] += sum(sstable.get_file_size() for sstable in sstables_to_compact)
                self._stats['last_compaction_time'] = time.time()
                
                print(f"Compacted level {level}: merged {len(sstables_to_compact)} SSTables into level {target_level}")
        
        duration = time.time() - start_time
        print(f"Compaction completed in {duration:.2f} seconds")
    
    def _select_sstables_for_compaction(self, level: int) -> List[SSTableReader]:
        """
        Select SSTables to compact at the given level.
        Uses size-tiered strategy: select oldest/largest SSTables.
        
        Args:
            level: Level to select SSTables from
            
        Returns:
            List of SSTables to compact
        """
        sstables = self.sstables[level]
        if len(sstables) <= 1:
            return []
        
        # Sort by file size (largest first) and take half
        sorted_sstables = sorted(sstables, key=lambda s: s.get_file_size(), reverse=True)
        return sorted_sstables[:len(sorted_sstables) // 2]
    
    def _merge_sstables(self, sstables: List[SSTableReader], output_path: str) -> Optional[SSTableReader]:
        """
        Merge multiple SSTables into a single SSTable.
        
        Args:
            sstables: List of SSTables to merge
            output_path: Path for output SSTable
            
        Returns:
            SSTableReader for the merged SSTable, or None on error
        """
        try:
            # Collect all key-value pairs from all SSTables
            all_pairs = []
            
            for sstable in sstables:
                for key, value in sstable.get_all():
                    all_pairs.append((key, value))
            
            # Sort by key
            all_pairs.sort(key=lambda x: x[0])
            
            # Remove duplicates, keeping the LAST occurrence (newest in list order)
            unique_pairs = []
            seen_keys = set()
            
            for key, value in reversed(all_pairs):  # Process in reverse to keep newest
                if key not in seen_keys:
                    seen_keys.add(key)
                    unique_pairs.append((key, value))
            
            unique_pairs.reverse()
            
            # Write merged SSTable
            writer = SSTableWriter(output_path, len(unique_pairs))
            for key, value in unique_pairs:
                writer.add(key, value)
            writer.write()
            
            # Return reader for the new SSTable
            return SSTableReader(output_path)
            
        except Exception as e:
            print(f"Error merging SSTables: {e}")
            return None
    
    def get_stats(self) -> dict:
        """Get compaction statistics"""
        with self._lock:
            stats = self._stats.copy()
            stats['running'] = self._running
            stats['thread_count'] = len(self._compaction_threads)
            return stats
    
    def _ranges_overlap(self, range1: Tuple[bytes, bytes], range2: Tuple[bytes, bytes]) -> bool:
        """
        Check if two key ranges overlap.
        
        Args:
            range1: Tuple of (min_key, max_key) for first range
            range2: Tuple of (min_key, max_key) for second range
            
        Returns:
            True if ranges overlap, False otherwise
        """
        min1, max1 = range1
        min2, max2 = range2
        
        # Handle empty ranges
        if not min1 or not max1 or not min2 or not max2:
            return False
        
        # Two ranges overlap if: min1 <= max2 AND min2 <= max1
        return min1 <= max2 and min2 <= max1
    
    def _find_overlapping_sstables(self, target_sstable: SSTableReader, level: int) -> List[SSTableReader]:
        """
        Find SSTables in the given level that have overlapping key ranges with the target SSTable.
        
        Args:
            target_sstable: SSTable to check for overlaps
            level: Level to search in
            
        Returns:
            List of SSTables that overlap with the target SSTable
        """
        if level >= len(self.sstables) or level < 0:
            return []
        
        target_range = target_sstable.get_key_range()
        if not target_range[0] or not target_range[1]:  # Empty range
            return []
        
        overlapping = []
        for sstable in self.sstables[level]:
            if sstable == target_sstable:  # Skip self
                continue
            
            sstable_range = sstable.get_key_range()
            if self._ranges_overlap(target_range, sstable_range):
                overlapping.append(sstable)
        
        return overlapping
    
    def force_compaction(self) -> None:
        """Force immediate compaction of all levels"""
        print("Forcing compaction of all levels...")
        
        for level in range(MAX_LEVELS):
            if len(self.sstables[level]) > 0:
                self._compact_level(level)
    
    def __enter__(self):
        """Context manager entry"""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop()
