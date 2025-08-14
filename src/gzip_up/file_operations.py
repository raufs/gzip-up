"""
File operations for scanning directories and generating task files.
"""

import os
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Set
import tempfile
import time

from .utils import print_status, print_progress


def find_files_with_suffixes(directory: str, suffixes: Set[str]) -> List[str]:
    """
    Recursively find all files with the specified suffixes in the directory.
    
    Args:
        directory: Root directory to search
        suffixes: Set of file suffixes to look for
        
    Returns:
        List of file paths matching the suffixes
    """
    matching_files = []
    
    print_status(f"Scanning directory: {os.path.abspath(directory)}")
    print_status(f"Looking for suffixes: {', '.join(sorted(suffixes))}")
    
    try:
        # Count total files for progress tracking
        total_files = sum(len(files) for _, _, files in os.walk(directory))
        scanned_files = 0
        
        for root, dirs, files in os.walk(directory):
            for file in files:
                scanned_files += 1
                print_progress(scanned_files, total_files, "Scanning files")
                
                file_path = os.path.join(root, file)
                # Check if file ends with any of the specified suffixes
                if any(file.endswith(suffix) for suffix in suffixes):
                    matching_files.append(file_path)
        
        print()  # Clear progress bar
        print_status(f"Scan complete! Found {len(matching_files)} matching files out of {total_files} total files")
        
    except Exception as e:
        print(f"\n[ERROR] Error scanning directory {directory}: {e}")
        return []
    
    return sorted(matching_files)


def generate_chunked_task_file(files: List[str], output_file: str = "gzip.cmds", max_jobs: int = 1000):
    """
    Generate a chunked task file when there are more than max_jobs files.
    Each line contains multiple gzip commands separated by semicolons.
    
    Args:
        files: List of file paths to compress
        output_file: Name of the output task file
        max_jobs: Maximum number of SLURM array jobs allowed
        
    Returns:
        Tuple of (task_file_path, actual_jobs, commands_per_job)
    """
    task_file_path = os.path.abspath(output_file)
    
    # Calculate optimal chunking
    total_files = len(files)
    commands_per_job = max(1, total_files // max_jobs)
    actual_jobs = (total_files + commands_per_job - 1) // max_jobs
    
    # Ensure we don't exceed max_jobs
    if actual_jobs > max_jobs:
        commands_per_job = max(1, total_files // max_jobs)
        actual_jobs = max_jobs
    
    print_status(f"Restructuring for SLURM array limit: {max_jobs} max jobs", "[INFO]")
    print_status(f"Files to compress: {total_files}", "[INFO]")
    print_status(f"Jobs to submit: {actual_jobs}", "[INFO]")
    print_status(f"Commands per job: {commands_per_job}", "[INFO]")
    
    skipped_messages = []
    skipped_already_compressed_count = 0
    skipped_existing_gz_count = 0
    commands_written = 0
    
    with open(task_file_path, 'w') as f:
        for i in range(0, total_files, commands_per_job):
            chunk = files[i:i + commands_per_job]
            chunk_commands = []
            
            for file_path in chunk:
                # Skip if file is already compressed
                if file_path.endswith('.gz'):
                    skipped_already_compressed_count += 1
                    continue
                
                # Skip if a .gz counterpart already exists
                gz_counterpart = f"{file_path}.gz"
                if os.path.exists(gz_counterpart):
                    skipped_existing_gz_count += 1
                    skipped_messages.append(
                        f"file {file_path} skipped because {gz_counterpart} already exists"
                    )
                    continue
                
                # Add gzip command to chunk
                chunk_commands.append(f"gzip '{file_path}'")
            
            # Write chunk if it contains any commands
            if chunk_commands:
                combined_cmd = "; ".join(chunk_commands)
                f.write(f"{combined_cmd}\n")
                commands_written += 1
            
            # Show progress
            print_progress(i + len(chunk), total_files, "Writing chunked commands")
    
    print()  # Clear progress bar
    
    # Write skipped log if there were any skips due to existing .gz counterparts
    if skipped_messages:
        skipped_log_path = f"{task_file_path}.skipped.log"
        try:
            with open(skipped_log_path, 'w') as logf:
                for msg in skipped_messages:
                    logf.write(msg + "\n")
            print_status(
                f"Logged {len(skipped_messages)} skipped files (existing .gz) to {skipped_log_path}",
                "[WARN]"
            )
        except Exception as e:
            print_status(f"Failed to write skipped log: {e}", "[ERROR]")
    
    if skipped_already_compressed_count > 0:
        print_status(f"Skipped {skipped_already_compressed_count} already compressed files", "[WARN]")
    if skipped_existing_gz_count > 0:
        print_status(f"Skipped {skipped_existing_gz_count} files because a .gz counterpart exists", "[WARN]")
    
    print_status(f"Chunked task file created with {commands_written} job chunks", "[OK]")
    
    return task_file_path, actual_jobs, commands_per_job


def generate_task_file(files: List[str], output_file: str = "gzip.cmds", auto_run: bool = False) -> str:
    """
    Generate a task file with gzip commands for the found files.
    Automatically uses chunked approach if more than 1000 files and auto_run is True.
    
    Args:
        files: List of file paths to compress
        output_file: Name of the output task file
        auto_run: Whether this is for auto-execution (affects chunking behavior)
        
    Returns:
        Path to the generated task file
    """
    # Use chunked approach if more than 1000 files AND auto_run is requested
    if len(files) > 1000 and auto_run:
        print_status("More than 1000 files detected with --auto-run, using chunked approach for SLURM compatibility", "[INFO]")
        # Create temporary directory for chunked files
        import tempfile
        temp_dir = tempfile.mkdtemp(prefix="gzip_up_chunks_")
        temp_output_file = os.path.join(temp_dir, os.path.basename(output_file))
        
        task_file_path, _, _ = generate_chunked_task_file(files, temp_output_file)
        
        # Store temp directory info for cleanup later
        task_file_path = temp_output_file
        # Add a marker file to indicate this is a temp directory
        with open(os.path.join(temp_dir, ".gzip_up_temp"), 'w') as f:
            f.write(f"Original output: {output_file}\n")
            f.write(f"Created: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        print_status(f"Chunked files created in temporary directory: {temp_dir}", "[INFO]")
        return task_file_path
    
    # Original logic for 1000 or fewer files, or when auto_run is False
    task_file_path = os.path.abspath(output_file)
    
    print_status(f"Generating task file: {task_file_path}")
    
    skipped_messages = []
    skipped_already_compressed_count = 0
    skipped_existing_gz_count = 0
    
    with open(task_file_path, 'w') as f:
        for i, file_path in enumerate(files):
            # Skip if file is already compressed
            if file_path.endswith('.gz'):
                skipped_already_compressed_count += 1
                print_progress(i + 1, len(files), "Writing commands")
                continue
            
            # Skip if a .gz counterpart already exists in the same directory
            gz_counterpart = f"{file_path}.gz"
            if os.path.exists(gz_counterpart):
                skipped_existing_gz_count += 1
                skipped_messages.append(
                    f"file {file_path} skipped because {gz_counterpart} already exists"
                )
                print_progress(i + 1, len(files), "Writing commands")
                continue
            
            # Create gzip command
            gzip_cmd = f"gzip '{file_path}'"
            f.write(f"{gzip_cmd}\n")
            
            # Show progress
            print_progress(i + 1, len(files), "Writing commands")
    
    print()  # Clear progress bar
    
    # Write skipped log if there were any skips due to existing .gz counterparts
    if skipped_messages:
        skipped_log_path = f"{task_file_path}.skipped.log"
        try:
            with open(skipped_log_path, 'w') as logf:
                for msg in skipped_messages:
                    logf.write(msg + "\n")
            print_status(
                f"Logged {len(skipped_messages)} skipped files (existing .gz) to {skipped_log_path}",
                "[WARN]"
            )
        except Exception as e:
            print_status(f"Failed to write skipped log: {e}", "[ERROR]")
    
    commands_written = len(files) - skipped_already_compressed_count - skipped_existing_gz_count
    if skipped_already_compressed_count > 0:
        print_status(f"Skipped {skipped_already_compressed_count} already compressed files", "[WARN]")
    if skipped_existing_gz_count > 0:
        print_status(f"Skipped {skipped_existing_gz_count} files because a .gz counterpart exists", "[WARN]")
    
    print_status(f"Task file created with {commands_written} gzip commands", "[OK]")
    
    return task_file_path


def execute_gzip_local(files: List[str], num_threads: int = 1) -> dict:
    """
    Execute gzip operations locally using threading.
    
    Args:
        files: List of file paths to compress
        num_threads: Number of threads to use (0 for auto-detect)
        
    Returns:
        Dictionary with execution results
    """
    if num_threads == 0:
        # Auto-detect based on available cores (but cap at 8 for I/O operations)
        import multiprocessing
        num_threads = min(multiprocessing.cpu_count(), 8)
    
    if num_threads < 1:
        num_threads = 1
    
    print_status(f"Starting local gzip execution with {num_threads} threads")
    
    # Filter out already compressed files
    uncompressed_files = [f for f in files if not f.endswith('.gz')]
    skipped_count = len(files) - len(uncompressed_files)
    
    if skipped_count > 0:
        print_status(f"Skipped {skipped_count} already compressed files", "[WARN]")
    
    if not uncompressed_files:
        print_status("No files to compress", "[WARN]")
        return {
            'total': len(files),
            'compressed': 0,
            'skipped': skipped_count,
            'errors': 0,
            'error_files': []
        }
    
    results = {
        'total': len(files),
        'compressed': 0,
        'skipped': skipped_count,
        'errors': 0,
        'error_files': []
    }
    
    # Thread-safe counter for progress
    completed_lock = threading.Lock()
    completed_count = 0
    
    def compress_file(file_path: str) -> tuple:
        """Compress a single file using gzip."""
        nonlocal completed_count
        
        try:
            # Run gzip command
            result = subprocess.run(
                ['gzip', file_path],
                capture_output=True,
                text=True,
                check=True
            )
            
            # Update progress
            with completed_lock:
                completed_count += 1
                print_progress(completed_count, len(uncompressed_files), "Compressing files")
            
            return (file_path, True, None)
            
        except subprocess.CalledProcessError as e:
            # Update progress
            with completed_lock:
                completed_count += 1
                print_progress(completed_count, len(uncompressed_files), "Compressing files")
            
            return (file_path, False, str(e))
        except Exception as e:
            # Update progress
            with completed_lock:
                completed_count += 1
                print_progress(completed_count, len(uncompressed_files), "Compressing files")
            
            return (file_path, False, str(e))
    
    # Execute compression using thread pool
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit all tasks
        future_to_file = {
            executor.submit(compress_file, file_path): file_path 
            for file_path in uncompressed_files
        }
        
        # Process completed tasks
        for future in as_completed(future_to_file):
            file_path, success, error = future.result()
            
            if success:
                results['compressed'] += 1
            else:
                results['errors'] += 1
                results['error_files'].append((file_path, error))
    
    print()  # Clear progress bar
    
    # Print summary
    print_status(f"Local execution complete!", "[OK]")
    print_status(f"Files compressed: {results['compressed']}", "[OK]")
    print_status(f"Files skipped: {results['skipped']}", "[WARN]")
    
    if results['errors'] > 0:
        print_status(f"Errors encountered: {results['errors']}", "[ERROR]")
        for file_path, error in results['error_files'][:5]:  # Show first 5 errors
            print_status(f"  {file_path}: {error}", "[ERROR]")
        if len(results['error_files']) > 5:
            print_status(f"  ... and {len(results['error_files']) - 5} more errors", "[ERROR]")
    
    return results
