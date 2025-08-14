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
    
    # Calculate optimal chunking using the same robust logic
    total_files = len(files)
    
    print_status(f"Chunked task file calculation for {total_files} files with {max_jobs} job limit", "[DEBUG]")
    
    # Calculate minimum commands per job needed to stay under max_jobs
    # We need: total_files / commands_per_job <= max_jobs
    # So: commands_per_job >= total_files / max_jobs
    commands_per_job = max(1, (total_files + max_jobs - 1) // max_jobs)
    print_status(f"Initial calculation: {commands_per_job} commands per job", "[DEBUG]")
    
    # Double-check: ensure we don't exceed max_jobs
    actual_jobs = (total_files + commands_per_job - 1) // commands_per_job
    print_status(f"Initial job count: {actual_jobs}", "[DEBUG]")
    
    # If we still exceed max_jobs, increase commands_per_job
    iterations = 0
    while actual_jobs > max_jobs and iterations < 100:  # Safety limit
        commands_per_job += 1
        actual_jobs = (total_files + commands_per_job - 1) // commands_per_job
        iterations += 1
        print_status(f"Iteration {iterations}: {commands_per_job} commands per job -> {actual_jobs} jobs", "[DEBUG]")
    
    if iterations >= 100:
        print_status("WARNING: Exceeded maximum iterations in chunking calculation", "[WARN]")
    
    # Final verification
    actual_jobs = min(max_jobs, (total_files + commands_per_job - 1) // commands_per_job)
    
    print_status(f"Restructuring for SLURM array limit: {max_jobs} max jobs", "[INFO]")
    print_status(f"Files to compress: {total_files}", "[INFO]")
    print_status(f"Jobs to submit: {actual_jobs}", "[INFO]")
    print_status(f"Commands per job: {commands_per_job}", "[INFO]")
    
    # Verify we're under the limit
    if actual_jobs > max_jobs:
        print_status(f"ERROR: Job count {actual_jobs} exceeds limit {max_jobs}", "[ERROR]")
        print_status("This should never happen - please report this bug", "[ERROR]")
        return actual_jobs, 0
    
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
    
    # Final verification that we wrote the expected number of lines
    if commands_written != actual_jobs:
        print_status(f"WARNING: Expected {actual_jobs} jobs but wrote {commands_written} lines", "[WARN]")
        print_status("This may indicate a problem with the chunking logic", "[WARN]")
    
    return actual_jobs, commands_per_job


def generate_task_file(files: List[str], output_file: str = "gzip.cmds", auto_run: bool = False, max_jobs: int = 1000) -> str:
    """
    Generate a task file with gzip commands for the found files.
    Always creates the full task file, and optionally creates chunked versions for auto-run or max-jobs limits.
    
    Args:
        files: List of file paths to compress
        output_file: Name of the output task file
        auto_run: Whether this is for auto-execution (affects chunking behavior)
        max_jobs: Maximum number of jobs to allow in chunked mode
        
    Returns:
        Path to the generated task file (main file, not chunked)
    """
    # Always create the full task file first
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
    
    # If chunking is requested and there are more than max_jobs files, create chunked versions
    if max_jobs and len(files) > max_jobs:
        print_status(f"More than {max_jobs} files detected, creating chunked version for SLURM compatibility", "[INFO]")
        
        # Create chunked version directly in the main output file
        chunked_jobs, chunked_commands = generate_chunked_task_file(files, task_file_path, max_jobs)
        
        # Verify the chunked generation returned valid values
        if chunked_jobs == 0 or chunked_commands == 0:
            print_status("ERROR: Chunked file generation failed", "[ERROR]")
            print_status("Falling back to standard task file", "[ERROR]")
            # Re-generate the standard task file
            return generate_task_file(files, output_file, auto_run=False, max_jobs=None)
        
        # Verify the generated file has the expected number of lines
        try:
            with open(task_file_path, 'r') as f:
                actual_lines = sum(1 for line in f if line.strip() and not line.strip().startswith('#'))
            print_status(f"Generated chunked file has {actual_lines} job lines", "[DEBUG]")
            if actual_lines != chunked_jobs:
                print_status(f"WARNING: Expected {chunked_jobs} jobs but generated file has {actual_lines} lines", "[WARN]")
                print_status("This may indicate a problem with the chunking logic", "[WARN]")
        except Exception as e:
            print_status(f"Could not verify chunked file line count: {e}", "[WARN]")
        
        print_status(f"Task file updated with chunked commands: {chunked_jobs} jobs, {chunked_commands} commands per job", "[INFO]")
        print_status(f"Job array size will be: {chunked_jobs} (within {max_jobs} limit)", "[INFO]")
        
        return task_file_path
    
    # Return just the main file path if no chunking
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
