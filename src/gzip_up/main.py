#!/usr/bin/env python3
"""
Gzip-up Task Generator

This program scans directories for files with specific suffixes and generates
a task file for gzip compression operations. It can also auto-run on Slurm using srun.
"""

import argparse
import os
import sys
from pathlib import Path
from typing import List, Set

from .utils import (
    print_header,
    print_section,
    print_status,
    print_progress,
    display_file_summary,
)
from .file_operations import find_files_with_suffixes, generate_task_file
from .slurm_operations import generate_slurm_script
from . import __version__


def print_logo():
    """Print a cool ASCII art logo."""
    logo = r"""
╔═════════════════════════════════════╗
║           _                         ║
║  __ _ ___(_)_ __        _   _ _ __  ║
║ / _` |_  | | '_ \ _____| | | | '_ \ ║
║| (_| |/ /| | |_) |_____| |_| | |_) |║
║ \__, /___|_| .__/       \__,_| .__/ ║
║ |___/      |_|               |_|    ║
╚═════════════════════════════════════╝
    """
    print(logo)


def print_colored_banner():
    """Print a colorful banner with version info."""
    import datetime
    
    current_year = datetime.datetime.now().year
    banner = f"""
  ->  gzip-up Task Generator v1.0.0
  ->  High-Performance File Compression Made Easy
  ->  Developed by Rauf Salamzade
  ->  Relman Lab, 2025, Stanford University
  ->  https://github.com/raufs/gzip-up
  ->  BSD-3-Clause License
    """
    print(banner)


def print_examples():
    """Print usage examples."""
    print("\nExamples:")
    print("  gzip-up -s .txt .log")
    print("  gzip-up -d /path/to/files -s .txt .log -o my_tasks.cmds")
    print("  gzip-up -s .txt .log --local-run --threads 4")
    print("  gzip-up -s .txt .log --slurm --auto-run")


def validate_suffixes(suffixes: List[str]) -> Set[str]:
    """
    Validate and normalize file suffixes.
    
    Args:
        suffixes: List of file suffixes to validate
        
    Returns:
        Set of normalized suffixes
        
    Raises:
        ValueError: If any suffix is invalid
    """
    normalized_suffixes = set()
    
    for suffix in suffixes:
        # Normalize suffix to start with dot
        if not suffix.startswith('.'):
            normalized_suffix = f'.{suffix}'
        else:
            normalized_suffix = suffix
        
        # Reject .gz suffixes
        if normalized_suffix == '.gz':
                    raise ValueError(
            f"[ERROR] Invalid suffix '{suffix}': Cannot compress already compressed .gz files"
        )
        
        # Reject other compression formats
        compression_formats = {'.gz', '.bz2', '.xz', '.zip', '.tar', '.7z', '.rar'}
        if normalized_suffix in compression_formats:
                    raise ValueError(
            f"[ERROR] Invalid suffix '{suffix}': File appears to already be compressed"
        )
        
        normalized_suffixes.add(normalized_suffix)
    
    return normalized_suffixes


def create_colored_parser():
    """Create a colorful and enhanced argument parser."""
    parser = argparse.ArgumentParser(
        description="[*] Generate gzip task files and optionally run on Slurm",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
[*]  Examples:
  # Basic usage - scan current directory for .txt and .log files
  [*] python -m gzip_up -s .txt .log
  
  # Scan specific directory with custom output file
  [*] python -m gzip_up -d /path/to/files -s .txt .log -o my_tasks.cmds
  
  # Run locally with 4 threads
  [*] python -m gzip_up -d /path/to/files -s .txt .log --local-run --threads 4
  
  # Generate Slurm script with custom parameters
  [*] python -m gzip_up -d /path/to/files -s .txt .log --slurm --partition=short --ntasks=4

[*]  Tips:
  • Use -s to specify file suffixes (e.g., .txt, .log, .csv)
  • Use --local-run for immediate local execution with threading
  • Use --threads 0 for auto-detection of optimal thread count
  • Add --slurm to generate Slurm batch scripts
  • Check generated files before execution
        """,
        add_help=True
    )
    
    # File discovery options
    file_group = parser.add_argument_group(
        "Main Options",
        "Configure which files to find and compress"
    )
    file_group.add_argument(
        '-d', '--directory', 
        default='.',
        help='Directory to scan for files (default: current directory)',
        metavar='DIR'
    )
    file_group.add_argument(
        '-s', '--suffixes', 
        nargs='+', 
        required=True,
        help='File suffixes to look for (e.g., .txt .log .csv)',
        metavar='SUFFIX'
    )
    file_group.add_argument(
        '-o', '--output', 
        default='gzip.cmds',
        help='Output task file name (default: gzip.cmds)',
        metavar='FILE'
    )
    
    # Slurm options
    slurm_group = parser.add_argument_group(
        "[*] Slurm Integration Options",
        "Configure Slurm batch job parameters"
    )
    slurm_group.add_argument(
        '--slurm', 
        action='store_true',
        help='[*] Generate Slurm batch script'
    )
    slurm_group.add_argument(
        '--max-jobs',
        type=int,
        help='[*] Maximum number of jobs in Slurm job array (enables chunking when exceeded)',
        metavar='N'
    )
    
    # Local execution options
    local_group = parser.add_argument_group(
        "Local Execution Options",
        "Configure local threading for gzip operations"
    )
    local_group.add_argument(
        '--threads', 
        type=int, 
        default=1,
        help='Number of threads for local execution (default: 1, use 0 for auto-detect)',
        metavar='N'
    )
    local_group.add_argument(
        '--local-run', 
        action='store_true',
        help='Run gzip operations locally using threading (instead of just generating task file)'
    )
    
    # Slurm-specific arguments
    slurm_params_group = parser.add_argument_group(
        "Slurm Parameters",
        "Customize Slurm job configuration"
    )
    slurm_params_group.add_argument(
        '--partition', 
        help='Slurm partition (e.g., short, long, gpu)',
        metavar='PART'
    )
    slurm_params_group.add_argument(
        '--nodes', 
        help='Number of nodes',
        metavar='N'
    )
    slurm_params_group.add_argument(
        '--ntasks', 
        help='Number of tasks',
        metavar='N'
    )
    slurm_params_group.add_argument(
        '--cpus-per-task', 
        help='CPUs per task',
        metavar='N'
    )
    slurm_params_group.add_argument(
        '--mem', 
        help='Memory per node (e.g., 8G, 16GB)',
        metavar='MEM'
    )
    slurm_params_group.add_argument(
        '--mem-per-cpu', 
        help='Memory per CPU (e.g., 1G, 2GB) - overrides --mem',
        metavar='MEM'
    )
    slurm_params_group.add_argument(
        '--time', 
        help='Time limit (HH:MM:SS)',
        metavar='TIME'
    )
    slurm_params_group.add_argument(
        '--output-log', 
        help='Output log file',
        metavar='FILE'
    )
    slurm_params_group.add_argument(
        '--error-log', 
        help='Error log file',
        metavar='FILE'
    )
    
    return parser


def main():
    # Print logo and banner
    print_logo()
    print_colored_banner()
    
    # Create parser
    parser = create_colored_parser()
    args = parser.parse_args()
    
    # Validate arguments
    if not os.path.isdir(args.directory):
        print_status(f"[ERROR] Directory '{args.directory}' does not exist.", "[ERROR]")
        sys.exit(1)
    
    # Validate and normalize suffixes
    try:
        suffixes = validate_suffixes(args.suffixes)
    except ValueError as e:
        print_status(str(e), "[ERROR]")
        sys.exit(1)
    
    print_header("[*] Gzip-up Task Generator")
    
    # Find matching files
    files = find_files_with_suffixes(args.directory, suffixes)
    
    if not files:
        print_status("[WARN]  No files found with the specified suffixes.", "[WARN]")
        sys.exit(0)
    
    # Display file summary
    display_file_summary(files)
    
    # Generate task file
    print_section("[*] Task File Generation")
    
    # Check if chunking is needed based on max-jobs
    use_chunking = False
    max_jobs_for_chunking = None
    
    if args.max_jobs and len(files) > args.max_jobs:
        use_chunking = True
        max_jobs_for_chunking = args.max_jobs
        print_status(f"Using chunked approach to respect --max-jobs limit of {args.max_jobs}", "[INFO]")
    else:
        print_status("Using standard approach (no chunking)", "[INFO]")
    
    # Generate task file with or without chunking
    if use_chunking:
        task_file_path = generate_task_file(files, args.output, max_jobs_for_chunking)
    else:
        task_file_path = generate_task_file(files, args.output)
    
    print_status(f"Task file generated: {task_file_path}", "[OK]")
    
    # Execute locally if requested
    if args.local_run:
        print_section("[*] Local Threading Execution")
        from .file_operations import execute_gzip_local
        results = execute_gzip_local(files, args.threads)
        
        if results['errors'] > 0:
            print_status(f"Local execution completed with {results['errors']} errors", "[WARN]")
        else:
            print_status("Local execution completed successfully!", "[OK]")
    
    # Generate Slurm script if requested
    if args.slurm:
        print_section("[*] Slurm Script Generation")
        slurm_args = {
            'partition': args.partition,
            'nodes': args.nodes,
            'ntasks': args.ntasks,
            'cpus_per_task': args.cpus_per_task,
            'mem': args.mem,
            'time': args.time,
            'output': args.output_log,
            'error': args.error_log
        }
        
        # Remove None values
        slurm_args = {k: v for k, v in slurm_args.items() if v is not None}
        
        script_path = generate_slurm_script(files, slurm_args)
        
        print_section("[*] Ready for Manual Execution")
        print_status(f"Task file: {task_file_path}", "[*]")
        print_status(f"Slurm script: {script_path}", "[*]")
        print()
        print("To run manually:")
        print(f"  Submit to Slurm: sbatch {script_path}")
        print(f"  Run locally: parallel < {task_file_path}")
        print(f"  Note: SLURM script uses job arrays - each task processes one file from {task_file_path}")
        print(f"  Defaults: partition=short, time=02:00:00, mem-per-cpu=1G")
        print(f"  Output: Single stdout/stderr files for entire job array (gzip_%j.out/err)")
    else:
        print_section("[*] Ready for Execution")
        print_status(f"Task file: {task_file_path}", "[*]")
        print()
        print("To run:")
        print(f"  # Using threading: python -m gzip_up -s {' '.join(args.suffixes)} --local-run --threads 4")
        print(f"  # Using parallel: parallel < {task_file_path}")
        print(f"  # Using xargs: xargs -P $(nproc) -a {task_file_path}")
        print(f"  # Or run each command individually")
    
    print_header("[*] Task Complete!")
    print_status("All files generated successfully. Happy compressing!", "[*]")


if __name__ == "__main__":
    main()
