#!/usr/bin/env python3
"""
Gzip-up Task Generator

This program scans directories for files with specific suffixes and generates
a task file for gzip compression operations. It can also auto-run on Slurm using srun.
"""

import os
import sys
import argparse
from typing import List, Set
from rich_argparse import RichHelpFormatter
from .utils import (
    print_header, print_section, print_status, print_progress, 
    display_file_summary
)
from .file_operations import generate_task_file, find_files_with_suffixes
from .slurm_operations import generate_slurm_script, run_on_slurm
from . import __version__


class CustomRichHelpFormatter(RichHelpFormatter):
    """Custom formatter that combines rich-argparse with proper width handling."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.width = 80
        self.max_help_position = 30


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
    →  gzip-up Task Generator v{__version__}
    →  High-Performance File Compression Made Easy
    →  Designed by Rauf Salamzade
    →  Relman Lab, 2025, Stanford University
    →  https://github.com/raufs/gzip-up
    →  License: GNU GPL v3.0
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
        description="Generate gzip task files and optionally run on Slurm or locally using threading",
        formatter_class=CustomRichHelpFormatter,
        epilog="",
        add_help=True
    )
    
    # Configure rich-argparse for better coloring
    parser.formatter_class = CustomRichHelpFormatter # Use the custom formatter
    parser.formatter_class.rich_theme = "dracula"
    parser.formatter_class.rich_console_options = {"force_terminal": True, "color_system": "auto"}
    parser.formatter_class.rich_show_help = True
    # The width and max_help_position are now handled by CustomRichHelpFormatter
    
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
        "Slurm Integration Options",
        "Configure Slurm batch job parameters (uses job arrays for parallelization)"
    )
    slurm_group.add_argument(
        '--slurm', 
        action='store_true',
        help='Generate Slurm batch script'
    )
    slurm_group.add_argument(
        '--auto-run', 
        action='store_true',
        help='Automatically submit to Slurm (requires --slurm)'
    )
    slurm_group.add_argument(
        '--max-jobs',
        type=int,
        help='Maximum number of jobs in task file (enables chunking when exceeded)',
        metavar='N'
    )
    slurm_group.add_argument(
        '--no-chunk',
        action='store_true',
        help='Disable automatic chunking for --auto-run'
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
    if args.auto_run and not args.slurm:
        parser.error("[ERROR] --auto-run requires --slurm")
    
    if not os.path.isdir(args.directory):
        print_status(f"[ERROR] Directory '{args.directory}' does not exist.", "[ERROR]")
        sys.exit(1)
    
    # Validate and normalize suffixes
    try:
        suffixes = validate_suffixes(args.suffixes)
    except ValueError as e:
        print_status(str(e), "[ERROR]")
        sys.exit(1)
    
    print_header("• Gzip-up Task Generator")
    
    # Find matching files
    files = find_files_with_suffixes(args.directory, suffixes)
    
    if not files:
        print_status("[WARN]  No files found with the specified suffixes.", "[WARN]")
        sys.exit(0)
    
    # Display file summary
    display_file_summary(files)
    
    # Generate task file
    print_section("• Task File Generation")
    
    # Check if output files already exist
    if os.path.exists(args.output):
        print_status(f"ERROR: Output file already exists: {args.output}", "[ERROR]")
        print_status("Please remove or rename the existing file to continue", "[ERROR]")
        print_status("This prevents accidentally overwriting previous work", "[ERROR]")
        sys.exit(1)
    
    # Check if SLURM script already exists
    if args.slurm and os.path.exists("gzip_slurm.sh"):
        print_status("ERROR: SLURM script already exists: gzip_slurm.sh", "[ERROR]")
        print_status("Please remove or rename the existing script to continue", "[ERROR]")
        print_status("This prevents accidentally overwriting previous work", "[ERROR]")
        sys.exit(1)
    
    # Determine if we should use chunking
    # Chunking occurs when:
    # 1. --max-jobs is specified AND >max_jobs files, OR
    # 2. --auto-run is specified AND >1000 files (default auto-chunking, unless --no-chunk)
    use_chunking = False
    max_jobs_for_chunking = None
    
    if args.max_jobs and len(files) > args.max_jobs:
        use_chunking = True
        max_jobs_for_chunking = args.max_jobs
        print_status(f"Using chunked approach to respect --max-jobs limit of {args.max_jobs}", "[INFO]")
    elif args.auto_run and len(files) > 1000 and not args.no_chunk:
        # Auto-run with default chunking (can be disabled with --no-chunk)
        use_chunking = True
        max_jobs_for_chunking = 1000
        print_status("Using chunked approach for --auto-run with large file count", "[INFO]")
    elif args.auto_run and len(files) > 1000 and args.no_chunk:
        print_status("Auto-run with large file count but chunking disabled", "[INFO]")
        use_chunking = False
        max_jobs_for_chunking = None
    else:
        print_status("Using standard approach (no chunking)", "[INFO]")
        use_chunking = False
        max_jobs_for_chunking = None
    
    # Only pass max_jobs if we actually want chunking
    if use_chunking:
        result = generate_task_file(files, args.output, args.auto_run, max_jobs_for_chunking)
    else:
        result = generate_task_file(files, args.output, args.auto_run, None)
    
    # Handle return value (now just the file path)
    task_file_path = result
    
    print_status(f"Task file generated: {task_file_path}", "[OK]")
    
    # Count commands in the task file
    try:
        with open(task_file_path, 'r') as f:
            command_count = sum(1 for line in f if line.strip() and not line.strip().startswith('#'))
        print_status(f"Task file contains {command_count} commands", "[INFO]")
    except Exception as e:
        print_status(f"Could not count commands in task file: {e}", "[WARN]")
        command_count = "unknown"
    
    # Execute locally if requested
    if args.local_run:
        print_section("• Local Threading Execution")
        from .file_operations import execute_gzip_local
        results = execute_gzip_local(files, args.threads)
        
        if results['errors'] > 0:
            print_status(f"Local execution completed with {results['errors']} errors", "[WARN]")
        else:
            print_status("Local execution completed successfully!", "[OK]")
    
    # Generate Slurm script if requested
    if args.slurm:
        print_section("• Slurm Script Generation")
        
        # Use the main task file for SLURM
        slurm_task_file = task_file_path
        print_status(f"SLURM will use task file: {slurm_task_file}", "[INFO]")
        
        # Check if this is a chunked file (contains semicolons)
        is_chunked = False
        try:
            with open(task_file_path, 'r') as f:
                for line in f:
                    if line.strip() and ';' in line.strip():
                        is_chunked = True
                        break
        except Exception:
            pass
        
        if is_chunked:
            print_status("Chunked execution detected - task file contains multiple commands per line", "[INFO]")
            print_status("Each SLURM array task will process multiple gzip operations", "[INFO]")
        else:
            print_status("Standard execution - each SLURM array task processes one file", "[INFO]")
        
        if len(files) > 1000 and not is_chunked:
            print_status(f"Large job array will be created: {len(files)} tasks", "[INFO]")
            print_status("Note: Some SLURM clusters may have array size limits", "[WARN]")
        else:
            print_status(f"Job array size: {command_count} tasks", "[INFO]")
        
        slurm_args = {
            'partition': args.partition,
            'nodes': args.nodes,
            'ntasks': args.ntasks,
            'cpus_per_task': args.cpus_per_task,
            'mem': args.mem,
            'mem_per_cpu': args.mem_per_cpu,
            'time': args.time,
            'output': args.output_log,
            'error': args.error_log
        }
        
        # Remove None values
        slurm_args = {k: v for k, v in slurm_args.items() if v is not None}
        
        script_path = generate_slurm_script(files, slurm_args, slurm_task_file)
        
        # Verify SLURM script exists
        if not os.path.exists(script_path):
            print_status(f"ERROR: SLURM script was not created: {script_path}", "[ERROR]")
            sys.exit(1)
        
        if args.auto_run:
            print_section("• Slurm Job Submission")
            print_status("[WARN]  WARNING: About to submit job to Slurm!", "[WARN]")
            print(f"• Task file: {task_file_path}")
            print(f"• Slurm script: {script_path}")
            print()
            print("Please review the generated files above to ensure everything looks correct.")
            
            response = input("• Proceed with submitting to Slurm? (yes/no): ").lower().strip()
            
            if response in ['yes', 'y']:
                print()
                if run_on_slurm(script_path):
                    print_status("[OK] Job submitted successfully!", "[OK]")
                else:
                    print_status("[ERROR] Failed to submit job.", "[ERROR]")
            else:
                print_status("[STOP] Job submission cancelled.", "[STOP]")
        else:
            print_section("• Ready for Manual Execution")
            print_status(f"Task file: {task_file_path}", "•")
            print_status(f"Slurm script: {script_path}", "•")
            print()
            print("To run manually:")
            print(f"  Submit to Slurm: sbatch {script_path}")
            print(f"  Run locally: parallel < {task_file_path}")
            print(f"  Note: SLURM script uses job arrays - each task processes one file from {task_file_path}")
            print(f"  Defaults: partition=short, time=02:00:00, mem-per-cpu=1G")
            print(f"  Output: Single stdout/stderr files for entire job array (gzip_%j.out/err)")
    else:
        print_section("• Ready for Execution")
        print_status(f"Task file: {task_file_path}", "•")
        print()
        print("To run:")
        print(f"  Using threading: gzip-up -s {' '.join(args.suffixes)} --local-run --threads 4")
        print(f"  Using parallel: parallel < {task_file_path}")
        print(f"  Using xargs: xargs -P $(nproc) -a {task_file_path}")
        print(f"  Or run each command individually")
    
    print_header("• Task Complete!")
    print_status("All files generated successfully. Happy compressing!", "•")


if __name__ == "__main__":
    main()
