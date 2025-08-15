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
from rich_argparse import RichHelpFormatter

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
        description="Generate (de)compression command/task list file and optionally a Slurm script.",
        formatter_class=CustomRichHelpFormatter,
        add_help=True
    )
    
    # Configure rich-argparse for better coloring
    parser.formatter_class = CustomRichHelpFormatter
    parser.formatter_class.rich_theme = "dracula"
    parser.formatter_class.rich_console_options = {"force_terminal": True, "color_system": "auto"}
    parser.formatter_class.rich_show_help = True
    
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
        nargs='*', 
        required=False,
        help='File suffixes to look for (e.g., .fastq .fastq.gz .txt .log). Not required with --gunzip, --sam-to-bam, or --bam-to-sam modes.',
        metavar='SUFFIX'
    )
    file_group.add_argument(
        '-o', '--output', 
        default='gzip.cmds',
        help='Output task file name (default: gzip.cmds)',
        metavar='FILE'
    )
    
    # Modes
    modes_group = parser.add_argument_group(
        "Modes",
        "Choose between compression, decompression, or SAM/BAM conversion"
    )
    modes_group.add_argument(
        '--gunzip',
        action='store_true',
        help='Decompress .gz files instead of compressing (ignores -s/--suffixes)'
    )
    modes_group.add_argument(
        '--sam-to-bam',
        action='store_true',
        help='Convert SAM files to BAM format using samtools'
    )
    modes_group.add_argument(
        '--bam-to-sam',
        action='store_true',
        help='Convert BAM files to SAM format using samtools'
    )

    # Slurm options
    slurm_group = parser.add_argument_group(
        "Slurm Integration Options",
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
    
    # Examples and Tips
    examples_group = parser.add_argument_group(
        "Examples",
        "Common usage patterns"
    )
    examples_group.add_argument(
        '--show-examples',
        action='store_true',
        help='Show detailed examples and exit'
    )
    

    
    return parser


def main():
    # Print logo and banner
    print_logo()
    print_colored_banner()
    
    # Create parser
    parser = create_colored_parser()
    args = parser.parse_args()
    
    # Handle examples display
    if hasattr(args, 'show_examples') and args.show_examples:
        examples_help = """Examples:
  # Basic usage - scan current directory for .fastq files
  gzip-up -s .fastq
  
  # Scan specific directory with custom output file
  gzip-up -d /path/to/files -s .fastq .fastq.gz -o my_tasks.cmds
  
  # Run locally with 4 threads
  gzip-up -d /path/to/files -s .fastq --local-run --threads 4
  
  # Generate Slurm script with custom parameters
  gzip-up -d /path/to/files -s .fastq .fastq.gz --slurm --partition=short --ntasks=4
  
  # Decompress .gz files
  gzip-up --gunzip --local-run --threads 4
  
  # Convert SAM to BAM
  gzip-up --sam-to-bam --local-run --threads 4
  
  # Convert BAM to SAM
  gzip-up --bam-to-sam --local-run --threads 4

Tips:
  • Use -s to specify file suffixes (e.g., .fastq, .fastq.gz, .txt, .log)
  • Use --gunzip to decompress .gz files (ignores -s/--suffixes)
  • Use --sam-to-bam or --bam-to-sam for SAM/BAM conversion
  • Use --local-run for immediate local execution with threading
  • Use --threads 0 for auto-detection of optimal thread count
  • Add --slurm to generate Slurm batch scripts
  • Check generated files before execution"""
        
        print_header("[*] Examples and Usage")
        print(examples_help)
        sys.exit(0)
    
    # Determine operation mode
    operation_mode = "gzip"
    if args.gunzip:
        operation_mode = "gunzip"
    elif args.sam_to_bam:
        operation_mode = "sam_to_bam"
    elif args.bam_to_sam:
        operation_mode = "bam_to_sam"
    

    
    # Check if suffixes are provided (not required for certain modes)
    if not args.suffixes and operation_mode == "gzip":
        print_status("[ERROR] File suffixes (-s/--suffixes) are required for gzip mode.", "[ERROR]")
        print_status("Use --show-examples to see usage examples.", "[INFO]")
        sys.exit(1)
    
    # Validate arguments
    if not os.path.isdir(args.directory):
        print_status(f"[ERROR] Directory '{args.directory}' does not exist.", "[ERROR]")
        sys.exit(1)
    
    # Determine suffixes based on mode
    if operation_mode == "gunzip":
        suffixes = {".gz"}
    elif operation_mode == "sam_to_bam":
        suffixes = {".sam"}
    elif operation_mode == "bam_to_sam":
        suffixes = {".bam"}
    else:
        # Validate and normalize user-provided suffixes
        try:
            suffixes = validate_suffixes(args.suffixes)
        except ValueError as e:
            print_status(str(e), "[ERROR]")
            sys.exit(1)
    
    print_header(f"[*] Gzip-up Task Generator - {operation_mode.upper()} Mode")
    
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
        task_file_path = generate_task_file(files, args.output, max_jobs_for_chunking, operation_mode, args)
    else:
        task_file_path = generate_task_file(files, args.output, operation_mode=operation_mode, mode_args=args)
    
    print_status(f"Task file generated: {task_file_path}", "[OK]")
    
    # Execute locally if requested
    if args.local_run:
        print_section("[*] Local Threading Execution")
        from .file_operations import execute_gzip_local
        results = execute_gzip_local(files, args.threads, operation_mode, args)
        
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
        
        script_path = generate_slurm_script(files, slurm_args, operation_mode)
        
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
        if operation_mode == "gzip":
            print(f"  # Using threading: gzip-up -s {' '.join(args.suffixes)} --local-run --threads 4")
        elif operation_mode == "gunzip":
            print(f"  # Using threading: gzip-up --gunzip --local-run --threads 4")
        elif operation_mode == "sam_to_bam":
            print(f"  # Using threading: gzip-up --sam-to-bam --local-run --threads 4")
        elif operation_mode == "bam_to_sam":
            print(f"  # Using threading: gzip-up --bam-to-sam --local-run --threads 4")
        
        print(f"  # Using parallel: parallel < {task_file_path}")
        print(f"  # Using xargs: xargs -P $(nproc) -a {task_file_path}")
        print(f"  # Or run each command individually")
    
    print_header("[*] Task Complete!")
    print_status("All files generated successfully. Happy processing!", "[*]")


if __name__ == "__main__":
    main()
