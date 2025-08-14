#!/usr/bin/env python3
"""
Slurm Gzip Task Generator

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
from .slurm_operations import generate_slurm_script, run_on_slurm


def print_logo():
    """Print a cool ASCII art logo."""
    logo = """
____ _    _  _ ____ _  _    ____ ___  _ ___ 
[__  |    |  | |__/ |\/| __ | __   /  | |__]
___] |___ |__| |  \ |  |    |__]  /__ | |   
    """
    print(logo)


def print_colored_banner():
    """Print a colorful banner with version info."""
    import datetime
    
    current_year = datetime.datetime.now().year
    banner = f"""
    ->  Slurm Gzip Task Generator v1.0.0  🌟
    ->  High-Performance File Compression Made Easy
    ->  Developed by Rauf Salamzade
    ->  Relman Lab, 2025, Stanford University
    ->  https://github.com/raufs/slurm_gzip
    ->  BSD-3-Clause License
    """
    print(banner)


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
                f"❌ Invalid suffix '{suffix}': Cannot compress already compressed .gz files"
            )
        
        # Reject other compression formats
        compression_formats = {'.gz', '.bz2', '.xz', '.zip', '.tar', '.7z', '.rar'}
        if normalized_suffix in compression_formats:
            raise ValueError(
                f"❌ Invalid suffix '{suffix}': File appears to already be compressed"
            )
        
        normalized_suffixes.add(normalized_suffix)
    
    return normalized_suffixes


def create_colored_parser():
    """Create a colorful and enhanced argument parser."""
    parser = argparse.ArgumentParser(
        description="🚀 Generate gzip task files and optionally run on Slurm",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
🎨  Examples:
  # Basic usage - scan current directory for .txt and .log files
  🌟 python -m slurm_gzip -s .txt .log
  
  # Scan specific directory with custom output file
  📁 python -m slurm_gzip -d /path/to/files -s .txt .log -o my_tasks.cmds
  
  # Generate Slurm script with custom parameters
  ⚡ python -m slurm_gzip -d /path/to/files -s .txt .log --slurm --partition=short --ntasks=4
  
  # Auto-run on Slurm (with confirmation)
  🚀 python -m slurm_gzip -d /path/to/files -s .txt .log --slurm --auto-run

🔧  Tips:
  • Use -s to specify file suffixes (e.g., .txt, .log, .csv)
  • Add --slurm to generate Slurm batch scripts
  • Use --auto-run to submit jobs automatically (with confirmation)
  • Check generated files before execution
        """,
        add_help=True
    )
    
    # File discovery options
    file_group = parser.add_argument_group(
        "📁 File Discovery Options",
        "Configure which files to find and compress"
    )
    file_group.add_argument(
        '-d', '--directory', 
        default='.',
        help='📂 Directory to scan for files (default: current directory)',
        metavar='DIR'
    )
    file_group.add_argument(
        '-s', '--suffixes', 
        nargs='+', 
        required=True,
        help='🎯 File suffixes to look for (e.g., .txt .log .csv)',
        metavar='SUFFIX'
    )
    file_group.add_argument(
        '-o', '--output', 
        default='gzip.cmds',
        help='📝 Output task file name (default: gzip.cmds)',
        metavar='FILE'
    )
    
    # Slurm options
    slurm_group = parser.add_argument_group(
        "⚡ Slurm Integration Options",
        "Configure Slurm batch job parameters"
    )
    slurm_group.add_argument(
        '--slurm', 
        action='store_true',
        help='🚀 Generate Slurm batch script'
    )
    slurm_group.add_argument(
        '--auto-run', 
        action='store_true',
        help='🎯 Automatically submit to Slurm (requires --slurm)'
    )
    
    # Slurm-specific arguments
    slurm_params_group = parser.add_argument_group(
        "⚙️  Slurm Parameters",
        "Customize Slurm job configuration"
    )
    slurm_params_group.add_argument(
        '--partition', 
        help='🏗️  Slurm partition (e.g., short, long, gpu)',
        metavar='PART'
    )
    slurm_params_group.add_argument(
        '--nodes', 
        help='🖥️  Number of nodes',
        metavar='N'
    )
    slurm_params_group.add_argument(
        '--ntasks', 
        help='🔢 Number of tasks',
        metavar='N'
    )
    slurm_params_group.add_argument(
        '--cpus-per-task', 
        help='💻 CPUs per task',
        metavar='N'
    )
    slurm_params_group.add_argument(
        '--mem', 
        help='🧠 Memory per node (e.g., 8G, 16GB)',
        metavar='MEM'
    )
    slurm_params_group.add_argument(
        '--time', 
        help='⏰ Time limit (HH:MM:SS)',
        metavar='TIME'
    )
    slurm_params_group.add_argument(
        '--output-log', 
        help='📋 Output log file',
        metavar='FILE'
    )
    slurm_params_group.add_argument(
        '--error-log', 
        help='❌ Error log file',
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
        parser.error("❌ --auto-run requires --slurm")
    
    if not os.path.isdir(args.directory):
        print_status(f"❌ Directory '{args.directory}' does not exist.", "❌")
        sys.exit(1)
    
    # Validate and normalize suffixes
    try:
        suffixes = validate_suffixes(args.suffixes)
    except ValueError as e:
        print_status(str(e), "❌")
        sys.exit(1)
    
    print_header("🚀 Slurm Gzip Task Generator")
    
    # Find matching files
    files = find_files_with_suffixes(args.directory, suffixes)
    
    if not files:
        print_status("⚠️  No files found with the specified suffixes.", "⚠️")
        sys.exit(0)
    
    # Display file summary
    display_file_summary(files)
    
    # Generate task file
    print_section("📝 Task File Generation")
    task_file = generate_task_file(files, args.output)
    
    # Generate Slurm script if requested
    if args.slurm:
        print_section("⚡ Slurm Script Generation")
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
        
        if args.auto_run:
            print_section("🚀 Slurm Job Submission")
            print_status("⚠️  WARNING: About to submit job to Slurm!", "⚠️")
            print(f"📋 Task file: {task_file}")
            print(f"📜 Slurm script: {script_path}")
            print()
            print("Please review the generated files above to ensure everything looks correct.")
            
            response = input("🎯 Proceed with submitting to Slurm? (yes/no): ").lower().strip()
            
            if response in ['yes', 'y']:
                print()
                if run_on_slurm(script_path):
                    print_status("✅ Job submitted successfully!", "✅")
                else:
                    print_status("❌ Failed to submit job.", "❌")
            else:
                print_status("⏹️  Job submission cancelled.", "⏹️")
        else:
            print_section("📋 Ready for Manual Execution")
            print_status(f"Task file: {task_file}", "📋")
            print_status(f"Slurm script: {script_path}", "📜")
            print()
            print("To run manually:")
            print(f"  # Submit to Slurm: sbatch {script_path}")
            print(f"  # Run locally: parallel < {task_file}")
    else:
        print_section("🎯 Ready for Execution")
        print_status(f"Task file: {task_file}", "📋")
        print()
        print("To run:")
        print(f"  # Using parallel: parallel < {task_file}")
        print(f"  # Using xargs: xargs -P $(nproc) -a {task_file}")
        print(f"  # Or run each command individually")
    
    print_header("🎉 Task Complete!")
    print_status("All files generated successfully. Happy compressing! 🎉", "🎯")


if __name__ == "__main__":
    main()
