"""
Utility functions for visual formatting and display.
"""

from typing import List
from pathlib import Path


def print_header(title: str):
    """Print a formatted header."""
    print("\n" + "=" * 60)
    print(f"[*] {title}")
    print("=" * 60)


def print_section(title: str):
    """Print a formatted section header."""
    print(f"\n[+] {title}")
    print("-" * 40)


def print_status(message: str, status: str = "[i]"):
    """Print a status message with an indicator."""
    print(f"{status} {message}")


def print_progress(current: int, total: int, prefix: str = "Progress"):
    """Print a simple progress bar."""
    bar_length = 30
    filled_length = int(bar_length * current // total)
    bar = '█' * filled_length + '░' * (bar_length - filled_length)
    percentage = current / total * 100
    print(f"\r{prefix}: [{bar}] {current}/{total} ({percentage:.1f}%)", end='', flush=True)
    if current == total:
        print()  # New line when complete


def display_file_summary(files: List[str]):
    """Display a summary of found files with statistics."""
    print_section("File Summary")
    
    # Count by suffix
    suffix_counts = {}
    total_size = 0
    
    for file_path in files:
        suffix = Path(file_path).suffix
        suffix_counts[suffix] = suffix_counts.get(suffix, 0) + 1
        
        try:
            total_size += Path(file_path).stat().st_size
        except OSError:
            pass
    
    # Display suffix breakdown
    print("[*] Files by type:")
    for suffix, count in sorted(suffix_counts.items()):
        print(f"   {suffix}: {count} files")
    
    # Display total size
    if total_size > 0:
        size_mb = total_size / (1024 * 1024)
        if size_mb > 1024:
            size_gb = size_mb / 1024
            print(f"[*] Total size: {size_gb:.2f} GB")
        else:
            print(f"[*] Total size: {size_mb:.2f} MB")
    
    print(f"[*] Total files: {len(files)}")
