"""
Slurm Gzip Task Generator

A Python program that scans directories for files with specific suffixes and generates
task files for gzip compression operations. It can also auto-run on Slurm using srun.
"""

__author__ = "Rauf Salamzade"
__email__ = "salamzader@gmail.com"

from .main import main

__all__ = ["main"]
