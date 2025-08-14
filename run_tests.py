#!/usr/bin/env python3
"""
Test runner script for slurm_gzip package.
"""

import sys
import subprocess
import os
from pathlib import Path


def run_tests():
    """Run the test suite."""
    print("[*] Running slurm_gzip test suite...")
    print("=" * 50)
    
    # Check if we're in the right directory
    if not Path("src/slurm_gzip").exists():
        print("[ERROR] Error: Please run this script from the project root directory")
        print("   (where src/slurm_gzip/ exists)")
        sys.exit(1)
    
    # Check if pytest is available
    try:
        import pytest
    except ImportError:
        print("[ERROR] Error: pytest not found. Please install it first:")
        print("   pip install pytest pytest-cov")
        sys.exit(1)
    
    # Run tests with coverage
    cmd = [
        sys.executable, "-m", "pytest",
        "tests/",
        "-v",
        "--cov=src/slurm_gzip",
        "--cov-report=term-missing",
        "--cov-report=html:htmlcov",
        "--tb=short"
    ]
    
    print(f"[*] Running: {' '.join(cmd)}")
    print()
    
    try:
        result = subprocess.run(cmd, check=True)
        print("\n[OK] All tests passed!")
        
        # Show coverage summary
        if Path("htmlcov").exists():
            print("\n[*] Coverage report generated in htmlcov/ directory")
            print("   Open htmlcov/index.html in your browser to view detailed coverage")
        
        return 0
        
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] Tests failed with exit code {e.returncode}")
        return e.returncode


def run_specific_tests(test_pattern=None):
    """Run specific tests based on pattern."""
    if test_pattern:
        print(f"[*] Running tests matching: {test_pattern}")
        cmd = [
            sys.executable, "-m", "pytest",
            f"tests/test_{test_pattern}.py",
            "-v",
            "--tb=short"
        ]
    else:
        return run_tests()
    
    try:
        subprocess.run(cmd, check=True)
        print("\n[OK] Tests completed successfully!")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] Tests failed with exit code {e.returncode}")
        return e.returncode


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Run specific test module
        test_module = sys.argv[1]
        sys.exit(run_specific_tests(test_module))
    else:
        # Run all tests
        sys.exit(run_tests())
