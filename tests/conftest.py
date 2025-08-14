"""
Shared test fixtures and configuration for slurm_gzip tests.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
import os


@pytest.fixture
def sample_files(tmp_path):
    """Create sample files for testing."""
    # Create test files with different extensions
    files = {
        'txt': ['file1.txt', 'file2.txt', 'subdir/file3.txt'],
        'log': ['app.log', 'error.log'],
        'csv': ['data.csv', 'results.csv'],
        'gz': ['compressed.gz'],  # Already compressed
        'py': ['script.py']  # Different extension
    }
    
    # Create subdirectory
    subdir = tmp_path / "subdir"
    subdir.mkdir()
    
    # Create files
    created_files = []
    for ext, filenames in files.items():
        for filename in filenames:
            file_path = tmp_path / filename
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(f"Sample content for {filename}")
            created_files.append(str(file_path))
    
    return {
        'files': created_files,
        'txt_files': [f for f in created_files if f.endswith('.txt')],
        'log_files': [f for f in created_files if f.endswith('.log')],
        'csv_files': [f for f in created_files if f.endswith('.csv')],
        'gz_files': [f for f in created_files if f.endswith('.gz')],
        'py_files': [f for f in created_files if f.endswith('.py')],
        'root_dir': str(tmp_path)
    }


@pytest.fixture
def large_file(tmp_path):
    """Create a large file for testing size calculations."""
    large_file_path = tmp_path / "large_file.txt"
    
    # Create a file with ~1MB of content
    content = "This is a test line. " * 50000  # ~1MB
    large_file_path.write_text(content)
    
    return str(large_file_path)


@pytest.fixture
def empty_directory(tmp_path):
    """Create an empty directory for testing."""
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    return str(empty_dir)


@pytest.fixture
def nested_directory_structure(tmp_path):
    """Create a nested directory structure for testing."""
    # Create nested structure
    (tmp_path / "level1" / "level2" / "level3").mkdir(parents=True)
    
    # Add files at different levels
    (tmp_path / "level1" / "file1.txt").write_text("Level 1 file")
    (tmp_path / "level1" / "level2" / "file2.txt").write_text("Level 2 file")
    (tmp_path / "level1" / "level2" / "level3" / "file3.txt").write_text("Level 3 file")
    
    return str(tmp_path)


@pytest.fixture
def mock_slurm_environment(monkeypatch):
    """Mock Slurm environment variables."""
    mock_env = {
        'SLURM_JOB_ID': '12345',
        'SLURM_NTASKS': '4',
        'SLURM_NODELIST': 'node1,node2',
        'SLURM_PARTITION': 'test'
    }
    
    for key, value in mock_env.items():
        monkeypatch.setenv(key, value)
    
    return mock_env


@pytest.fixture
def cleanup_files():
    """Cleanup fixture to remove generated files after tests."""
    generated_files = []
    
    yield generated_files
    
    # Cleanup generated files
    for file_path in generated_files:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except (OSError, PermissionError):
            pass  # Ignore cleanup errors


# Pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )


def pytest_collection_modifyitems(config, items):
    """Automatically mark tests based on their names."""
    for item in items:
        # Mark tests based on naming convention
        if "test_main" in item.name:
            item.add_marker(pytest.mark.integration)
        elif "test_" in item.name:
            item.add_marker(pytest.mark.unit)
        
        # Mark slow tests based on certain patterns
        if any(pattern in item.name for pattern in ["large", "slow", "integration"]):
            item.add_marker(pytest.mark.slow)
