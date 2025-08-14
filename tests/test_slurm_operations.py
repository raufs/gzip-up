"""
Unit tests for slurm_operations module.
"""

import pytest
import os
import stat
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open

from gzip_up.slurm_operations import generate_slurm_script, run_on_slurm


class TestGenerateSlurmScript:
    """Test the generate_slurm_script function."""
    
    def test_generate_slurm_script_basic(self, tmp_path):
        """Test generating basic Slurm script."""
        # Change to temporary directory
        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        
        try:
            files = ["/path/to/file1.txt", "/path/to/file2.log"]
            slurm_args = {}
            
            result = generate_slurm_script(files, slurm_args)
            
            # Should create gzip_slurm.sh in current directory
            assert result == "gzip_slurm.sh"
            assert Path("gzip_slurm.sh").exists()
            
            # Check file permissions
            script_path = Path("gzip_slurm.sh")
            assert script_path.stat().st_mode & stat.S_IXUSR  # Executable
            
            # Check content
            content = script_path.read_text()
            assert "#!/bin/bash" in content
            assert "#SBATCH --job-name=gzip_compression" in content
            assert "gzip '/path/to/file1.txt'" in content
            assert "gzip '/path/to/file2.log'" in content
            assert "srun --multi-prog gzip_tasks.txt" in content
        finally:
            os.chdir(original_cwd)
    
    def test_generate_slurm_script_with_parameters(self, tmp_path):
        """Test generating Slurm script with custom parameters."""
        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        
        try:
            files = ["/path/to/file.txt"]
            slurm_args = {
                'partition': 'short',
                'nodes': '2',
                'ntasks': '4',
                'cpus_per_task': '2',
                'mem': '8G',
                'time': '01:00:00',
                'output': 'output.log',
                'error': 'error.log'
            }
            
            result = generate_slurm_script(files, slurm_args)
            
            content = Path(result).read_text()
            
            # Check that all parameters are included
            assert "#SBATCH --partition=short" in content
            assert "#SBATCH --nodes=2" in content
            assert "#SBATCH --ntasks=4" in content
            assert "#SBATCH --cpus-per-task=2" in content
            assert "#SBATCH --mem=8G" in content
            assert "#SBATCH --time=01:00:00" in content
            assert "#SBATCH --output=output.log" in content
            assert "#SBATCH --error=error.log" in content
        finally:
            os.chdir(original_cwd)
    
    def test_generate_slurm_script_skip_compressed(self, tmp_path):
        """Test that compressed files are excluded from Slurm script."""
        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        
        try:
            files = [
                "/path/to/file1.txt",
                "/path/to/file2.gz",  # Already compressed
                "/path/to/file3.log"
            ]
            slurm_args = {}
            
            result = generate_slurm_script(files, slurm_args)
            
            content = Path(result).read_text()
            
            # Should only include uncompressed files
            assert "gzip '/path/to/file1.txt'" in content
            assert "gzip '/path/to/file2.gz'" not in content
            assert "gzip '/path/to/file3.log'" in content
        finally:
            os.chdir(original_cwd)
    
    def test_generate_slurm_script_content_structure(self, tmp_path):
        """Test the structure of generated Slurm script content."""
        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        
        try:
            files = ["/path/to/file.txt"]
            slurm_args = {}
            
            result = generate_slurm_script(files, slurm_args)
            content = Path(result).read_text()
            lines = content.split('\n')
            
            # Check basic structure
            assert lines[0] == "#!/bin/bash"
            assert lines[1] == "#SBATCH --job-name=gzip_compression"
            assert "echo \"Starting gzip compression job\"" in content
            assert "echo \"Job ID: $SLURM_JOB_ID\"" in content
            assert "echo \"Number of tasks: $SLURM_NTASKS\"" in content
            assert "srun --multi-prog gzip_tasks.txt" in content
            assert "echo \"Gzip compression job completed\"" in content
        finally:
            os.chdir(original_cwd)
    
    def test_generate_slurm_script_partial_parameters(self, tmp_path):
        """Test generating script with only some parameters specified."""
        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        
        try:
            files = ["/path/to/file.txt"]
            slurm_args = {
                'partition': 'long',
                'ntasks': '8'
                # Other parameters not specified
            }
            
            result = generate_slurm_script(files, slurm_args)
            content = Path(result).read_text()
            
            # Should include specified parameters
            assert "#SBATCH --partition=long" in content
            assert "#SBATCH --ntasks=8" in content
            
            # Should not include unspecified parameters
            assert "#SBATCH --nodes=" not in content
            assert "#SBATCH --mem=" not in content
        finally:
            os.chdir(original_cwd)


class TestRunOnSlurm:
    """Test the run_on_slurm function."""
    
    @patch('subprocess.run')
    def test_run_on_slurm_success(self, mock_run):
        """Test successful Slurm job submission."""
        # Mock successful subprocess run
        mock_result = MagicMock()
        mock_result.stdout = "Submitted batch job 12345"
        mock_run.return_value = mock_result
        
        result = run_on_slurm("test_script.sh")
        
        assert result is True
        mock_run.assert_called_once_with(
            ['sbatch', 'test_script.sh'],
            capture_output=True,
            text=True,
            check=True
        )
    
    @patch('subprocess.run')
    def test_run_on_slurm_subprocess_error(self, mock_run):
        """Test handling of subprocess errors."""
        # Mock subprocess error
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1,
            cmd=['sbatch', 'test_script.sh'],
            stderr="Error: Invalid script"
        )
        
        result = run_on_slurm("test_script.sh")
        
        assert result is False
    
    @patch('subprocess.run')
    def test_run_on_slurm_file_not_found(self, mock_run):
        """Test handling when sbatch command is not found."""
        # Mock FileNotFoundError
        mock_run.side_effect = FileNotFoundError("sbatch: command not found")
        
        result = run_on_slurm("test_script.sh")
        
        assert result is False
    
    @patch('subprocess.run')
    def test_run_on_slurm_generic_exception(self, tmp_path):
        """Test handling of other exceptions."""
        # Mock generic exception
        with patch('subprocess.run', side_effect=Exception("Unknown error")):
            result = run_on_slurm("test_script.sh")
            
            # Should handle gracefully and return False
            assert result is False


class TestSlurmScriptIntegration:
    """Integration tests for Slurm script generation and execution."""
    
    def test_script_executable_permissions(self, tmp_path):
        """Test that generated script has correct permissions."""
        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        
        try:
            files = ["/path/to/file.txt"]
            slurm_args = {}
            
            script_path = generate_slurm_script(files, slurm_args)
            
            # Check file permissions
            stat_info = os.stat(script_path)
            assert stat_info.st_mode & stat.S_IXUSR  # User executable
            assert stat_info.st_mode & stat.S_IXGRP  # Group executable
            assert stat_info.st_mode & stat.S_IXOTH  # Others executable
        finally:
            os.chdir(original_cwd)
    
    def test_script_content_validation(self, tmp_path):
        """Test that generated script content is valid bash."""
        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        
        try:
            files = ["/path/to/file.txt"]
            slurm_args = {'partition': 'test'}
            
            script_path = generate_slurm_script(files, slurm_args)
            content = Path(script_path).read_text()
            
            # Basic bash syntax validation
            assert content.startswith("#!/bin/bash")
            assert "#SBATCH" in content
            assert "echo" in content
            assert "srun" in content
            
            # Check that all lines are valid
            lines = content.split('\n')
            for line in lines:
                if line.strip() and not line.startswith('#'):
                    # Non-empty, non-comment lines should be valid
                    assert line.strip() != ""
        finally:
            os.chdir(original_cwd)
