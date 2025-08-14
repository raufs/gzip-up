"""
Unit tests for utils module.
"""

import pytest
from unittest.mock import patch
from io import StringIO
import sys

from gzip_up.utils import (
    print_header,
    print_section,
    print_status,
    print_progress,
    display_file_summary,
)


class TestPrintFunctions:
    """Test the print utility functions."""
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_print_header(self, mock_stdout):
        """Test print_header function."""
        print_header("Test Title")
        output = mock_stdout.getvalue()
        
        assert "[*] Test Title" in output
        assert "=" * 60 in output
        assert output.count("=") == 120  # 60 on each line
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_print_section(self, mock_stdout):
        """Test print_section function."""
        print_section("Test Section")
        output = mock_stdout.getvalue()
        
        assert "[+] Test Section" in output
        assert "-" * 40 in output
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_print_status_default(self, mock_stdout):
        """Test print_status function with default emoji."""
        print_status("Test message")
        output = mock_stdout.getvalue()
        
        assert "[i] Test message" in output
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_print_status_custom(self, mock_stdout):
        """Test print_status function with custom emoji."""
        print_status("Test message", "[OK]")
        output = mock_stdout.getvalue()
        
        assert "[OK] Test message" in output
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_print_progress(self, mock_stdout):
        """Test print_progress function."""
        print_progress(5, 10, "Test Progress")
        output = mock_stdout.getvalue()
        
        assert "Test Progress:" in output
        assert "5/10" in output
        assert "50.0%" in output
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_print_progress_complete(self, mock_stdout):
        """Test print_progress function when complete."""
        print_progress(10, 10, "Test Progress")
        output = mock_stdout.getvalue()
        
        assert "Test Progress:" in output
        assert "10/10" in output
        assert "100.0%" in output


class TestDisplayFileSummary:
    """Test the display_file_summary function."""
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_display_file_summary_empty(self, mock_stdout):
        """Test display_file_summary with empty file list."""
        display_file_summary([])
        output = mock_stdout.getvalue()
        
        assert "[*] Total files: 0" in output
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_display_file_summary_single_file(self, mock_stdout, tmp_path):
        """Test display_file_summary with single file."""
        # Create a temporary file
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")
        
        display_file_summary([str(test_file)])
        output = mock_stdout.getvalue()
        
        assert "[*] Total files: 1" in output
        assert ".txt: 1 files" in output
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_display_file_summary_multiple_files(self, mock_path, tmp_path):
        """Test display_file_summary with multiple files of different types."""
        # Mock Path.stat() to return a mock stat object
        mock_stat = type('MockStat', (), {'st_size': 1024})()
        
        with patch('pathlib.Path.stat', return_value=mock_stat):
            files = [
                str(tmp_path / "file1.txt"),
                str(tmp_path / "file2.log"),
                str(tmp_path / "file3.txt")
            ]
            
            display_file_summary(files)
            output = mock_stdout.getvalue()
            
            assert "[*] Total files: 3" in output
            assert ".txt: 2 files" in output
            assert ".log: 1 files" in output
            assert "[*] Total size: 3.00 KB" in output
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_display_file_summary_large_files(self, mock_path, tmp_path):
        """Test display_file_summary with large files (GB)."""
        # Mock Path.stat() to return a large size (2 GB)
        mock_stat = type('MockStat', (), {'st_size': 2 * 1024 * 1024 * 1024})()
        
        with patch('pathlib.Path.stat', return_value=mock_stat):
            files = [str(tmp_path / "large_file.txt")]
            
            display_file_summary(files)
            output = mock_stdout.getvalue()
            
            assert "[*] Total size: 2.00 GB" in output
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_display_file_summary_stat_error(self, mock_path, tmp_path):
        """Test display_file_summary handles stat errors gracefully."""
        # Mock Path.stat() to raise an OSError
        with patch('pathlib.Path.stat', side_effect=OSError("Permission denied")):
            files = [str(tmp_path / "file.txt")]
            
            # Should not raise an exception
            display_file_summary(files)
            output = mock_stdout.getvalue()
            
            assert "[*] Total files: 1" in output
            # Size should be 0 due to error
            assert "[*] Total size: 0.00 MB" in output
