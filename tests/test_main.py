"""
Unit tests for main module CLI functionality.
"""

import pytest
import sys
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
from io import StringIO

from slurm_gzip.main import main, validate_suffixes, create_colored_parser


class TestSuffixValidation:
    """Test the validate_suffixes function."""
    
    def test_validate_suffixes_basic(self):
        """Test basic suffix validation and normalization."""
        suffixes = ['txt', 'log', '.csv']
        result = validate_suffixes(suffixes)
        
        assert result == {'.txt', '.log', '.csv'}
    
    def test_validate_suffixes_reject_gz(self):
        """Test that .gz suffixes are rejected."""
        with pytest.raises(ValueError, match="Cannot compress already compressed .gz files"):
            validate_suffixes(['.gz'])
    
    def test_validate_suffixes_reject_gz_no_dot(self):
        """Test that gz suffixes without dot are rejected."""
        with pytest.raises(ValueError, match="Cannot compress already compressed .gz files"):
            validate_suffixes(['gz'])
    
    def test_validate_suffixes_reject_other_compressed(self):
        """Test that other compression formats are rejected."""
        compressed_formats = ['.bz2', '.xz', '.zip', '.tar', '.7z', '.rar']
        
        for fmt in compressed_formats:
            with pytest.raises(ValueError, match="File appears to already be compressed"):
                validate_suffixes([fmt])
    
    def test_validate_suffixes_mixed_valid_invalid(self):
        """Test validation with mix of valid and invalid suffixes."""
        with pytest.raises(ValueError, match="Cannot compress already compressed .gz files"):
            validate_suffixes(['.txt', '.gz', '.log'])
    
    def test_validate_suffixes_empty_list(self):
        """Test validation with empty list."""
        result = validate_suffixes([])
        assert result == set()
    
    def test_validate_suffixes_duplicates(self):
        """Test that duplicate suffixes are handled correctly."""
        suffixes = ['.txt', 'txt', '.txt']
        result = validate_suffixes(suffixes)
        
        assert result == {'.txt'}


class TestColoredParser:
    """Test the create_colored_parser function."""
    
    def test_create_colored_parser_basic(self):
        """Test that parser is created with correct structure."""
        parser = create_colored_parser()
        
        # Check that required arguments exist
        assert parser.get_default('directory') == '.'
        assert parser.get_default('output') == 'gzip.cmds'
        
        # Check that help text contains emojis
        help_text = parser.format_help()
        assert '[*]' in help_text
        assert '[*]' in help_text
        assert '[*]' in help_text
    
    def test_create_colored_parser_argument_groups(self):
        """Test that argument groups are properly created."""
        parser = create_colored_parser()
        
        # Check that argument groups exist
        group_names = [group.title for group in parser._action_groups]
        assert '[*] File Discovery Options' in group_names
        assert '[*] Slurm Integration Options' in group_names
        assert '[*]  Slurm Parameters' in group_names


class TestMainCLI:
    """Test the main CLI functionality."""
    
    @patch('slurm_gzip.main.find_files_with_suffixes')
    @patch('slurm_gzip.main.generate_task_file')
    @patch('slurm_gzip.main.display_file_summary')
    @patch('slurm_gzip.main.print_header')
    @patch('slurm_gzip.main.print_section')
    @patch('slurm_gzip.main.print_status')
    @patch('slurm_gzip.main.print_logo')
    @patch('slurm_gzip.main.print_colored_banner')
    def test_main_basic_usage(self, mock_banner, mock_logo, mock_status, mock_section, 
                             mock_header, mock_summary, mock_gen_task, mock_find_files):
        """Test basic CLI usage with file suffix."""
        # Mock command line arguments
        with patch('sys.argv', ['slurm_gzip', '-s', '.txt']):
            # Mock return values
            mock_find_files.return_value = ['/path/to/file.txt']
            mock_gen_task.return_value = '/path/to/gzip.cmds'
            
            # Mock current directory
            with patch('os.getcwd', return_value='/current/dir'):
                with patch('os.path.isdir', return_value=True):
                    main()
        
        # Verify function calls
        mock_logo.assert_called_once()
        mock_banner.assert_called_once()
        mock_find_files.assert_called_once_with('.', {'.txt'})
        mock_gen_task.assert_called_once_with(['/path/to/file.txt'], 'gzip.cmds')
        mock_summary.assert_called_once_with(['/path/to/file.txt'])
    
    @patch('slurm_gzip.main.find_files_with_suffixes')
    @patch('slurm_gzip.main.generate_task_file')
    @patch('slurm_gzip.main.display_file_summary')
    @patch('slurm_gzip.main.print_logo')
    @patch('slurm_gzip.main.print_colored_banner')
    def test_main_custom_directory_and_suffixes(self, mock_banner, mock_logo, 
                                              mock_summary, mock_gen_task, mock_find_files):
        """Test CLI with custom directory and multiple suffixes."""
        mock_find_files.return_value = ['/test/dir/file1.txt', '/test/dir/file2.log']
        mock_gen_task.return_value = '/test/dir/gzip.cmds'
        
        with patch('sys.argv', ['slurm_gzip', '-d', '/test/dir', '-s', '.txt', '.log']):
            with patch('os.path.isdir', return_value=True):
                main()
        
        mock_find_files.assert_called_once_with('/test/dir', {'.txt', '.log'})
        mock_gen_task.assert_called_once_with(
            ['/test/dir/file1.txt', '/test/dir/file2.log'], 
            'gzip.cmds'
        )
    
    @patch('slurm_gzip.main.find_files_with_suffixes')
    @patch('slurm_gzip.main.generate_task_file')
    @patch('slurm_gzip.main.display_file_summary')
    @patch('slurm_gzip.main.print_logo')
    @patch('slurm_gzip.main.print_colored_banner')
    def test_main_custom_output_file(self, mock_banner, mock_logo, 
                                   mock_summary, mock_gen_task, mock_find_files):
        """Test CLI with custom output file."""
        mock_find_files.return_value = ['/path/to/file.txt']
        mock_gen_task.return_value = '/path/to/custom.cmds'
        
        with patch('sys.argv', ['slurm_gzip', '-s', '.txt', '-o', 'custom.cmds']):
            with patch('os.path.isdir', return_value=True):
                main()
        
        mock_gen_task.assert_called_once_with(['/path/to/file.txt'], 'custom.cmds')
    
    @patch('slurm_gzip.main.find_files_with_suffixes')
    @patch('slurm_gzip.main.generate_task_file')
    @patch('slurm_gzip.main.generate_slurm_script')
    @patch('slurm_gzip.main.display_file_summary')
    @patch('slurm_gzip.main.print_logo')
    @patch('slurm_gzip.main.print_colored_banner')
    def test_main_with_slurm(self, mock_banner, mock_logo, mock_summary, 
                            mock_gen_slurm, mock_gen_task, mock_find_files):
        """Test CLI with Slurm script generation."""
        mock_find_files.return_value = ['/path/to/file.txt']
        mock_gen_task.return_value = '/path/to/gzip.cmds'
        mock_gen_slurm.return_value = '/path/to/gzip_slurm.sh'
        
        with patch('sys.argv', ['slurm_gzip', '-s', '.txt', '--slurm']):
            with patch('os.path.isdir', return_value=True):
                main()
        
        mock_gen_slurm.assert_called_once()
        # Verify Slurm args are empty dict when no parameters specified
        call_args = mock_gen_slurm.call_args[0][1]
        assert call_args == {}
    
    @patch('slurm_gzip.main.find_files_with_suffixes')
    @patch('slurm_gzip.main.generate_task_file')
    @patch('slurm_gzip.main.generate_slurm_script')
    @patch('slurm_gzip.main.display_file_summary')
    @patch('slurm_gzip.main.print_logo')
    @patch('slurm_gzip.main.print_colored_banner')
    def test_main_with_slurm_parameters(self, mock_banner, mock_logo, mock_summary, 
                                       mock_gen_slurm, mock_gen_task, mock_find_files):
        """Test CLI with Slurm parameters."""
        mock_find_files.return_value = ['/path/to/file.txt']
        mock_gen_task.return_value = '/path/to/gzip.cmds'
        mock_gen_slurm.return_value = '/path/to/gzip_slurm.sh'
        
        with patch('sys.argv', ['slurm_gzip', '-s', '.txt', '--slurm', '--partition', 'short', '--ntasks', '4']):
            with patch('os.path.isdir', return_value=True):
                main()
        
        # Verify Slurm args contain the specified parameters
        call_args = mock_gen_slurm.call_args[0][1]
        assert call_args['partition'] == 'short'
        assert call_args['ntasks'] == '4'
    
    @patch('slurm_gzip.main.find_files_with_suffixes')
    @patch('slurm_gzip.main.generate_task_file')
    @patch('slurm_gzip.main.generate_slurm_script')
    @patch('slurm_gzip.main.run_on_slurm')
    @patch('slurm_gzip.main.display_file_summary')
    @patch('slurm_gzip.main.print_logo')
    @patch('slurm_gzip.main.print_colored_banner')
    @patch('builtins.input', return_value='yes')
    def test_main_auto_run_slurm(self, mock_input, mock_banner, mock_logo, 
                                mock_summary, mock_run_slurm, mock_gen_slurm, 
                                mock_gen_task, mock_find_files):
        """Test CLI with auto-run Slurm submission."""
        mock_find_files.return_value = ['/path/to/file.txt']
        mock_gen_task.return_value = '/path/to/gzip.cmds'
        mock_gen_slurm.return_value = '/path/to/gzip_slurm.sh'
        mock_run_slurm.return_value = True
        
        with patch('sys.argv', ['slurm_gzip', '-s', '.txt', '--slurm', '--auto-run']):
            with patch('os.path.isdir', return_value=True):
                main()
        
        mock_run_slurm.assert_called_once_with('/path/to/gzip_slurm.sh')
    
    @patch('slurm_gzip.main.find_files_with_suffixes')
    @patch('slurm_gzip.main.generate_task_file')
    @patch('slurm_gzip.main.generate_slurm_script')
    @patch('slurm_gzip.main.run_on_slurm')
    @patch('slurm_gzip.main.display_file_summary')
    @patch('slurm_gzip.main.print_logo')
    @patch('slurm_gzip.main.print_colored_banner')
    @patch('builtins.input', return_value='no')
    def test_main_auto_run_cancelled(self, mock_input, mock_banner, mock_logo, 
                                    mock_summary, mock_run_slurm, mock_gen_slurm, 
                                    mock_gen_task, mock_find_files):
        """Test CLI with auto-run Slurm submission cancelled."""
        mock_find_files.return_value = ['/path/to/file.txt']
        mock_gen_task.return_value = '/path/to/gzip.cmds'
        mock_gen_slurm.return_value = '/path/to/gzip_slurm.sh'
        
        with patch('sys.argv', ['slurm_gzip', '-s', '.txt', '--slurm', '--auto-run']):
            with patch('os.path.isdir', return_value=True):
                main()
        
        # Should not call run_on_slurm when cancelled
        mock_run_slurm.assert_not_called()
    
    @patch('slurm_gzip.main.find_files_with_suffixes')
    @patch('slurm_gzip.main.print_logo')
    @patch('slurm_gzip.main.print_colored_banner')
    def test_main_no_files_found(self, mock_banner, mock_logo, mock_find_files):
        """Test CLI when no files are found."""
        mock_find_files.return_value = []
        
        with patch('sys.argv', ['slurm_gzip', '-s', '.txt']):
            with patch('os.path.isdir', return_value=True):
                with patch('sys.exit') as mock_exit:
                    main()
                    mock_exit.assert_called_once_with(0)
    
    @patch('slurm_gzip.main.print_logo')
    @patch('slurm_gzip.main.print_colored_banner')
    def test_main_invalid_directory(self, mock_banner, mock_logo):
        """Test CLI with invalid directory."""
        with patch('sys.argv', ['slurm_gzip', '-s', '.txt']):
            with patch('os.path.isdir', return_value=False):
                with patch('sys.exit') as mock_exit:
                    main()
                    mock_exit.assert_called_once_with(1)
    
    @patch('slurm_gzip.main.print_logo')
    @patch('slurm_gzip.main.print_colored_banner')
    def test_main_auto_run_without_slurm(self, mock_banner, mock_logo):
        """Test CLI with auto-run but without slurm flag."""
        with patch('sys.argv', ['slurm_gzip', '--auto-run']):
            with patch('sys.exit') as mock_exit:
                main()
                mock_exit.assert_called_once_with(2)  # ArgumentParser error exit code
    
    @patch('slurm_gzip.main.find_files_with_suffixes')
    @patch('slurm_gzip.main.generate_task_file')
    @patch('slurm_gzip.main.display_file_summary')
    @patch('slurm_gzip.main.print_logo')
    @patch('slurm_gzip.main.print_colored_banner')
    def test_main_suffix_normalization(self, mock_banner, mock_logo, 
                                     mock_summary, mock_gen_task, mock_find_files):
        """Test that suffixes are properly normalized."""
        mock_find_files.return_value = ['/path/to/file.txt']
        mock_gen_task.return_value = '/path/to/gzip.cmds'
        
        with patch('sys.argv', ['slurm_gzip', '-s', '.txt']):
            with patch('os.path.isdir', return_value=True):
                main()
        
        # Verify suffixes are normalized to start with dot
        call_args = mock_find_files.call_args[0][1]
        assert call_args == {'.txt'}
    
    @patch('slurm_gzip.main.find_files_with_suffixes')
    @patch('slurm_gzip.main.generate_task_file')
    @patch('slurm_gzip.main.display_file_summary')
    @patch('slurm_gzip.main.print_logo')
    @patch('slurm_gzip.main.print_colored_banner')
    def test_main_suffix_without_dot(self, mock_banner, mock_logo, 
                                   mock_summary, mock_gen_task, mock_find_files):
        """Test CLI with suffixes that don't start with dot."""
        mock_find_files.return_value = ['/path/to/file.txt', '/path/to/file.log']
        mock_gen_task.return_value = '/path/to/gzip.cmds'
        
        with patch('sys.argv', ['slurm_gzip', '-s', 'txt', 'log']):
            with patch('os.path.isdir', return_value=True):
                main()
        
        # Verify suffixes are normalized to start with dot
        call_args = mock_find_files.call_args[0][1]
        assert call_args == {'.txt', '.log'}
    
    @patch('slurm_gzip.main.print_logo')
    @patch('slurm_gzip.main.print_colored_banner')
    def test_main_reject_gz_suffix(self, mock_banner, mock_logo):
        """Test that .gz suffixes are rejected."""
        with patch('sys.argv', ['slurm_gzip', '-s', '.gz']):
            with patch('os.path.isdir', return_value=True):
                with patch('sys.exit') as mock_exit:
                    main()
                    mock_exit.assert_called_once_with(1)
    
    @patch('slurm_gzip.main.print_logo')
    @patch('slurm_gzip.main.print_colored_banner')
    def test_main_reject_compressed_formats(self, mock_banner, mock_logo):
        """Test that other compression formats are rejected."""
        compressed_formats = ['.bz2', '.xz', '.zip', '.tar', '.7z', '.rar']
        
        for fmt in compressed_formats:
            with patch('sys.argv', ['slurm_gzip', '-s', fmt]):
                with patch('os.path.isdir', return_value=True):
                    with patch('sys.exit') as mock_exit:
                        main()
                        mock_exit.assert_called_once_with(1)


class TestMainIntegration:
    """Integration tests for main module."""
    
    def test_main_complete_workflow(self, tmp_path):
        """Test complete workflow from CLI to file generation."""
        # Create test files
        test_file1 = tmp_path / "file1.txt"
        test_file1.write_text("content1")
        test_file2 = tmp_path / "file2.log"
        test_file2.write_text("content2")
        
        # Mock command line arguments
        with patch('sys.argv', ['slurm_gzip', '-d', str(tmp_path), '-s', '.txt', '.log']):
            with patch('slurm_gzip.main.display_file_summary') as mock_summary:
                with patch('slurm_gzip.main.generate_task_file') as mock_gen_task:
                    with patch('slurm_gzip.main.print_header'):
                        with patch('slurm_gzip.main.print_section'):
                            with patch('slurm_gzip.main.print_status'):
                                with patch('slurm_gzip.main.print_logo'):
                                    with patch('slurm_gzip.main.print_colored_banner'):
                                        main()
        
        # Verify that the workflow executed
        mock_summary.assert_called_once()
        mock_gen_task.assert_called_once()
        
        # Check that the summary was called with the correct files
        summary_call_args = mock_summary.call_args[0][0]
        assert len(summary_call_args) == 2
        assert any('file1.txt' in f for f in summary_call_args)
        assert any('file2.log' in f for f in summary_call_args)
