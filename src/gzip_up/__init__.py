"""
Gzip-up Task Generator
"""

import os
import re
from pathlib import Path

def _get_version_from_pyproject():
    """Get version from pyproject.toml file."""
    try:
        # Get the directory containing this __init__.py file
        init_dir = Path(__file__).parent.parent.parent
        pyproject_path = init_dir / "pyproject.toml"
        
        if pyproject_path.exists():
            with open(pyproject_path, 'r') as f:
                content = f.read()
                # Extract version using regex
                match = re.search(r'^version\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE)
                if match:
                    return match.group(1)
    except Exception:
        pass
    
    # Fallback version if pyproject.toml can't be read
    return "1.0.1"

__version__ = _get_version_from_pyproject()
__author__ = "Rauf Salamzade"
__email__ = "salamzader@gmail.com"

from .main import main

__all__ = ["main"]
