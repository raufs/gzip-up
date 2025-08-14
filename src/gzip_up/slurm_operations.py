"""
Slurm operations for generating batch scripts and submitting jobs.
"""

import os
import subprocess
from typing import List, Dict

from .utils import print_status


def generate_slurm_script(files: List[str], slurm_args: Dict[str, str], task_file: str = "gzip.cmds") -> str:
    """
    Generate a Slurm batch script for gzip operations using job arrays.
    
    Args:
        files: List of file paths to compress (used only for info)
        slurm_args: Dictionary of Slurm parameters
        task_file: Path to the task file containing one gzip command per line
        
    Returns:
        Path to the generated Slurm script
    """
    script_path = "gzip_slurm.sh"
    
    print_status(f"Generating Slurm script: {script_path}")
    
    # Determine array size by counting non-empty, non-comment lines in task_file
    try:
        array_size = 0
        with open(task_file, 'r') as tf:
            for line in tf:
                stripped = line.strip()
                if stripped and not stripped.startswith('#'):
                    array_size += 1
    except Exception as e:
        print_status(f"Failed to read task file '{task_file}': {e}", "[ERROR]")
        array_size = 0
    
    # Ensure at least 1 to satisfy SLURM; runtime guard will no-op if no command
    if array_size == 0:
        print_status("No commands found in task file; array size will be set to 1 and tasks will no-op", "[WARN]")
        array_size = 1
    
    # Set default SLURM parameters if not provided
    defaults = {
        'partition': 'short',
        'ntasks': '1',
        'cpus_per_task': '1',
        'mem_per_cpu': '1G',
        'time': '02:00:00',
        'output': 'gzip-up_%j.out',
        'error': 'gzip-up_%j.err'
    }
    
    # Merge user-provided args with defaults
    for key, default_value in defaults.items():
        if key not in slurm_args or slurm_args[key] is None:
            slurm_args[key] = default_value
    
    with open(script_path, 'w') as f:
        f.write("#!/bin/bash\n")
        f.write("#SBATCH --job-name=gzip_compression\n")
        f.write(f"#SBATCH --partition={slurm_args['partition']}\n")
        f.write(f"#SBATCH --ntasks={slurm_args['ntasks']}\n")
        f.write(f"#SBATCH --cpus-per-task={slurm_args['cpus_per_task']}\n")
        f.write(f"#SBATCH --mem-per-cpu={slurm_args['mem_per_cpu']}\n")
        f.write(f"#SBATCH --time={slurm_args['time']}\n")
        f.write(f"#SBATCH --output={slurm_args['output']}\n")
        f.write(f"#SBATCH --error={slurm_args['error']}\n")
        f.write(f"#SBATCH --array=1-{array_size}\n")
        
        # Add additional Slurm parameters if specified
        if slurm_args.get('nodes'):
            f.write(f"#SBATCH --nodes={slurm_args['nodes']}\n")
        if slurm_args.get('mem'):
            f.write(f"#SBATCH --mem={slurm_args['mem']}\n")
        
        f.write("\n# Gzip compression job using job array\n")
        f.write("# Each array task processes one file from the task file\n")
        f.write("# This script expects the task file to exist at submission time\n\n")
        
        f.write("echo \"Starting gzip compression job\"\n")
        f.write("echo \"Job ID: $SLURM_JOB_ID\"\n")
        f.write("echo \"Array Task ID: $SLURM_ARRAY_TASK_ID\"\n")
        f.write("echo \"Total array size: $SLURM_ARRAY_TASK_COUNT\"\n\n")
        
        f.write("# Specify the path to the task file\n")
        f.write(f"task_file=\"{task_file}\"\n\n")
        
        f.write("# Extract the individual command for this array task\n")
        f.write("gcmd=$(awk -v SID=$SLURM_ARRAY_TASK_ID 'NR==SID {print; exit}' \"$task_file\")\n\n")
        
        f.write("# If no command is found (empty line or out-of-range), exit gracefully\n")
        f.write("if [ -z \"$gcmd\" ]; then\n")
        f.write("  echo \"No command found for task $SLURM_ARRAY_TASK_ID; exiting\"\n")
        f.write("  exit 0\n")
        f.write("fi\n\n")
        
        f.write("echo \"Executing command: $gcmd\"\n")
        f.write("echo \"Processing file: $SLURM_ARRAY_TASK_ID of $SLURM_ARRAY_TASK_COUNT\"\n\n")
        
        f.write("# Execute the gzip command\n")
        f.write("eval $gcmd\n\n")
        
        f.write("echo \"Task $SLURM_ARRAY_TASK_ID completed successfully\"\n")
    
    # Make script executable
    os.chmod(script_path, 0o755)
    
    print_status(f"Slurm script created and made executable", "[OK]")
    print_status(f"Job array size (from task file): {array_size} tasks", "[INFO]")
    print_status(f"Using defaults: partition={slurm_args['partition']}, time={slurm_args['time']}, mem-per-cpu={slurm_args['mem_per_cpu']}", "[INFO]")
    
    return script_path


def run_on_slurm(script_path: str) -> bool:
    """
    Submit the Slurm script using sbatch.
    
    Args:
        script_path: Path to the Slurm script
        
    Returns:
        True if successful, False otherwise
    """
    print_status("Submitting job to Slurm...", "[*]")
    
    try:
        result = subprocess.run(['sbatch', script_path], 
                              capture_output=True, text=True, check=True)
        print_status(f"Job submitted successfully: {result.stdout.strip()}", "[OK]")
        return True
    except subprocess.CalledProcessError as e:
        print_status(f"Error submitting job: {e}", "[ERROR]")
        print(f"stderr: {e.stderr}")
        return False
    except FileNotFoundError:
        print_status("sbatch command not found. Make sure you're on a Slurm cluster.", "[ERROR]")
        return False
