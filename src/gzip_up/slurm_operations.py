"""
Slurm operations for generating batch scripts and submitting jobs.
"""

import os
import subprocess
from typing import List, Dict

from .utils import print_status


def generate_slurm_script(files: List[str], slurm_args: Dict[str, str]) -> str:
    """
    Generate a Slurm batch script for gzip operations using job arrays.
    
    Args:
        files: List of file paths to compress
        slurm_args: Dictionary of Slurm parameters
        
    Returns:
        Path to the generated Slurm script
    """
    script_path = "gzip_slurm.sh"
    
    print_status(f"Generating Slurm script: {script_path}")
    
    # Count uncompressed files for array size
    uncompressed_files = [f for f in files if not f.endswith('.gz')]
    array_size = len(uncompressed_files)
    
    # Set default SLURM parameters if not provided
    defaults = {
        'partition': 'short',
        'ntasks': '1',
        'cpus_per_task': '1',
        'mem_per_cpu': '1G',
        'time': '02:00:00',
        'output': 'gzip_%j.out',
        'error': 'gzip_%j.err'
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
        f.write("# This script expects gzip.cmds to exist in the same directory\n\n")
        
        f.write("echo \"Starting gzip compression job\"\n")
        f.write("echo \"Job ID: $SLURM_JOB_ID\"\n")
        f.write("echo \"Array Task ID: $SLURM_ARRAY_TASK_ID\"\n")
        f.write("echo \"Total array size: $SLURM_ARRAY_TASK_COUNT\"\n\n")
        
        f.write("# Specify the path to the task file\n")
        f.write("task_file=\"gzip.cmds\"\n\n")
        
        f.write("# Extract the individual command for this array task\n")
        f.write("gcmd=$(awk -v SID=$SLURM_ARRAY_TASK_ID 'NR==SID {print; exit}' $task_file)\n\n")
        
        f.write("echo \"Executing command: $gcmd\"\n")
        f.write("echo \"Processing file: $SLURM_ARRAY_TASK_ID of $SLURM_ARRAY_TASK_COUNT\"\n\n")
        
        f.write("# Execute the gzip command\n")
        f.write("eval $gcmd\n\n")
        
        f.write("echo \"Task $SLURM_ARRAY_TASK_ID completed successfully\"\n")
    
    # Make script executable
    os.chmod(script_path, 0o755)
    
    print_status(f"Slurm script created and made executable", "[OK]")
    print_status(f"Job array size: {array_size} tasks", "[INFO]")
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
