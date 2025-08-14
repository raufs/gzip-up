"""
Slurm operations for generating batch scripts and submitting jobs.
"""

import os
import subprocess
from typing import List, Dict

from .utils import print_status


def generate_slurm_script(files: List[str], slurm_args: Dict[str, str]) -> str:
    """
    Generate a Slurm batch script for gzip operations.
    
    Args:
        files: List of file paths to compress
        slurm_args: Dictionary of Slurm parameters
        
    Returns:
        Path to the generated Slurm script
    """
    script_path = "gzip_slurm.sh"
    
    print_status(f"Generating Slurm script: {script_path}")
    
    with open(script_path, 'w') as f:
        f.write("#!/bin/bash\n")
        f.write("#SBATCH --job-name=gzip_compression\n")
        
        # Add Slurm parameters
        if slurm_args.get('partition'):
            f.write(f"#SBATCH --partition={slurm_args['partition']}\n")
        if slurm_args.get('nodes'):
            f.write(f"#SBATCH --nodes={slurm_args['nodes']}\n")
        if slurm_args.get('ntasks'):
            f.write(f"#SBATCH --ntasks={slurm_args['ntasks']}\n")
        if slurm_args.get('cpus_per_task'):
            f.write(f"#SBATCH --cpus-per-task={slurm_args['cpus_per_task']}\n")
        if slurm_args.get('mem'):
            f.write(f"#SBATCH --mem={slurm_args['mem']}\n")
        if slurm_args.get('time'):
            f.write(f"#SBATCH --time={slurm_args['time']}\n")
        if slurm_args.get('output'):
            f.write(f"#SBATCH --output={slurm_args['output']}\n")
        if slurm_args.get('error'):
            f.write(f"#SBATCH --error={slurm_args['error']}\n")
        
        f.write("\n# Load any required modules\n")
        f.write("# module load your_module\n\n")
        
        f.write("echo \"Starting gzip compression job\"\n")
        f.write("echo \"Job ID: $SLURM_JOB_ID\"\n")
        f.write("echo \"Number of tasks: $SLURM_NTASKS\"\n\n")
        
        f.write("# Create task file\n")
        f.write("cat > gzip_tasks.txt << 'EOF'\n")
        for file_path in files:
            if not file_path.endswith('.gz'):
                f.write(f"gzip '{file_path}'\n")
        f.write("EOF\n\n")
        
        f.write("# Run gzip commands in parallel\n")
        f.write("srun --multi-prog gzip_tasks.txt\n\n")
        
        f.write("echo \"Gzip compression job completed\"\n")
    
    # Make script executable
    os.chmod(script_path, 0o755)
    
    print_status(f"Slurm script created and made executable", "[OK]")
    
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
