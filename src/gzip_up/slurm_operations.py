"""
Slurm operations for generating batch scripts and submitting jobs.
"""

import os
import subprocess
import time
import threading
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
    
    # Set sensible defaults for Slurm parameters
    defaults = {
        'job_name': 'gzip_compression',
        'ntasks': '1',
        'cpus_per_task': '4',
        'mem_per_cpu': '2G',
        'partition': 'short',
        'time': '02:00:00',
        'output': 'gzip_%A_%a.out',
        'error': 'gzip_%A_%a.err'
    }
    
    # Override defaults with user-provided values
    for key, value in slurm_args.items():
        if value is not None:
            # Map user keys to Slurm parameter names
            if key == 'mem':
                # Convert total memory to per-CPU memory
                if 'G' in value.upper():
                    mem_gb = float(value.upper().replace('G', ''))
                    defaults['mem_per_cpu'] = f"{max(1, mem_gb // int(defaults['cpus_per_task']))}G"
                elif 'M' in value.upper():
                    mem_mb = float(value.upper().replace('M', ''))
                    defaults['mem_per_cpu'] = f"{max(1024, mem_mb // int(defaults['cpus_per_task']))}M"
            elif key == 'output_log':
                defaults['output'] = value
            elif key == 'error_log':
                defaults['error'] = value
            else:
                # Direct mapping for other parameters
                defaults[key] = value
    
    # Count uncompressed files for array size
    uncompressed_files = [f for f in files if not f.endswith('.gz')]
    array_size = len(uncompressed_files)
    
    if array_size == 0:
        print_status("No uncompressed files found for Slurm script", "[WARN]")
        return ""
    
    # Get the task file name from the output parameter or use default
    task_file_name = slurm_args.get('output', 'gzip.cmds')
    
    # Check if this is a chunked file (contains semicolons)
    is_chunked = False
    try:
        with open(task_file_name, 'r') as f:
            for line in f:
                if line.strip() and ';' in line.strip():
                    is_chunked = True
                    break
    except Exception:
        pass
    
    if is_chunked:
        print_status("Chunked execution detected - task file contains multiple commands per line", "[INFO]")
        print_status("Each SLURM array task will process multiple gzip operations", "[INFO]")
    else:
        print_status("Standard execution - each SLURM array task processes one file", "[INFO]")
    
    with open(script_path, 'w') as f:
        f.write("#!/bin/bash\n")
        f.write(f"#SBATCH --job-name={defaults['job_name']}\n")
        f.write(f"#SBATCH --ntasks={defaults['ntasks']}\n")
        f.write(f"#SBATCH --cpus-per-task={defaults['cpus_per_task']}\n")
        f.write(f"#SBATCH --mem-per-cpu={defaults['mem_per_cpu']}\n")
        f.write(f"#SBATCH --array=1-{array_size}\n")
        f.write(f"#SBATCH --partition={defaults['partition']}\n")
        f.write(f"#SBATCH --time={defaults['time']}\n")
        f.write(f"#SBATCH --output={defaults['output']}\n")
        f.write(f"#SBATCH --error={defaults['error']}\n")
        
        f.write("\n# Load any required modules\n")
        f.write("# module load your_module\n\n")
        
        f.write("echo \"Starting gzip compression job\"\n")
        f.write("echo \"Job ID: $SLURM_JOB_ID\"\n")
        f.write("echo \"Array Job ID: $SLURM_ARRAY_JOB_ID\"\n")
        f.write("echo \"Array Task ID: $SLURM_ARRAY_TASK_ID\"\n")
        f.write("echo \"Number of CPUs: $SLURM_CPUS_PER_TASK\"\n")
        f.write("echo \"Memory per CPU: $SLURM_MEM_PER_CPU\"\n\n")
        
        f.write("# Specify the path to the task file\n")
        f.write(f"task_file=\"{task_file_name}\"\n\n")
        
        f.write("# Extract the individual command for this array task\n")
        f.write("gzip_cmd=$(awk -v SID=$SLURM_ARRAY_TASK_ID 'NR==SID {print; exit}' $task_file)\n\n")
        
        f.write("echo \"Executing: $gzip_cmd\"\n")
        f.write("echo \"Task $SLURM_ARRAY_TASK_ID of $SLURM_ARRAY_TASK_COUNT\"\n\n")
        
        f.write("# Execute the gzip command\n")
        f.write("eval $gzip_cmd\n\n")
        
        f.write("echo \"Gzip compression task $SLURM_ARRAY_TASK_ID completed\"\n")
    
    # Make script executable
    os.chmod(script_path, 0o755)
    
    print_status(f"Slurm script created with job array 1-{array_size}", "[OK]")
    print_status(f"Script made executable: {script_path}", "[OK]")
    
    return script_path


def run_on_slurm(script_path: str) -> bool:
    """
    Execute the Slurm script using srun and monitor progress until completion.
    
    Args:
        script_path: Path to the Slurm script
        
    Returns:
        True if successful, False otherwise
    """
    print_status("Executing job with srun...", "[*]")
    
    try:
        # Use srun to execute the job directly
        srun_cmd = ['srun', 'bash', script_path]
        
        # Start srun process
        srun_process = subprocess.Popen(srun_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        
        # Monitor progress with squeue every 10 seconds
        def monitor_progress():
            """Monitor job progress using squeue every 10 seconds."""
            while srun_process.poll() is None:  # While process is still running
                try:
                    # Check running jobs with squeue (filter by user and script name)
                    squeue_result = subprocess.run(['squeue', '--user', os.getenv('USER', ''), '--name', 'gzip-up_compression'], 
                                                 capture_output=True, text=True, check=True)
                    
                    if 'gzip-up_compression' in squeue_result.stdout or 'gzip-up_compression' in squeue_result.stderr:
                        # Job is still running
                        lines = squeue_result.stdout.strip().split('\n')
                        if len(lines) > 1:  # Skip header line
                            status_line = lines[1]
                            parts = status_line.split()
                            if len(parts) >= 5:
                                status = parts[4]  # Job status column
                                job_id = parts[0]  # Job ID column
                                print_status(f"Job {job_id} status: {status}", "[INFO]")
                    else:
                        # Job completed or not found
                        print_status("Job no longer in queue", "[INFO]")
                        break
                        
                except subprocess.CalledProcessError:
                    # squeue failed, job might be completed
                    break
                except Exception as e:
                    print_status(f"Error monitoring job: {e}", "[WARN]")
                
                time.sleep(10)  # Wait 10 seconds before next check
        
        # Start monitoring in a separate thread
        monitor_thread = threading.Thread(target=monitor_progress, daemon=True)
        monitor_thread.start()
        
        # Wait for srun to complete
        stdout, stderr = srun_process.communicate()
        
        if srun_process.returncode == 0:
            print_status("Job completed successfully!", "[OK]")
            if stdout.strip():
                print_status("Job output:", "[INFO]")
                print(stdout)
            return True
        else:
            print_status(f"Job failed with return code: {srun_process.returncode}", "[ERROR]")
            if stderr.strip():
                print_status("Job error output:", "[ERROR]")
                print(stderr)
            return False
            
    except FileNotFoundError:
        print_status("srun command not found. Make sure you're on a Slurm cluster.", "[ERROR]")
        return False
    except Exception as e:
        print_status(f"Unexpected error: {e}", "[ERROR]")
        return False
