# gzip-up 

A Python program that scans directories for files with specific suffixes and generates task files for gzip compression operations. 

<img width="270" height="264" alt="image" src="https://github.com/user-attachments/assets/e9807d90-65f6-4056-9044-245343a064f8" />


It can also generate and optionally auto-submit Slurm batch scripts or auto-run locally using threading.

> [!WARNING]
> This project is still in development. AI was used for its development and not all pieces might be working as intended; however, we have tried to prioritize safety throughout. 

> [!IMPORTANT]
> I am a bioinformatician, so if there is interest, we can add other commands besides gzip - like samtools to compress sam files to bam files.

> [!NOTE]
> If interested in compressing a really large file using threading/processing, check out [`pigz`](https://github.com/madler/pigz).

## Features

- **File Discovery**: Recursively scan directories for files with specified suffixes
- **Task File Generation**: Create command files ready for parallel execution
- **Slurm Integration**: Generate Slurm batch scripts with customizable parameters
- **Auto-submission**: Option to automatically submit jobs to Slurm (with confirmation)
- **Smart Chunking**: Automatically chunk large file sets to respect SLURM job array limits (only if `--slurm --auto-run` issued!)
- **Flexible Configuration**: Customize output files, Slurm parameters, and execution options

## Requirements

- Python 3.8+
- Access to a Slurm cluster (for Slurm functionality)
- Standard Unix tools (gzip, parallel, etc.)

## Installation

1. Clone or download the repository
2. Make the script executable:
   ```bash
   chmod +x gzip_up.py
   ```

## Usage

### Basic Usage

Scan current directory for `.txt` and `.log` files:
```bash
python gzip_up.py -s .txt .log
```

Scan specific directory:
```bash
python gzip_up.py -d /path/to/files -s .txt .log
```

Custom output file:
```bash
python gzip_up.py -s .txt .log -o my_tasks.cmds
```

### Slurm Integration

Generate Slurm batch script:
```bash
python -m gzip_up -s .txt .log --slurm
```

**Important**: The `--slurm` flag alone will create SLURM scripts for any number of files, potentially creating very large job arrays (e.g., 20,000 jobs for 20,000 files). Some SLURM clusters may have job array size limits.

**Manual Chunking**: You can control job array size using the `--max-jobs` option:
```bash
python -m gzip_up -s .txt .log --slurm --max-jobs 500
```
This will create chunked task files with a maximum of 500 jobs, regardless of file count.

**Auto-run Options**: When using `--auto-run`, you can control chunking behavior:
```bash
# Auto-run with automatic chunking (default for >1000 files)
python -m gzip_up -s .txt .log --slurm --auto-run

# Auto-run without chunking (creates large job arrays)
python -m gzip_up -s .txt .log --slurm --auto-run --no-chunk

# Auto-run with custom chunking limit
python -m gzip_up -s .txt .log --slurm --auto-run --max-jobs 500
```

Customize Slurm parameters:
```bash
python -m gzip_up -s .txt .log --slurm \
  --partition=short \
  --ntasks=4 \
  --mem=8G \
  --time=01:00:00
```

Auto-submit to Slurm with smart chunking (recommended for large file sets):
```bash
python -m gzip_up -s .txt .log --slurm --auto-run
```

**Smart Chunking with `--auto-run`**: When you specify `--auto-run` with more than 1000 files, the system automatically:
- Creates chunked task files where each SLURM job processes multiple gzip commands
- Ensures the job array never exceeds 1000 tasks (respecting SLURM limits)
- Optimizes timing based on chunk size (30 min base + 5 min per additional command)
- Creates temporary chunked files that are automatically cleaned up after execution

### Command Line Options

```
-d, --directory DIR     Directory to scan (default: current directory)
-s, --suffixes SUFFIX   File suffixes to look for (required)
-o, --output FILE       Output task file name (default: gzip.cmds)
--slurm                 Generate Slurm batch script
--auto-run             Automatically submit to Slurm (requires --slurm)
--max-jobs N            Maximum number of jobs in task file (enables chunking when exceeded)
--no-chunk            Disable automatic chunking for --auto-run

Slurm Parameters:
--partition PART        Slurm partition
--nodes N              Number of nodes
--ntasks N             Number of tasks
--cpus-per-task N      CPUs per task
--mem MEM              Memory per node
--time TIME            Time limit (HH:MM:SS)
--output-log FILE      Output log file
--error-log FILE       Error log file

### Local Execution Options
--threads N            Number of threads for local execution (0 for auto-detect)
--local-run            Execute gzip operations locally using threading
```

## Output Files

### Task File (default: `gzip.cmds`)
Contains one gzip command per line, ready for parallel execution:
```bash
# Example gzip.cmds
gzip '/path/to/file1.txt'
gzip '/path/to/file2.log'
gzip '/path/to/file3.txt'
```

### Slurm Script (default: `gzip_slurm.sh`)
Bash script with Slurm directives and execution logic.

## Execution Methods

### Local Execution
```bash
# Using built-in threading (recommended)
python -m gzip_up -s .txt .log --local-run --threads 4

# Using GNU parallel
parallel < gzip.cmds

# Using xargs
xargs -P $(nproc) -a gzip.cmds

# Individual execution
bash gzip.cmds
```

### Slurm Execution
```bash
# Submit batch job
sbatch gzip_slurm.sh

# Interactive execution
srun --multi-prog gzip.cmds
```

## Examples

### Example 1: Basic File Compression
```bash
# Find and compress all .txt files in current directory
python -m gzip_up -s .txt

# Review generated task file
cat gzip.cmds

# Execute locally with threading
python -m gzip_up -s .txt --local-run --threads 4

# Or execute with parallel
parallel < gzip.cmds
```

### Example 2: Slurm Batch Job (Small File Set)
```bash
# Generate Slurm script for small dataset (≤1000 files)
python -m gzip_up -d /data/small_files -s .csv .tsv --slurm \
  --partition=short \
  --ntasks=16 \
  --mem=32G \
  --time=01:00:00

# Submit job
sbatch gzip_slurm.sh
```

### Example 2b: Manual Chunking Control
```bash
# Generate Slurm script with manual chunking (max 500 jobs)
python -m gzip_up -d /data/large_files -s .csv --slurm --max-jobs 500

# This will:
# - Create chunked task files with max 500 jobs
# - Each job processes multiple gzip commands
# - Respect your specified job limit
# - No automatic cleanup (files remain for manual review)
```

### Example 3: Auto-submission with Smart Chunking (Large File Set)
```bash
# Generate and auto-submit with automatic chunking (>1000 files)
python -m gzip_up -d /data/large_files -s .log --slurm --auto-run

# This will automatically:
# - Create chunked task files if >1000 files
# - Limit job array to ≤1000 tasks
# - Optimize timing per chunk
# - Clean up temporary files after completion
```

### Example 3b: Auto-submission without Chunking
```bash
# Generate and auto-submit without chunking (creates large job arrays)
python -m gzip_up -d /data/large_files -s .log --slurm --auto-run --no-chunk

# This will:
# - Submit large job arrays (e.g., 20,000 jobs for 20,000 files)
# - No chunking or temporary files
# - May exceed SLURM job array limits on some clusters
# - Use standard SLURM execution
```

## Safety Features

- **Confirmation Required**: Auto-submission always prompts for user confirmation
- **File Review**: Generated files are displayed for review before execution
- **Skip Compressed**: Already compressed (.gz) files are automatically skipped
- **Error Handling**: Comprehensive error checking and informative messages

## Tips

1. **Always review** generated task files before execution
2. **Test locally** with a small subset before running on Slurm
3. **Use appropriate** Slurm parameters for your cluster
4. **Monitor jobs** using `squeue` and `sacct` commands
5. **Check logs** for any compression errors

### Threading vs Multiprocessing

For gzip operations, **threading is recommended** over multiprocessing because:
- **I/O Bound**: Gzip compression is primarily I/O bound (file reading/writing)
- **Memory Efficient**: Threads share memory space, processes duplicate memory
- **GIL Friendly**: Python's Global Interpreter Lock doesn't significantly impact I/O operations
- **Optimal Thread Count**: Use `--threads 0` for auto-detection, or manually set to 4-8 threads

## Troubleshooting

### Common Issues

- **No files found**: Check directory path and suffix specifications
- **Permission denied**: Ensure script is executable and you have read access to target directory
- **Slurm not found**: Verify you're on a Slurm cluster and `sbatch` is available
- **Compression errors**: Check file permissions and disk space

### Debug Mode
Add `-v` or `--verbose` for more detailed output (if implemented).

## License

This project is licensed under the GNU General Public License v3 - see the LICENSE file for details.

The GNU GPL is a free, copyleft license that ensures the software remains free and open source. This means:
- You can use, modify, and distribute the software
- Any derivative works must also be released under the GPL
- The source code must be made available
- The software cannot be incorporated into proprietary programs

For more information about the GNU GPL, visit: https://www.gnu.org/licenses/

## Disclaimer

**This software is provided "AS IS" without warranty of any kind.** Use at your own risk. The authors are not liable for any damages resulting from the use of this software.

**Important Notes:**
- This project is still in development and has been AI-generated without comprehensive vetting
- **DO NOT USE** in production environments without thorough testing
- Always backup your data before running compression operations
- Test with small file sets before processing large datasets
- The software may contain bugs or errors that could result in data loss

## Contributing

Contributions are welcome! Please feel free to submit issues, feature requests, or pull requests.
