# gzip-up 

A Python program that scans directories for files with specific suffixes and generates task files for gzip compression operations. 

<img width="270" height="264" alt="image" src="https://github.com/user-attachments/assets/e9807d90-65f6-4056-9044-245343a064f8" />


It can also generate and optionally auto-submit Slurm batch scripts or auto-run locally using threading.

> [!Warning]
> THIS PROJECT IS STILL IN DEVELPMENT AND JUST AI GENERATED WITHOUT VETTING AT THIS POINT. DON'T USE.

> [!IMPORTANT]
> I AM A BIOINFORMATICIAN - IF POPULAR INTEREST - WE CAN ADD SAMTOOLS AS DEPENDENCY FOR SAM FILE TO BAM FILE COMPRESSION.

> [!NOTE]
> If interested in compressing a really large file using threading/processing, check out [`pigz`](https://github.com/madler/pigz).

## Features

- **File Discovery**: Recursively scan directories for files with specified suffixes
- **Task File Generation**: Create command files ready for parallel execution
- **Slurm Integration**: Generate Slurm batch scripts with customizable parameters
- **Auto-submission**: Option to automatically submit jobs to Slurm (with confirmation)
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
python gzip_up.py -s .txt .log --slurm
```

Customize Slurm parameters:
```bash
python gzip_up.py -s .txt .log --slurm \
  --partition=short \
  --ntasks=4 \
  --mem=8G \
  --time=01:00:00
```

Auto-submit to Slurm (with confirmation):
```bash
python gzip_up.py -s .txt .log --slurm --auto-run
```

### Command Line Options

```
-d, --directory DIR     Directory to scan (default: current directory)
-s, --suffixes SUFFIX   File suffixes to look for (required)
-o, --output FILE       Output task file name (default: gzip.cmds)
--slurm                 Generate Slurm batch script
--auto-run             Automatically submit to Slurm (requires --slurm)

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

### Example 2: Slurm Batch Job
```bash
# Generate Slurm script for large dataset
python -m gzip_up -d /data/large_files -s .csv .tsv --slurm \
  --partition=long \
  --ntasks=16 \
  --mem=32G \
  --time=04:00:00

# Submit job
sbatch gzip_slurm.sh
```

### Example 3: Auto-submission with Confirmation
```bash
# Generate and auto-submit (will prompt for confirmation)
python -m gzip_up -d /data/files -s .log --slurm --auto-run
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

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit issues, feature requests, or pull requests.
