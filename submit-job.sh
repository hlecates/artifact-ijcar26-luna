#!/bin/bash

color_blue="\e[94m"
color_green="\e[92m"
color_red="\e[91m"
color_default="\e[39m"

benchmark_sets_user=$(pwd)
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

runlim_binary="$HOME/.local/runlim"
runlim_options=""

# Cluster configuration
max_mem=64000

# Default options
default_time_limit=1200
default_memory_limit=8000
default_num_cpus=2
default_num_gpus=0
default_partition="cpu-q"


qos_max_tlimit=72000


# Options
time_limit=$default_time_limit
use_wall_time=
memory_limit=$default_memory_limit
num_cpus=$default_num_cpus
num_gpus=$default_num_gpus
partition="$default_partition"
working_dir=""
solver_options=""
benchmark_sets=""
multi_argument=
job_name=
cmpr=
sbatch_options=
tool_dir=""
python_bin="python3"

re_numeric='^[0-9]+$'
original_cmd="$0 $(printf "%q " "$@")"


#
# Helper functions
#

function usage ()
{
echo -e "
usage: $0 [options] EXECUTABLE

positional arguments:
 EXECUTABLE                            solver binary (LIRPA mode only)

optional arguments:
 -h, --help                            show this help message and exit
 -l                                    list available benchmark sets
 -p PART, --partition PART             use SLURM partition PART (default: $color_blue$default_partition$color_default)
                                       Available partitions:
                                         cpu-q, cpu-dense-hwulab-q, cpu-dense-preempt-q (CPU)
                                         gpu-a100-q (A100 GPUs, newer/faster)
                                         gpu-a5000-q (A5000 GPUs with ray-tracing)
 -t N, --time-limit N                  use time limit of N seconds CPU time
                                       (default: $color_blue$default_time_limit$color_default)
 -w, --wall-time                       use WALL time instead of CPU time
 -m N, --memory-limit N                use memory limit of N MB
                                       (default: $color_blue$default_memory_limit$color_default)
 -c N, --cpus N                        allocate N CPUs per job (default: $color_blue$default_num_cpus$color_default)
 -g N, --gpus N                        allocate N GPUs per job (default: $color_blue$default_num_gpus$color_default)
                                       Auto-set to 1 when using GPU partitions
 -d DIR, --working-dir DIR             use working directory DIR
 -o \"OPTS\",
 --solver-options \"OPTS\"               run EXECUTABLE with options OPTS
 -b \"SET1 SET2 ...\",
 --benchmark-sets \"SET1 SET2 ...\"      run EXECUTABLE on benchmark sets
 -j N, --max-jobs-per-node N           restrict number of jobs per node to N
 -n NAME, --job-name NAME              use job name NAME
 --multi                               multi-argument jobs
 --tool-dir DIR                        alpha-beta-CROWN repo dir (enables AB mode)
 --python-bin BIN                      python executable (default: python3)
 --notify <email>                      send email when job is done
"
}

function die ()
{
  echo -e "[${color_red}error${color_default}] $*" 1>&2
  exit 1
}

function warn ()
{
  echo -e "[${color_red}warn${color_default}] $*"
  exit 1
}

function info ()
{
  echo -e "[${color_green}info${color_default}] $*"
}

#
# Find available system and user benchmark sets
#
find_paths="$benchmark_sets_user"
sets=$(find $find_paths -maxdepth 1 -type f -name 'benchmark_set_*' | sort)

[[ -z "$sets" ]] && \
  die "no benchmark sets found in '$benchmark_sets_user'"


#
# Option parsing
#

while [ $# -gt 0 ]
do
  case $1 in
    -h|--help)
      usage
      exit 1
      ;;
    -p|--partition)
      shift
      partition="$1"
      ;;
    -t|--time-limit)
      shift
      time_limit="$1"
      ;;
    -w|--wall-time)
      use_wall_time="yes"
      ;;
    -m|--memory-limit)
      shift
      memory_limit="$1"
      ;;
    -c|--cpus)
      shift
      num_cpus="$1"
      ;;
    -g|--gpus)
      shift
      num_gpus="$1"
      ;;
    -d|--working-dir)
      shift
      working_dir="$1"
      ;;
    -o|--solver-options)
      shift
      solver_options="$1"
      ;;
    -e|--exclude)
      shift
      sbatch_options="$sbatch_options --exclude=$1"
      ;;
    -b|--benchmark-sets|--arguments)
      shift
      benchmark_sets="$1"
      ;;
    -n|--job-name)
      shift
      job_name="$1"
      ;;
    --multi)
      multi_argument="yes"
      ;;
    --tool-dir)
      shift
      tool_dir="$1"
      ;;
    --python-bin)
      shift
      python_bin="$1"
      ;;
    --notify)
      shift
      sbatch_options="$sbatch_options --mail-user=$1 --mail-type=END"
      ;;
    -*)
      die "invalid option '$1'"
      ;;
    *)
      [[ -n "$solver" ]] && die "executable already set to '$solver'"
      solver="$1"
      ;;
  esac
  shift
done

use_abcrown="no"
if [[ -n "$tool_dir" ]]; then
  use_abcrown="yes"
fi

if [[ "$use_abcrown" == "yes" ]]; then
  if [[ -n "$solver" ]]; then
    die "executable is not used when --tool-dir is set"
  fi
  if [[ ! -d "$tool_dir" ]]; then
    die "alpha-beta-CROWN directory '$tool_dir' does not exist"
  fi
  tool_dir="$(realpath "$tool_dir")"
  abcrown_script="$tool_dir/complete_verifier/abcrown.py"
  if [[ ! -f "$abcrown_script" ]]; then
    die "abcrown script '$abcrown_script' does not exist"
  fi
  if ! command -v "$python_bin" >/dev/null 2>&1; then
    die "python binary '$python_bin' not found in PATH"
  fi
  info "using python '$python_bin'"
else
[ -z "$solver" ] && die "no executable specified"
[ ! -e "$solver" ] && die "executable '$solver' does not exist"
[ -d "$solver" ] && die "executable '$solver' is a directory"
[ ! -x "$solver" ] && die "executable '$solver' is not executable"

solver=$(readlink -f "$solver")
solver_name=$(basename "$solver")
info "using solver '$solver'"
fi

[[ -n "$copy_dir" && ! -e "$copy_dir" ]] && \
  die "copy directory '$copy_dir' does not exist"

declare -A benchmark_list
declare -A benchmark_list_rev
declare -A benchmark_files
cnt=1
for f in $sets; do
  file_name=$(basename "$f")
  set_name=${file_name#benchmark_set_}
  benchmark_list[$cnt]=$set_name
  benchmark_list_rev[${set_name,,}]=$cnt
  benchmark_files[$cnt]=$f
  ((cnt+=1))
done

# Select benchmark sets
benchmark_indices=""
msg="no benchmarks selected"

# Allow bash range syntax {n..m} for selecting multiple benchmark sets
if [[ "$benchmark_sets" == *".."* ]]; then
  benchmark_sets="$(eval echo "$benchmark_sets")"
fi
IFS=" " read -r -a sets <<< "$benchmark_sets"
for bset in "${sets[@]}"; do
  if [[ "$bset" =~ $re_numeric ]]; then
    if (( bset >= 1 && bset < cnt)); then
      benchmark_indices="$benchmark_indices $bset"
      continue
    fi
  else
    regex="$bset"
    if [[ "$bset" == *"*"* ]]; then
      regex="${bset//\*/.*}" # Replace * with .* to have proper wildcard
      msg="no matching benchmark sets found with '$bset'"
    fi
    for s in "${benchmark_list[@]}"; do
      if [[ "$s" =~ ^$regex$ ]]; then
        s=${s,,}
        if [ -n "${benchmark_list_rev[$s]}" ]; then
          benchmark_indices="$benchmark_indices ${benchmark_list_rev[$s]}"
        fi
      fi
    done
    continue
  fi
done
[ -z "$benchmark_indices" ] && warn "$msg"

benchmark_sets=""
num_benchmark_sets=0
info "using benchmark set(s):"
for idx in $benchmark_indices; do
  file="${benchmark_files[$idx]}"
  info "  $file"
  benchmark_sets="$benchmark_sets $file"
  (( num_benchmark_sets++ ))
  [[ $(tail -c1 "$file" | wc -l) == 0 ]] && \
    die "Benchmark set file '$file' does not end with a newline"
done

#
# Check working directory
#
[ -z "$working_dir" ] && warn "no working directory specified"
[ -d "$working_dir" ] && warn "directory '$working_dir' already exists"
info "using directory '${working_dir}'"

#
# Configure solver options
#
[ -n "$solver_options" ] && info "using options '$solver_options'"

#
# Check partition
#
case "$partition" in
    "cpu-q")
    ;;
    "cpu-dense-hwulab-q")
    ;;
    "cpu-dense-preempt-q")
    ;;
    "gpu-a100-q")
    ;;
    "gpu-a5000-q")
    ;;
  *)
    warn "invalid partition '$partition' choose 'cpu-q', 'cpu-dense-hwulab-q', 'cpu-dense-preempt-q', 'gpu-a100-q', or 'gpu-a5000-q'"
    ;;
esac
info "using partition '$partition'"

# Set maximum memory based on partition
if [[ "$partition" == "cpu-q" ]]; then
  max_mem=192000  # 192 GB for cpu-q
elif [[ "$partition" == "cpu-dense-hwulab-q" ]]; then
  max_mem=700000  # ~700 GB for cpu-dense-hwulab-q (nodes have 751 GB)
elif [[ "$partition" == "cpu-dense-preempt-q" ]]; then
  max_mem=700000  # ~700 GB for cpu-dense-preempt-q
fi

#
# Configure GPU allocation
#
if [[ "$partition" == "gpu-a100-q" || "$partition" == "gpu-a5000-q" ]]; then
  if [[ $num_gpus -eq 0 ]]; then
    num_gpus=1
    info "auto-requesting 1 GPU for GPU partition"
  fi
elif [[ $num_gpus -gt 0 ]]; then
  warn "GPU requested but partition '$partition' is not a GPU partition"
fi

#
# Check time limit
#
if [[ "$time_limit" == "0" ]]; then
  warn "unlimited time limit not allowed"
elif [[ "$time_limit" -gt "$qos_max_tlimit" ]]; then
  warn "Partition has a maximum time limit of $qos_max_tlimit seconds"
fi
[[ ! $time_limit =~ $re_numeric ]] && \
  warn "time limit '$time_limit' is not a number"
info "using time limit '$time_limit'"

#
# Check memory limit
#

# Set default memory limit
[[ ! $memory_limit =~ $re_numeric ]] && \
  warn "space limit '$memory_limit' is not a number"
[[ $memory_limit -gt $max_mem ]] && \
  warn "memory limit '$memory_limit' exceeds maximum of ${max_mem}M for partition '$partition'"
info "using memory limit of ${memory_limit}M"


# Maximum number of CPUs depends on selected partition
if [[ "$partition" == "cpu-q" ]]; then
  num_virtual_cores=128
else
  num_virtual_cores=192
fi

#
# Check number of CPUs per job
#
max_num_cpus=$num_virtual_cores
[[ ! $num_cpus =~ $re_numeric ]] && warn "'$num_cpus' is not a number"
[[ $num_cpus -lt 1 || $num_cpus -gt $max_num_cpus ]] && \
  warn "number of CPUs must be between 1 and $max_num_cpus"

info "using $num_cpus CPUs"

#
# Check number of GPUs per job
#
[[ ! $num_gpus =~ $re_numeric ]] && warn "'$num_gpus' is not a number"
[[ $num_gpus -lt 0 ]] && warn "number of GPUs must be non-negative"

if [[ $num_gpus -gt 0 ]]; then
  info "using $num_gpus GPUs"
fi

#
# Configure runlim options
#
if [[ $time_limit != 0 ]]; then
  if [ -z "$use_wall_time" ]; then
    runlim_options="-t $time_limit"
  else
    runlim_options="-r $time_limit"
  fi
fi
runlim_options="$runlim_options -s ${memory_limit}"
if [[ $num_gpus -gt 0 ]]; then
  info "using time + timeout (GPU mode) instead of runlim"
else
  info "using runlim options: $runlim_options"
fi

memory_limit_slurm=$memory_limit

#
# Setup working directory
#
mkdir -p "$working_dir"
chmod 755 "$working_dir"
working_dir="$(realpath "$working_dir")"

# Create options file
{
  echo "$original_cmd"
  echo

  if [ -z "$use_wall_time" ]; then
    echo "cpu time limit:  $time_limit"
  else
    echo "wall time limit: $time_limit"
  fi
  echo "memory limit:    $memory_limit"
  echo "gpus:            $num_gpus"
  if [[ "$use_abcrown" == "yes" ]]; then
    echo "command:         $python_bin $abcrown_script $solver_options"
  else
  echo "command:         $solver_name $solver_options"
  fi
  if [[ $num_gpus -gt 0 ]]; then
    echo "measurement:     time + timeout"
  else
    echo "runlim:         $runlim_options"
  fi
  echo "partition:       $partition"
} > "$working_dir/options"

#
# Setup binaries/scripts
#

# Copy solver binary
if [[ "$use_abcrown" != "yes" ]]; then
solver_abs_path="$(realpath "$solver")"
  if [[ -z "$solver_abs_path" ]]; then
    die "failed to resolve solver path: '$solver'"
  fi
if [[ $copy_bin == "yes" ]]
then
    cp "$solver" "$working_dir"
      solver_abs_path="$working_dir/$(basename "$solver")"
  fi
  info "solver absolute path: '$solver_abs_path'"
fi

# Copy contents of directory
[ -n "$copy_dir" ] && cp -a "$copy_dir/." "$working_dir/"

#
# Create array job for each benchmark set
#
for benchmark_set in $benchmark_sets; do
  set_name="$(basename "$benchmark_set")"
  set_name="${set_name#benchmark_set_}"
  working_dir_set="$working_dir/$set_name"
  mkdir -p "$working_dir_set"

  # Save benchmarks file
  cp "$benchmark_set" "$working_dir_set/benchmarks"

  # Number of benchmark files = number of jobs in the array job
  ntasks=$(wc -l "$benchmark_set" | cut -d ' ' -f 1)

  # Single-argument script: benchmark set files contain an input file per line
  COMMAND=""

  # Create sbatch script
  SBATCH_SCRIPT="$working_dir_set/script.sh"

  # Configure GPU directive if needed.
  # --gpus=1 (or legacy --gres=gpu:1) requests 1 GPU per task.
  # This only works on GPU partitions (gpu-a100-q or gpu-a5000-q).
  GPU_DIRECTIVE=""
  if [[ $num_gpus -gt 0 ]]; then
    GPU_DIRECTIVE="#SBATCH --gpus=$num_gpus"
  fi

  # Limit concurrent array tasks when using GPUs (4 total jobs).
  # This caps total running tasks across the cluster, not per node.
  ARRAY_THROTTLE=""
  if [[ $num_gpus -gt 0 ]]; then
    ARRAY_THROTTLE="%4"
  fi

  cat > "$SBATCH_SCRIPT" << EOF
#!/bin/bash
#SBATCH -e /dev/null
#SBATCH -o /dev/null
#SBATCH -c $num_cpus
#SBATCH -a 1-$ntasks$ARRAY_THROTTLE
#SBATCH --partition=$partition
#SBATCH -t 00:00:$(expr 2 '*' "$time_limit")
#SBATCH --mem=${memory_limit_slurm}M
$GPU_DIRECTIVE
#SBATCH -D $working_dir

set -e -o pipefail

ARGS="\$(sed \${SLURM_ARRAY_TASK_ID}'q;d' $working_dir_set/benchmarks)"
read -r ONNX_FILE VNNLIB_FILE <<< "\$ARGS"
decompress_file() {
  local f="\$1"
  local gz_file="\$f"
  if [[ "\$f" != *.gz ]]; then
    gz_file="\${f}.gz"
  fi
  if [ -e "\$f" ] && [[ "\$f" != *.gz ]]; then
    echo "\$f"
    return
  fi
  if [ -e "\$gz_file" ]; then
    local out="\${gz_file%.gz}"
    if [ ! -e "\$out" ]; then
      gunzip -k "\$gz_file"
    fi
    echo "\$out"
    return
  fi
  echo "\$f"
}
ONNX_FILE="\$(decompress_file "\$ONNX_FILE")"
VNNLIB_FILE="\$(decompress_file "\$VNNLIB_FILE")"
ARGS="\$ONNX_FILE \$VNNLIB_FILE"
if [ -z "$multi_argument" ]; then
  onnx_base="\$(basename "\$ONNX_FILE")"
  vnnlib_base="\$(basename "\$VNNLIB_FILE")"
  onnx_name="\${onnx_base%.onnx}"
  vnnlib_name="\${vnnlib_base%.vnnlib}"
  WSUBDIR="\$onnx_name/\$vnnlib_name"
else
  WSUBDIR="slurm-\${SLURM_ARRAY_TASK_ID}"
fi
LOGDIR="$working_dir_set/\$WSUBDIR"
mkdir -p "\$LOGDIR"
out="\$LOGDIR/run.out"
OUTPUT="\$LOGDIR/output.log"

export ARGS
export OUTPUT
export LOGDIR

MODE="$use_abcrown"
if [[ "\$MODE" == "yes" ]]; then
  export PYTHONPATH="$tool_dir"
  export OMP_NUM_THREADS=1

  DEVICE="cpu"
  if [[ $num_gpus -gt 0 ]]; then
    DEVICE="cuda"
  fi

  CONFIG_FILE="\$LOGDIR/init_crown_config.yaml"
  # Determine root_path (directory containing benchmarks folder)
  # ONNX_FILE is like: /path/to/benchmarks/category/onnx/file.onnx
  # We want: /path/to (the directory containing benchmarks/)
  # Go up 5 levels: file.onnx -> medical/ -> onnx/ -> category/ -> benchmarks/ -> parent
  ROOT_PATH="\$(dirname "\$(dirname "\$(dirname "\$(dirname "\$(dirname "\$ONNX_FILE")")")")")"
  
  if [ -z "$multi_argument" ]; then
    cat > "\$CONFIG_FILE" << CONFIG
model:
  onnx_path: "\$ONNX_FILE"

specification:
  vnnlib_path: "\$VNNLIB_FILE"

solver:
  bound_prop_method: init-crown
  alpha-crown:
    lr_alpha: 0.5
    iteration: 20

general:
  device: \$DEVICE
  complete_verifier: skip
  root_path: "\$ROOT_PATH"

attack:
  pgd_order: skip
CONFIG
    COMMAND="$python_bin \"$abcrown_script\" --config \"\$CONFIG_FILE\" --no_prune_after_crown $solver_options"
  else
    cat > "\$CONFIG_FILE" << CONFIG
model:
  onnx_path: "\$ONNX_FILE"

specification:
  vnnlib_path: "\$VNNLIB_FILE"

solver:
  bound_prop_method: init-crown
  alpha-crown:
    lr_alpha: 0.5
    iteration: 20

general:
  device: \$DEVICE
  complete_verifier: skip
  root_path: "\$ROOT_PATH"

attack:
  pgd_order: skip
CONFIG
    COMMAND="$python_bin \"$abcrown_script\" --config \"\$CONFIG_FILE\" --no_prune_after_crown $solver_options"
  fi
else
  if [ -z "$multi_argument" ]; then
    COMMAND="\"$solver_abs_path\" --input \"\$ONNX_FILE\" --vnnlib \"\$VNNLIB_FILE\" $solver_options"
  else
    COMMAND="\"$solver_abs_path\" \$ARGS $solver_options"
  fi
fi

(
  echo "c host:       \$(hostname)"
  echo "c start:      \$(date)"
  echo "c arrayjobid: \${SLURM_ARRAY_JOB_ID}"
  echo "c jobid:      \${SLURM_JOB_ID}"
  echo "c command:    \$COMMAND"
  echo "c args:       \$ARGS"

  cd "\$LOGDIR"
  if [[ $num_gpus -gt 0 ]]; then
    if [[ $time_limit -gt 0 ]]; then
      eval "/usr/bin/time -v -o \"\${OUTPUT}\" timeout $time_limit \$COMMAND"
    else
      eval "/usr/bin/time -v -o \"\${OUTPUT}\" \$COMMAND"
    fi
  else
    eval "$runlim_binary $runlim_options -o \"\${OUTPUT}\" \$COMMAND"
  fi
  echo "c done"
) > "\$out" 2>&1

EOF

  name="$job_name"
  if [ -z "$name" ]; then
    name="$set_name"
  fi

  echo "option:" $sbatch_options
  # Create sub shell, change working directory and execute script
  (cd "$working_dir_set" && exec sbatch $sbatch_options --job-name="$name" ./script.sh)
done
