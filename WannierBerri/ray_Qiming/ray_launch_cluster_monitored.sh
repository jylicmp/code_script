#!/bin/bash
# Run Ray on LSF and then run a user workload.
# Author: Jiayu Li@HKU
# - v1.0 20240914
# - v2.0 20260619
# Reference: https://github.com/IBMSpectrumComputing/ray-integration
#
# Usage example:
#   sh ./ray_launch_cluster_monitored.sh \
#      -c "python -u main_wb.py 2-nodes" \
#      -n "wberri_v1.8" \
#      -m 20000000000
#
# -c: command run after Ray cluster is ready
# -n: conda environment name/path
# -m: Ray object store memory in bytes per Ray node

set -u

echo "LSB_MCPU_HOSTS=$LSB_MCPU_HOSTS"

echo "---- LSB_AFFINITY_HOSTFILE=$LSB_AFFINITY_HOSTFILE"
if [ -n "${LSB_AFFINITY_HOSTFILE:-}" ] && [ -f "$LSB_AFFINITY_HOSTFILE" ]; then
    cat "$LSB_AFFINITY_HOSTFILE"
else
    echo "LSB_AFFINITY_HOSTFILE is not set or does not exist"
fi
echo "---- End of LSB_AFFINITY_HOSTFILE"

echo "---- LSB_DJOB_HOSTFILE=$LSB_DJOB_HOSTFILE"
cat "$LSB_DJOB_HOSTFILE"
echo "---- End of LSB_DJOB_HOSTFILE"

# User-specific Ray temp directory.
# You may override it in the LSF script before calling this script:
#   export RAY_TMPDIR=/work/.../ray_tmp
export RAY_TMPDIR="${RAY_TMPDIR:-/tmp/ray-$USER}"
echo "RAY_TMPDIR=$RAY_TMPDIR"
mkdir -p "$RAY_TMPDIR"

# Reduce nested threading inside every Ray worker.
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
export NUMEXPR_NUM_THREADS="${NUMEXPR_NUM_THREADS:-1}"
export VECLIB_MAXIMUM_THREADS="${VECLIB_MAXIMUM_THREADS:-1}"

# Make repeated Ray logs visible. Useful for worker debug.
export RAY_DEDUP_LOGS="${RAY_DEDUP_LOGS:-0}"

function getfreeport()
{
    CHECK="do while"
    while [[ ! -z $CHECK ]]; do
        port=$(( ( RANDOM % 40000 )  + 20000 ))
        CHECK=$(netstat -a | grep $port || true)
    done
    echo $port
}

user_command=""
conda_env=""
object_store_mem=""

while getopts ":c:n:m:" option; do
    case "${option}" in
        c) user_command=${OPTARG} ;;
        n) conda_env=${OPTARG} ;;
        m) object_store_mem=${OPTARG} ;;
        *) echo "Did not supply the correct arguments" ;;
    esac
done

if [ -z "$user_command" ]; then
    echo "ERROR: no user command supplied with -c"
    exit 1
fi

# Activate conda env if provided.
if [ -z "$conda_env" ]; then
    echo "No conda env provided, assuming ray is already available."
else
    eval "$(conda shell.bash hook)"
    conda activate "$conda_env"
fi

# Get the hosts.
hosts=()
for host in $(cat "$LSB_DJOB_HOSTFILE" | uniq); do
    echo "Adding host: $host"
    hosts+=("$host")
done

echo "The host list is: ${hosts[@]}"

port=$(getfreeport)
echo "Head node will use port: $port"
export port

dashboard_port=$(getfreeport)
echo "Dashboard will use port: $dashboard_port"
export dashboard_port

redis_password=$(uuidgen)
export redis_password

# Compute number of cores allocated to hosts from LSB_MCPU_HOSTS.
echo "Num cpus per host is: $LSB_MCPU_HOSTS"
IFS=' ' read -r -a array <<< "$LSB_MCPU_HOSTS"
declare -A associative
i=0
len=${#array[@]}
while [ $i -lt $len ]; do
    key=${array[$i]}
    value=${array[$i+1]}
    associative[$key]=$(( ${associative[$key]:-0} + value ))
    i=$((i+2))
done

for host in "${!associative[@]}"; do
    echo "host=$host cores=${associative[$host]}"
done

head_node=${hosts[0]}
export head_node

ip=$(blaunch -z "$head_node" hostname --ip-address)

if [[ $ip == *" "* ]]; then
    IFS=' ' read -ra ADDR <<<"$ip"
    if [[ ${#ADDR[0]} > 16 ]]; then
        ip=${ADDR[1]}
    else
        ip=${ADDR[0]}
    fi
    echo "We detect space in ip! You may be seeing IPv6 + IPv4. We use IPv4 as: $ip"
fi

echo "Starting Ray head node on: $head_node"

ip_head=$ip:$port
export ip_head
export RAY_ADDRESS="$ip_head"
export RAY_REDIS_PASSWORD="$redis_password"

echo "IP Head: $ip_head"
echo "RAY_ADDRESS=$RAY_ADDRESS"

if [ -z "$object_store_mem" ]; then
    echo "Using default object store memory of 4GB per Ray node"
    object_store_mem=4000000000
else
    echo "Object store memory per Ray node in bytes: $object_store_mem"
fi
export object_store_mem

num_cpu_for_head=${associative[$head_node]:-1}
echo "Starting Ray head with num-cpus=$num_cpu_for_head"

command_launch="blaunch -z $head_node ray start \
    --head \
    --node-ip-address=$ip \
    --port=$port \
    --redis-password=$redis_password \
    --num-cpus=$num_cpu_for_head \
    --object-store-memory=$object_store_mem \
    --temp-dir=$RAY_TMPDIR \
    --block"

echo "$command_launch"
$command_launch &

sleep 30

workers=("${hosts[@]:1}")
echo "Adding the workers to head node: ${workers[*]}"

i=1
for host in "${workers[@]}"; do
    echo "STARTING WORKER $i at $host"
    num_cpu=${associative[$host]:-1}
    echo "Worker $i num-cpus=$num_cpu"

    command_for_worker="blaunch -z $host ray start \
        --address=$ip_head \
        --redis-password=$redis_password \
        --num-cpus=$num_cpu \
        --object-store-memory=$object_store_mem \
        --temp-dir=$RAY_TMPDIR \
        --block"

    echo "$command_for_worker"
    $command_for_worker &
    sleep 5
    i=$((i+1))
done

echo "Waiting for Ray cluster to stabilize..."
sleep 10
ray status --address "$ip_head" || true

echo "Running user workload: $user_command"
bash -lc "$user_command"
exit_code=$?

echo "User workload exit code: $exit_code"
echo "Ray status before shutdown:"
ray status --address "$ip_head" || true

echo "Stopping Ray cluster on all hosts..."
for host in "${hosts[@]}"; do
    echo "Stopping Ray on $host"
    blaunch -z "$host" ray stop --force || true
done

if [ $exit_code -ne 0 ]; then
    echo "Failure: $exit_code"
    exit $exit_code
else
    echo "Done"
    echo "Shutting down the LSF job"
    bkill "$LSB_JOBID"
fi
