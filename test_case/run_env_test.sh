#!/usr/bin/env bash
#
# run_env_test.sh
#
# Demonstrates that pworker.py runs jobs in the conda environment it is
# invoked from.  Activate a named conda environment before running, e.g.:
#
#   conda activate devel
#   ./run_env_test.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

# Use scripts directly from the repo so the test works regardless of which
# conda environment is active (the scripts may only be installed in base).
PBOSS="python $REPO_ROOT/scripts/pboss.py"
PWORKER="python $REPO_ROOT/scripts/pworker.py"
export PYTHONPATH="$REPO_ROOT${PYTHONPATH:+:$PYTHONPATH}"

CURRENT_ENV="${CONDA_DEFAULT_ENV:-base}"
N_JOBS=4
N_WORKERS=2

echo "========================================"
echo "  conda environment inheritance test"
echo "========================================"
echo "invoking environment : $CURRENT_ENV"
if [ "$CURRENT_ENV" = "base" ]; then
    echo "NOTE: activate a named env first to fully exercise inheritance."
fi
echo ""

# ---- setup --------------------------------------------------------
[ -d par_run ] && rm -rf par_run
mkdir -p par_run/{queue,running,done,comms,logs,active_logs}

for i in $(seq 1 $N_JOBS); do
    cat > "par_run/queue/task_${i}" <<'TASK'
#!/usr/bin/env bash
echo "CONDA_DEFAULT_ENV : ${CONDA_DEFAULT_ENV:-<not set>}"
echo "CONDA_PREFIX      : ${CONDA_PREFIX:-<not set>}"
echo "python binary     : $(which python 2>/dev/null || echo '<not found>')"
python -c "import sys; print('python sys.prefix :', sys.prefix)" 2>/dev/null \
    || echo "python not available"
TASK
    chmod +x "par_run/queue/task_${i}"
done
echo "queued $N_JOBS env-check tasks"

# ---- run ----------------------------------------------------------
# -r = run the boss loop; -w = wait for all running jobs before exiting; -Q = quiet
$PBOSS -r -w -Q > par_run/boss.log 2>&1 &
BOSS_PID=$!
echo "started pboss (PID $BOSS_PID)"

# Give boss a moment to write its status file before workers try to connect
sleep 0.5

# pworker auto-detects CONDA_DEFAULT_ENV from this shell's environment
for i in $(seq 1 $N_WORKERS); do
    $PWORKER > "par_run/pworker_${i}.log" 2>&1 &
done
echo "started $N_WORKERS pworkers (env auto-detected as '$CURRENT_ENV')"
echo ""

wait $BOSS_PID
echo "all jobs complete"
echo ""

# ---- report -------------------------------------------------------
echo "========================================"
echo "  worker startup messages"
echo "========================================"
for log in par_run/pworker_*.log; do
    msg=$(grep "running jobs in conda" "$log" 2>/dev/null \
          || echo "(no env message — running without conda env override)")
    echo "  $(basename "$log"): $msg"
done
echo ""

echo "========================================"
echo "  per-job environment  (par_run/logs/)"
echo "========================================"
for log in $(ls -v par_run/logs/task_*.log 2>/dev/null); do
    echo "--- $(basename "$log") ---"
    grep -v '^#pworker' "$log"
    echo ""
done

# ---- pass / fail --------------------------------------------------
echo "========================================"
echo "  summary"
echo "========================================"
if [ "$CURRENT_ENV" != "base" ]; then
    n_correct=0
    for log in par_run/logs/task_*.log; do
        grep -q "$CURRENT_ENV" "$log" 2>/dev/null && n_correct=$((n_correct+1)) || true
    done
    echo "expected env name in output : $CURRENT_ENV"
    echo "jobs showing correct env    : $n_correct / $N_JOBS"
    if [ "$n_correct" -eq "$N_JOBS" ]; then
        echo "PASS"
    else
        echo "FAIL"
        exit 1
    fi
else
    echo "invoked from base — review python sys.prefix values above"
fi
