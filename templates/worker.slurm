#! /usr/bin/env bash

#SBATCH --job-name=[[JOB_NAME]]
#SBATCH --time=[[TIME]]
#SBATCH --ntasks=[[NUM_TASKS]]
#SBATCH --partition=packable
#SBATCH --qos=at15_pk
#SBATCH --array=[[JOB_NUMBERS]]
#SBATCH -o [[JOB_NAME]].%A_%a

echo "-------------check memory for the node -----------"
grep -i -e memfree -e memtotal -e swaptotal /proc/meminfo
echo "---------------------------------------------------"

# change directories to where we're running
#cd [[JOB_DIR]]

# assume we are in the job directory

[ -f slurm_top_level_jobs.txt ] || touch slurm_top_level_jobs.txt

this_pid=$$
this_hostname=`hostname`
this_time=`date`
task_file=[[PREFIX]]_${SLURM_ARRAY_TASK_ID}

# keep track of what top-level jobs have been started
echo "$this_time $this_pid $this_hostname" >> slurm_top_level_jobs.txt

source activate IS2

echo "starting "$task_file

log_file=logs/${task_file}.log
echo "hostname="$this_hostname > $log_file
echo "start time="`date` >> $log_file

mv queue/$task_file running

bash running/$task_file >>$log_file 2>&1

if grep -q -i killed $log_file; then
    [ -d logs/rerun ] || mkdir logs/rerun
    mv $log_file logs/rerun
    echo "restarting killed task" > $log_file
    bash running/$task_file >>$log_file 2>&1
elif grep -q -i "bus error" $log_file; then
    [ -d logs/rerun ] || mkdir logs/rerun
    mv $log_file logs/rerun
    echo "restarting task after bus error" > $log_file
    bash running/$task_file >>$log_file 2>&1
fi

mv running/$task_file done/

echo "end time="`date` >> $log_file

echo "done with "$task_file" at "`date`

