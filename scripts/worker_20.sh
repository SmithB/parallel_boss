#! /usr/bin/env bash

#SBATCH --job-name=worker_20.sh
#SBATCH --cpus-per-task=20            # Number of cores per MPI task 
#SBATCH --nodes=1                    # Maximum number of nodes to be allocated
#SBATCH --time=23:59:00



#while [ -d . ] ; do
#      if ls par_run/boss_status* 1> /dev/null 2>&1; then
#         break
#      fi
#      sleep 2
#done

[ -f slurm_master_jobs.txt ] || touch slurm_master_jobs.txt

this_pid=$$
this_hostname=`hostname`
this_time=`date`
echo "$this_time $this_pid $this_hostname" >> slurm_master_jobs.txt


#start a boss if there is not one running
if  ls par_run/boss_status* 1> /dev/null 2>&1; then 
    echo "Boss status file exists, not starting a new boss"  
else 
    echo "No boss status file found, starting pboss.py"
    pboss.py -w &
fi


echo "starting jobs"

run_pworkers -s 20 bg

start=$SECONDS
max_time=$(( 23*3600 ))

while ls par_run/boss_status* 1> /dev/null 2>&1 ; do
    sleep 20
    time_elapsed=$(( $SECONDS - start ))
    if (( $time_elapsed > $max_time ))  ; then
        if ls par_run/comms/worker_*$hostname* 1> /dev/null 2>&1 ; then
            echo "after $time_elapsed seconds, deleting the comms for this worker"
            rm -r par_run/comms/worker_*$hostname*
        fi
    fi
done

echo "batch is over, exiting"
