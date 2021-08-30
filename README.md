# pboss
Utility for dispatching jobs across a set of hosts

## To install:
--Clone the repository, then cd to the repository directory on your home machine

-- install the script with either:
\>\> python setup.py develop
or
\>\> python setup.py install
If you want to run the script from multiple environments, this will need to be done for each environment separately

## To use:
--Make a file containing a list of commands to run, one per line.  It can be called anything, but queue.txt is a reasonable choice.

--Run pboss.py to setup a directory structure (it makes a par_run directory), and ingest your queue file.  Assuming the lines in the file are shell commands, this would be:

\>\> pboss.py -s queue.txt

--Start the job dispatcher.  Usually, it's a good idea to use the -w flag, which will keep the dispatcher active until all jobs are finished:

\>\> pboss.py -w

Not much will happen.  

--Now you need to go to a different shell, in the same directory, and start a worker:

\>\> pworker.py

This will start a job that asks the pboss.py process for a job, and runs that job in a shell.  When it's done, it will ask for another job.  This can be done in multiple shells, and/or on multiple machines that can see the same directory structure.  Start workers until you run out of computing resources (generally one per available processor thread is a good choice).

-- As an alternative to starting multiple terminal instances in which to run the workers, the run_pworkers script will run the workers in tmux shells.  You can run a ten of them with:

\>\> run_pworkers -s 10

...which will start ten shell jobs.

--To gracefully stop worker jobs, look in the par_run/comms directory.  You'll see a series of directories whose names reflect the hosts running your jobs, and pworker.py process numbers.  If you delete one of these, its process will quit when it finishes its current job.  

--To gracefully stop the whole queue, delete the par_run/boss_status_xxx_yyy (where xxx_yyy is the hostname and process number of the pboss.py process).  The boss will exit, and will delete the worker subdirectories under par_run/comms.

--Logging information should end up in the par_run/logs

--You can see what jobs are quened and running in the par_run/queue and par_run/running, and finished jobs are in par_run/done
