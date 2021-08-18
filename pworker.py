#! /usr/bin/env python3

import sys, os, glob, time, re, subprocess, datetime, stat

this_PID=os.getpid()
this_hostname=os.uname()[1]
invoke_dir=os.getcwd()

if not os.path.isdir("par_run/comms"):
    print("par_run/comms directory not found, waiting")
    while not os.path.isdir("par_run/comms"):
        time.sleep(5)


worker_dir="par_run/comms/worker_%s.%s/"%(this_hostname, this_PID)
comms_to_worker_dir=worker_dir+"/to_worker"  
comms_to_boss_dir=worker_dir+"/to_boss"  
from_boss=comms_to_worker_dir+'/request.txt'
to_boss=comms_to_boss_dir+'/request.txt'
scratch=worker_dir+"scratch_from_worker.txt"

if not os.path.isdir(worker_dir):
    os.mkdir(worker_dir)
if not os.path.isdir(comms_to_worker_dir):
    os.mkdir(comms_to_worker_dir)
if not os.path.isdir(comms_to_boss_dir):
    os.mkdir(comms_to_boss_dir)
     
# check if the comms file exists
#flags = os.O_CREAT | os.O_EXCL
#os.close(os.open(comms_file, flags))

comms_count=0;
# if we get this far, can create the comms file
#fid=open(comms_file,'r+')

response_re=re.compile('response\[(\d+)\]\s+(.*);')

if not os.path.isdir('par_run/logs'):
    os.mkdir('par_run/logs');

while os.path.isdir(comms_to_worker_dir) or comms_count==0:
     
    comms_count=comms_count+1;
    with open(scratch,'w') as fid:
        fid.write("request new job %d;\n" % comms_count)
    os.rename(scratch, to_boss)
    # wait for updates on the file
    while not os.path.isfile(from_boss):
        if (not os.path.isdir(comms_to_worker_dir)) and (comms_count>0):
            break
        time.sleep(1)
         
    # read a line from the file
    with open(from_boss,'r') as fid:
        the_line=fid.readline().rstrip()
    os.remove(from_boss)
    print("parallel_boss sent line [%s]" % the_line)
    response_match=response_re.search(the_line)
    if response_match is None:
        continue
    proc_count=response_match.group(1)
    task_file=response_match.group(2)
    if not os.path.isfile(task_file):
        print("Task file %s Does not exist, continuing\n" % task_file)
        continue
    task_num=re.search('task_(\d+)', task_file).group(1)
    running_file="par_run/running/task_%s_%s_%s.%s"%(task_num, this_hostname, this_PID, task_num)
    os.rename(task_file, running_file)
    # make the running file executable
    stats=os.stat(running_file)
    os.chmod(running_file, stats.st_mode | stat.S_IEXEC)
    log_file='par_run/logs/'+os.path.basename(running_file)+'.log';
    log_fid=open(log_file,'wb')
    print("----  running task %s in directory %s ----" % (running_file, invoke_dir))
    print("----       log file is %s" % log_file)
    print("----       time is %s" % str(datetime.datetime.now()))
    my_env=os.environ.copy()
    my_env['MKL_NUM_THREADS']='1'
    p=subprocess.Popen(running_file, shell=True, stdout=subprocess.PIPE,  cwd=invoke_dir, stderr=subprocess.STDOUT, env=my_env)
    while True:
        #byte=p.stdout.read(1)
        byte = p.stdout.readline()
        if byte:
            sys.stdout.buffer.write(byte)
            sys.stdout.flush()
            log_fid.write(byte)
            log_fid.flush()
        else:        
            break
    p.wait()
    log_fid.close()
    done_file="par_run/done/task_%s_%s_%s.%s"%(task_num, this_hostname, this_PID, task_num)
    os.rename(running_file, done_file);
    print("------- Finished: %s" % str(datetime.datetime.now()))
    


    
 

