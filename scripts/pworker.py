#! /usr/bin/env python3

import sys, os, time, re, subprocess, datetime, stat

class pworker(object):
    def __init__(self, log=False):

        self.verbose=True
        self.retired=False

        self.PID=os.getpid()
        self.hostname=os.uname()[1]
        self.invoke_dir=os.getcwd()

        if not os.path.isdir("par_run/comms"):
            print("par_run/comms directory not found, waiting")
            while not os.path.isdir("par_run/comms"):
                time.sleep(1)

        self.worker_dir="par_run/comms/worker_%s.%s/"%(self.hostname, self.PID)
        self.comms_to_worker_dir=self.worker_dir+"/to_worker"
        self.comms_to_boss_dir=self.worker_dir+"/to_boss"
        self.from_boss_file=self.comms_to_worker_dir+'/request.txt'
        self.to_boss_file=self.comms_to_boss_dir+'/request.txt'
        self.scratch=self.worker_dir+"scratch_from_worker.txt"

        self.log_handle=None
        if log:
            log_dir="par_run/worker_logs"
            if not os.path.isdir(log_dir):
                os.mkdir(log_dir)
            self.log_handle=open(os.path.join(log_dir, "worker_%s.%s/"%(self.hostname, self.PID)))
            self.log('starting')
        if not os.path.isdir(self.worker_dir):
            os.mkdir(self.worker_dir)
        if not os.path.isdir(self.comms_to_worker_dir):
            os.mkdir(self.comms_to_worker_dir)
        if not os.path.isdir(self.comms_to_boss_dir):
            os.mkdir(self.comms_to_boss_dir)

        self.comms_count=0;

        if not os.path.isdir('par_run/logs'):
            os.mkdir('par_run/logs');

    def log(self, message):
        if self.log_handle is not None:
            self.log_handle.write(str(datetime.datetime.now())+':'+message+'\n')

    def get_new_job(self):
        response_re=re.compile('response\[(\d+)\]\s+(.*);')

        self.comms_count=self.comms_count+1;

        with open(self.scratch,'w') as fid:
            fid.write("request new job %d;\n" % self.comms_count)
        os.rename(self.scratch, self.to_boss_file)
        self.log(f'sent request {self.comms_count}')
        # wait for a response from the boss
        while not os.path.isfile(self.from_boss_file) and not self.retired:
            self.consider_retirement()
            time.sleep(1)

        # read a line from boss response
        with open(self.from_boss_file,'r') as fid:
            the_line=fid.readline().rstrip()
        os.remove(self.from_boss_file)
        if self.verbose:
            print("parallel_boss sent line [%s]" % the_line)
        self.log(f"received response from boss")

        response_match=response_re.search(the_line)
        if response_match is None:
            self.log("response did not match:"+the_line.rstrip())
            return None
        proc_count=response_match.group(1)
        task_file=response_match.group(2)
        if not os.path.isfile(task_file):
            self.log("Task file %s Does not exist, ignoring request\n" % task_file)
            print("Task file %s Does not exist, ignoring request\n" % task_file)
            return None
        task_num=re.search('task_(\d+)', task_file).group(1)
        return [proc_count, task_file, task_num]

    def consider_retirement(self):
        if (not os.path.isdir(self.comms_to_worker_dir)) and (self.comms_count>0):
            self.retired=True


    def run_job(self, proc_count, task_file, task_num):
        self.log("starting task_file:"+task_file)
        # move the task file to the running file
        running_file="par_run/running/task_%s_%s_%s.%s"%(task_num, self.hostname, self.PID, proc_count)
        os.rename(task_file, running_file)

        # make the running file executable
        stats=os.stat(running_file)
        os.chmod(running_file, stats.st_mode | stat.S_IEXEC)

        # creat the log file
        log_file='par_run/logs/'+os.path.basename(running_file)+'.log';
        log_fid=open(log_file,'wb')

        if self.verbose:
            print("----  running task %s in directory %s ----" % (running_file, self.invoke_dir))
            print("----       log file is %s" % log_file)
            print("----       time is %s" % str(datetime.datetime.now()))

        # run the running file
        my_env=os.environ.copy()
        my_env['MKL_NUM_THREADS']='1'
        p=subprocess.Popen(running_file, shell=True, stdout=subprocess.PIPE,  cwd=self.invoke_dir, stderr=subprocess.STDOUT, env=my_env)

        # wait for the task to finish
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

        # move the running file to the done file
        done_file="par_run/done/task_%s_%s_%s.%s"%(task_num, self.hostname, self.PID, proc_count)
        os.rename(running_file, done_file);
        if self.verbose:
            print("------- Finished: %s" % str(datetime.datetime.now()))
            self.log("finished "+task_file)
    def run_loop(self):
        while os.path.isdir(self.comms_to_worker_dir) or self.comms_count==0:
            job_info=self.get_new_job()
            print("job_info="+str(job_info))
            if job_info is not None:
                self.run_job(*job_info)
            self.consider_retirement()
            if self.retired:
                break

def __main__():
    if '--log' in sys.argv:
        log=True
    else:
        log=False
    this_worker = pworker(log=log)
    this_worker.run_loop()

if __name__=='__main__':
    __main__()





