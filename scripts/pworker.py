#! /usr/bin/env python3

import sys, os, time, re, subprocess, datetime, stat

class pworker(object):
    def __init__(self):

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

        if not os.path.isdir(self.worker_dir):
            os.mkdir(self.worker_dir)
        if not os.path.isdir(self.comms_to_worker_dir):
            os.mkdir(self.comms_to_worker_dir)
        if not os.path.isdir(self.comms_to_boss_dir):
            os.mkdir(self.comms_to_boss_dir)

        self.comms_count=0;

        if not os.path.isdir('par_run/logs'):
            os.mkdir('par_run/logs');

    def get_new_job(self):
        response_re=re.compile('response\[(\d+)\]\s+(.*);')

        self.comms_count=self.comms_count+1;

        with open(self.scratch,'w') as fid:
            fid.write("request new job %d;\n" % self.comms_count)
        os.rename(self.scratch, self.to_boss_file)

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
        response_match=response_re.search(the_line)
        if response_match is None:
            return None
        proc_count=response_match.group(1)
        task_file=response_match.group(2)
        if not os.path.isfile(task_file):
            print("Task file %s Does not exist, ignoring request\n" % task_file)
            return None
        task_num=re.search('task_(\d+)', task_file).group(1)
        return [proc_count, task_file, task_num]

    def consider_retirement(self):
        if (not os.path.isdir(self.comms_to_worker_dir)) and (self.comms_count>0):
            self.retired=True


    def run_job(self, proc_count, task_file, task_num):

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

    def run_loop(self):
        while os.path.isdir(self.comms_to_worker_dir) or self.comms_count==0:
            job_info=self.get_new_job()
            print("job_info="+str(job_info))
            if job_info is not None:
                self.run_job(*job_info)
            self.consider_retirement()
            print("outside: retired is:"+str(self.retired))
            if self.retired:
                break

def __main__():

    this_worker = pworker()
    this_worker.run_loop()

if __name__=='__main__':
    __main__()





