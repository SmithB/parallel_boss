#! /usr/bin/env python3

import sys, os, time, re, subprocess, datetime, stat, json, shutil

class pworker(object):
    def __init__(self, log=False, old_style=False):

        self.verbose=True
        self.retired=False
        self.PID=os.getpid()
        self.hostname=os.uname()[1]
        self.invoke_dir=os.getcwd()
        self.old_style=old_style

        if not os.path.isdir("par_run/comms"):
            print("par_run/comms directory not found, waiting")
            while not os.path.isdir("par_run/comms"):
                time.sleep(1)

        config = self._read_worker_config()
        self.conda_env = config.get('CONDA_ENV')
        self.conda_prefix = self._resolve_conda_prefix(self.conda_env) if self.conda_env else None
        self.venv = config.get('VIRTUAL_ENV')
        if self.conda_prefix:
            print(f"pworker: running jobs in conda environment '{self.conda_env}'")
        elif self.conda_env:
            print(f"pworker: WARNING: conda environment '{self.conda_env}' not found, running without env override")
        elif self.venv:
            print(f"pworker: running jobs in virtual environment '{self.venv}'")

        self.worker_dir="par_run/comms/worker_%s.%s"%(self.hostname, self.PID)
        self.comms_to_worker_dir=os.path.join(self.worker_dir, "to_worker")
        self.comms_to_boss_dir=os.path.join(self.worker_dir, "to_boss")
        self.from_boss_file=os.path.join(self.comms_to_worker_dir, 'request.txt')
        self.ack_file=os.path.join(self.comms_to_worker_dir, 'request_received.txt')
        self.to_boss_file=os.path.join(self.comms_to_boss_dir, 'request.txt')
        self.scratch=os.path.join(self.worker_dir, "scratch_from_worker.txt")
        self.current_log_file=None

        self.log_handle=None
        if log:
            log_dir="par_run/worker_logs"
            if not os.path.isdir(log_dir):
                os.mkdir(log_dir)
            self.log_handle=open(os.path.join(log_dir, "worker_%s.%s"%(self.hostname, self.PID)),'w', buffering=1)
            self.log('starting')
        if not os.path.isdir(self.worker_dir):
            os.mkdir(self.worker_dir)
        if not os.path.isdir(self.comms_to_worker_dir):
            os.mkdir(self.comms_to_worker_dir)
        if not os.path.isdir(self.comms_to_boss_dir):
            os.mkdir(self.comms_to_boss_dir)

        self.comms_count=0
        self.jobs_done=0
        self.done_count_file=os.path.join(self.worker_dir, 'done_count')

        if not os.path.isdir('par_run/logs'):
            os.mkdir('par_run/logs');
        if not self.old_style:
            if not os.path.isdir('par_run/active_logs'):
                os.mkdir('par_run/active_logs')

    def _read_worker_config(self):
        config_file = 'par_run/worker_config'
        if not os.path.isfile(config_file):
            return {}
        config = {}
        with open(config_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, _, value = line.partition('=')
                    config[key.strip()] = value.strip()
        return config

    def _resolve_conda_prefix(self, conda_env):
        if not conda_env:
            return None
        try:
            result = subprocess.run(
                ['conda', 'info', '--json'],
                capture_output=True, text=True
            )
            envs = json.loads(result.stdout).get('envs', [])
            prefix = next((p for p in envs if os.path.basename(p) == conda_env), None)
            if prefix is None:
                print(f"pworker: WARNING: conda environment '{conda_env}' not found")
            return prefix
        except Exception as e:
            print(f"pworker: WARNING: could not resolve conda environment '{conda_env}': {e}")
            return None

    def log(self, message):
        if self.log_handle is not None:
            self.log_handle.write(str(datetime.datetime.now())+':'+message+'\n')

    def get_new_job(self):
        response_re=re.compile(r'response\[(\d+)\]\s+(.*);')

        self.comms_count=self.comms_count+1;

        with open(self.scratch,'w') as fid:
            fid.write("request new job %d;\n" % self.comms_count)
        os.rename(self.scratch, self.to_boss_file)
        self.log(f'sent request {self.comms_count}')
        # wait for a response from the boss
        while not os.path.isfile(self.from_boss_file) and not self.retired:
            self.consider_retirement()
            time.sleep(1)

        if self.retired:
            return None

        # read the boss response, delete it immediately (prevents stale re-reads),
        # then write the ack so the boss knows the message was received
        with open(self.from_boss_file,'r') as fid:
            the_line=fid.readline().rstrip()
        os.remove(self.from_boss_file)
        with open(self.ack_file,'w') as fid:
            fid.write('received\n')
        if self.verbose:
            print("parallel_boss sent line [%s]" % the_line)
        self.log(f"received response from boss")

        response_match=response_re.search(the_line)
        if response_match is None:
            self.log("response did not match:"+the_line.rstrip())
            return None
        proc_count=response_match.group(1)
        task_file=response_match.group(2)
        # retry briefly in case NFS cache hasn't yet made the file visible
        for _ in range(10):
            if os.path.isfile(task_file):
                break
            time.sleep(0.5)
        else:
            self.log("Task file %s not visible after retries, ignoring\n" % task_file)
            print("Task file %s not visible after retries, ignoring\n" % task_file)
            return None
        task_match=re.search(r'task_(\d+)', task_file)
        if task_match is None:
            self.log("Could not parse task number from filename: "+task_file)
            return None
        task_num=task_match.group(1)
        return [proc_count, task_file, task_num]

    def consider_retirement(self):
        if (not os.path.isdir(self.comms_to_worker_dir)) and (self.comms_count>0):
            self.retired=True


    def run_job(self, proc_count, task_file, task_num):
        self.log("starting task_file:"+task_file)
        # move the task file to the running file
        if self.old_style:
            running_file="par_run/running/task_%s_%s_%s.%s"%(task_num, self.hostname, self.PID, proc_count)
        else:
            running_file="par_run/running/task_%s" % (task_num)
        os.rename(task_file, running_file)

        # make the running file executable
        stats=os.stat(running_file)
        os.chmod(running_file, stats.st_mode | stat.S_IEXEC)

        # create the log file
        if self.old_style:
            log_file = 'par_run/logs/'+os.path.basename(running_file)+'.log';
        else:
            self.current_log_file = 'par_run/active_logs/'+os.path.basename(running_file)+'.log'
            log_file = self.current_log_file
        log_fid=open(log_file,'wb')

        if self.verbose:
            print("----  running task %s in directory %s ----" % (running_file, self.invoke_dir))
            print("----       log file is %s" % log_file)
            print("----       time is %s" % str(datetime.datetime.now()))
        if not self.old_style:
            log_fid.write(f'#pworker_hostname: {self.hostname}\n'.encode())
            log_fid.write(f'#pworker_PID: {self.PID}\n'.encode())
            log_fid.write(f'#pworker_proc_count: {proc_count}\n'.encode())
            log_fid.write(f'#pworker_invoke_dir: {self.invoke_dir}\n'.encode())
            log_fid.flush()

        # run the running file
        my_env=os.environ.copy()
        my_env['MKL_NUM_THREADS']='1'
        if self.conda_prefix:
            cmd = f'conda run --no-capture-output --prefix {self.conda_prefix} bash {running_file}'
        elif self.venv:
            cmd = running_file
            my_env['PATH'] = os.path.join(self.venv, 'bin') + os.pathsep + my_env.get('PATH', '')
            my_env['VIRTUAL_ENV'] = self.venv
            my_env.pop('CONDA_DEFAULT_ENV', None)
            my_env.pop('CONDA_PREFIX', None)
        else:
            cmd = running_file
        p=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,  cwd=self.invoke_dir, stderr=subprocess.STDOUT, env=my_env)

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
        returncode = p.returncode
        log_fid.write(f"#job return code: {returncode}\n".encode())
        log_fid.close()

        # move the running file to the done file
        if self.old_style:
            done_file="par_run/done/task_%s_%s_%s.%s"%(task_num, self.hostname, self.PID, proc_count)
        else:
            done_file = f"par_run/done/{os.path.basename(running_file)}"
        os.rename(running_file, done_file)

        if not self.old_style:
            dst_log_file = 'par_run/logs/'+os.path.basename(running_file)+'.log'
            os.rename(self.current_log_file, dst_log_file)

            if returncode > 0:
                error_log_file  = 'par_run/error_logs/'+os.path.basename(running_file)+'.log'
                shutil.copyfile(dst_log_file, error_log_file)

        if self.verbose:
            print("------- Finished: %s" % str(datetime.datetime.now()))
            self.log("finished "+task_file)

    def run_loop(self):
        while os.path.isdir(self.comms_to_worker_dir) or self.comms_count==0:
            job_info=self.get_new_job()
            if job_info is not None:
                self.run_job(*job_info)
                self.jobs_done += 1
                with open(self.done_count_file, 'w') as fh:
                    fh.write(f'{self.jobs_done}\n')
            self.consider_retirement()
            if self.retired:
                break

def __main__():
    import argparse
    parser = argparse.ArgumentParser(description='Run a parallel_boss worker process.')
    parser.add_argument('--log', action='store_true', help='write a worker log file to par_run/worker_logs/')
    parser.add_argument('--old_style', action='store_true', help='use old-style task/log filenames that include hostname and PID')
    args = parser.parse_args()
    this_worker = pworker(log=args.log, old_style=args.old_style)
    this_worker.run_loop()

if __name__=='__main__':
    __main__()
