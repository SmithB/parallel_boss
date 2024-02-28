#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 23 23:01:08 2021

@author: ben
"""


import os, time, re, glob, shutil, datetime


class boss(object):
    def __init__(self, keep_running=False, preserve=False,
                 wait_for_workers_to_finish=False, quiet=False, log=False, delay=None):
        self.workers={}
        self.comms={}
        self.comms_last={}
        self.comms_updated=False
        self.boss_file=''
        self.preserve=preserve
        self.keep_running=keep_running
        self.quiet=quiet
        self.wait_for_workers_to_finish=wait_for_workers_to_finish
        self.task_list={'not_started':[], 'started': []}
        self.boss_file="par_run/boss_status_%s_%s" %(os.uname()[1], str(os.getpid()))
        self.delay=delay
        self.log_handle = None
        self.outbox=[]
        boss_check=glob.glob("par_run/boss_status_*")
        if len(boss_check) > 0:
            print ("boss file %s exists,exiting" % boss_check[0])
            return
        fh=open(self.boss_file,'w');
        fh.write('delete to kill the queue boss\n')
        fh.close();
        if log:
            self.log_handle=open("par_run/boss_log_%s_%s" %(os.uname()[1], str(os.getpid())), 'w', buffering=1)

    def log(self, message):
        if self.log_handle is not None:
            self.log_handle.write(str(datetime.datetime.now())+':'+message+'\n')

    def cleanup(self):
        # cleanup: delete all the comms files so that the workers exit
        if not self.preserve:
            for comms_dir in glob.glob('par_run/comms/worker_*'):
                shutil.rmtree(comms_dir)
        if os.path.isfile(self.boss_file):
            os.remove(self.boss_file)
        return

    def check_for_workers(self):
        # make sure state knows about all comm files
        #print "checking for workers"
        worker_dirs=glob.glob('par_run/comms/worker_*')
        for this_dir in worker_dirs:
            worker_match=re.search('/worker_(.*)', this_dir)
            if worker_match is not None:
                worker_name=worker_match.group(1);
                #print "found worker_name=%s" % worker_name
                if not worker_name in self.workers:
                    if not self.quiet:
                        print("found new worker_name=%s" % worker_name)
                    self.log("found new worker directory: "+this_dir)
                    # subtract 1 from the time of the new worker so that when we check it, it will show up as new.
                    self.workers[worker_name]={'to_boss':this_dir+'/to_boss/request.txt',
                         'to_worker':this_dir+'/to_worker/request.txt',
                         'scratch':this_dir+'/scratch.txt'};
        # make sure that there's a comm file for each worker in state
        for worker in list(self.workers.keys()):
            this_dir='par_run/comms/worker_%s' % worker
            if this_dir not in worker_dirs:
                del self.workers[worker]

    def check_for_new_comms(self):
        self.comms_last=self.comms.copy()
        for name, worker in self.workers.items():
            if os.path.isfile(worker['to_boss']):
                with open(worker['to_boss'],'r') as fid:
                    self.comms[name]=fid.readline().rstrip()
                    self.log('found to_boss file: '+worker['to_boss'])
                os.remove(worker['to_boss'])
        self.comms_updated =  not (self.comms_last==self.comms)

    def respond_to_comms(self):
        self.log(f'responding to {len(self.comms.keys())} comms')
        for this_worker in list(self.comms.keys()):
            req_match=re.search('request new job (.*);', self.comms[this_worker])
            if req_match is not None:
                request_name=req_match.group(1);
                if len(self.task_list['not_started']) > 0:
                    this_task=self.task_list['not_started'].pop(0)
                    self.task_list['started'].append(this_task)
                    if not self.quiet:
                        print("\t sending:"+self.workers[this_worker]['to_worker'])
                    self.log("sending:"+self.workers[this_worker]['to_worker'])
                    with open(self.workers[this_worker]['scratch'],'w') as fid:
                        fid.write('response[%s] %s;\n' % (request_name, this_task))
                        fid.close()
                    os.rename(self.workers[this_worker]['scratch'], self.workers[this_worker]['to_worker'])
                    self.outbox += [ self.workers[this_worker]['to_worker']]
                    del self.comms[this_worker]
                    if self.delay is not None:
                        self.log(f'waiting {self.delay} seconds before starting next job')
                        if not self.quiet:
                            print(f'waiting {self.delay} seconds before starting next job')
                        time.sleep(self.delay)
            else:
                print("Misunderstood communication from %s : %s\n" % (this_worker, self.comms[this_worker]))

    def wait_for_workers(self):
        self.log("waiting for workers to take jobs")
        while len(self.outbox) > 0:
            for to_worker_file in self.outbox:
                if not os.path.isfile(to_worker_file):
                    self.outbox.remove(to_worker_file)
            time.sleep(0.025)
        self.log("done waiting for workers")
    def update_task_list(self):
        task_list=self.task_list
        tasks=glob.glob('par_run/queue/task*')
        this_task_list=dict();
        for task in tasks:
            this_task_num=int(re.search('task_(\d+)',task).group(1));
            this_task_list[this_task_num]=task;
        for task_num in sorted(this_task_list):
            task=this_task_list[task_num]
            if task not in task_list['started'] and task not in task_list['not_started']:
                task_list['not_started'].append(task)

    def run(self):
        self.update_task_list()
        if not self.quiet:
            print("\t found %d tasks" % len(self.task_list['not_started']))
        num_running=0
        while os.path.isfile(self.boss_file):
            self.check_for_workers()
            self.check_for_new_comms()
            while len(self.comms) > 0 and os.path.isfile(self.boss_file):
                if self.comms_updated:
                    self.comms_last=self.comms.copy()
                    self.comms_updated=False
                    if not self.quiet:
                        print("\t have communication from processes:")
                        print("\t\t"+str(self.comms))
                if len(self.task_list['not_started']) == 0:
                    self.update_task_list()
                    if (len(self.task_list['not_started']) == 0) and self.keep_running is False:
                        # wait for the workers to grab the jobs
                        self.wait_for_workers()
                        if self.wait_for_workers_to_finish:
                            self.update_task_list()
                            last_num_running=num_running
                            num_running=len(glob.glob('par_run/running/task*'))
                            if num_running > 0:
                                if num_running != last_num_running:
                                    if not self.quiet:
                                        print("\t waiting for %d jobs to finish\n" % num_running)
                                time.sleep(0.5)
                                continue
                            else:
                                if not self.quiet:
                                    print("\t Ran out of tasks, and finished waiting")
                                self.cleanup()
                                return
                        else:
                            if not self.quiet:
                                print("\t Ran out of tasks")
                            self.cleanup()
                            return
                    if not self.quiet:
                        print("\t found %d tasks" % len(self.task_list['not_started']))
                else:
                    if not self.quiet:
                        print("have %d tasks"  % len(self.task_list['not_started']))
                self.respond_to_comms()
            time.sleep(0.05)
        print(f"File {self.boss_file} has been deleted")
        self.cleanup()
        return
