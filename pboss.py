#! /usr/bin/env python3

import os, time, re, glob, argparse, sys, stat, subprocess, shutil

def setup():
    # setup the directories
    dir_list=['par_run','par_run/queue','par_run/running','par_run/done','par_run/comms', 'par_run/logs']
    for thedir in dir_list:
        if not(os.path.isdir(thedir)):
            os.mkdir(thedir)
    state=dict()
    return state

def cleanup(boss_file, preserve=False):
    # cleanup: delete all the comms files so that the workers exit
    if not preserve:
        for comms_dir in glob.glob('par_run/comms/worker_*'):
            shutil.rmtree(comms_dir)
    if os.path.isfile(boss_file):
        os.remove(boss_file)
    return

def check_for_workers(state):
    # make sure state knows about all comm files
    #print "checking for workers" 
    worker_dirs=glob.glob('par_run/comms/worker_*')
    for this_dir in worker_dirs:
        worker_match=re.search('/worker_(.*)', this_dir)
        if worker_match is not None:
            worker_name=worker_match.group(1);
            #print "found worker_name=%s" % worker_name
            if not worker_name in state:
                print("found new worker_name=%s" % worker_name)
                # subtract 1 from the time of the new worker so that when we check it, it will show up as new. 
                state[worker_name]={'to_boss':this_dir+'/to_boss/request.txt', 
                     'to_worker':this_dir+'/to_worker/request.txt', 
                     'scratch':this_dir+'/scratch.txt'};
    # make sure that there's a comm file for each worker in state
    for worker in list(state.keys()):
        this_dir='par_run/comms/worker_%s' % worker
        if this_dir not in worker_dirs:
            del state[worker]
    #print "found %d workers" % len(state)
    return(state)

def check_for_new_comms(state, comms):
    for this_worker in state.keys():
        if os.path.isfile(state[this_worker]['to_boss']):
            with open(state[this_worker]['to_boss'],'r') as fid:
                comms[this_worker]=fid.readline().rstrip()  
            os.remove(state[this_worker]['to_boss'])
    return(state, comms)

def respond_to_comms(state, comms, task_list):
    for this_worker in list(comms.keys()):
        req_match=re.search('request new job (.*);', comms[this_worker])
        if req_match is not None:
            request_name=req_match.group(1);
            if len(task_list['not_started']) > 0:
                this_task=task_list['not_started'].pop(0)
                task_list['started'].append(this_task)
                print(state[this_worker]['to_worker'])
                with open(state[this_worker]['scratch'],'w') as fid:
                    fid.write('response[%s] %s;\n' % (request_name, this_task))
                    fid.close()
                os.rename(state[this_worker]['scratch'], state[this_worker]['to_worker'])
                del comms[this_worker]
        else:
            print("Misunderstood communication from %s : %s\n" % (this_worker, comms[this_worker]))
    return state, comms

def update_task_list(task_list):
    tasks=glob.glob('par_run/queue/task*')
    this_task_list=dict();
    for task in tasks:
        this_task_num=int(re.search('task_(\d+)',task).group(1));
        this_task_list[this_task_num]=task;
    #print this_task_list
    for task_num in sorted(this_task_list):
        task=this_task_list[task_num]
        if task not in task_list['started'] and task not in task_list['not_started']:
            task_list['not_started'].append(task)
    
    return task_list

def add_file_to_queue(task_list_file, matlab=False, shell=False, csh=False):
    if os.path.isfile('par_run/last_task'):
        fh=open('par_run/last_task','r');
        for line in fh:
            temp=line;
        last_file_num=int(temp);
        fh.close()
    else:
        last_file_num=0;
    
    fh=open(task_list_file,'r');
    add_count=0
    for line in fh:
        last_file_num=last_file_num+1;
        this_file='par_run/queue/task_%d' % last_file_num
        out_file=open(this_file,'w');
        #print("adding %s to queue" % this_file)
        add_count +=1
        if shell is True:
            out_file.write('#! /usr/bin/env bash\n')
        elif csh is True:
            out_file.write('#! /usr/bin/env csh\n')
        out_file.write('%s\n'% line.rstrip());
        out_file.close();
        if shell is True or csh is True:
            os.chmod(this_file, os.stat(this_file).st_mode | stat.S_IEXEC)
    print(f"added {add_count} files to the queue")
    fh=open('par_run/last_task','w+')
    fh.write('%d\n'% last_file_num)
    fh.close()


def __main__():
    parser = argparse.ArgumentParser(description='Start parallel boss (no arguments) or add jobs to the queue (-m or -s options).')
    parser.add_argument('--matlab_list', '-m', type=str, default=None)
    parser.add_argument('--shell_list', '-s', type=str, default=None)
    parser.add_argument('--csh_list', '-c', type=str, default=None)
    parser.add_argument('--jobs','-j', type=int, default=0)
    parser.add_argument('--keep_running','-k', action='store_true')
    parser.add_argument('--run','-r', action='store_true')
    parser.add_argument('--wait', '-w', action='store_true')
    parser.add_argument('--preserve','-p', action='store_true')
    args=parser.parse_args()

    if args.jobs >0 :
        args.run=True
    state=setup()

    comms=dict()
    if args.matlab_list is not None:
        print("parallel_boss: adding files from %s to queue in par_run/queue in Matlab mode.\n" % sys.argv[1])
        add_file_to_queue(args.matlab_list, matlab=True)
        if not args.run:# or ( args.jobs is not None ):
                return
    if args.shell_list is not None:
        print("parallel_boss: adding files from %s to queue in par_run/queue in Shell mode.\n" % sys.argv[1])
        add_file_to_queue(args.shell_list, shell=True)
        if not args.run:
            return
    if args.csh_list is not None:
        print("parallel_boss: adding files from %s to queue in par_run/queue in Shell mode.\n" % sys.argv[1])
        add_file_to_queue(args.csh_list, csh=True)
        if not args.run:
            return

    state['preserve'] = args.preserve

    boss_file="par_run/boss_status_%s_%s" %(os.uname()[1], str(os.getpid()))
    boss_check=glob.glob("par_run/boss_status_*")
    if len(boss_check) > 0:
        print ("boss file %s exists,exiting" % boss_check[0])
        return
    fh=open(boss_file,'w');
    fh.write('delete to kill the queue boss\n')
    fh.close();
    if args.jobs > 0:
        if args.shell_list is not None or args.csh_list is not None:
            print("starting %d shell jobs" % args.jobs)
            subprocess.call(["run_pworkers", "-s" , str(args.jobs)])
        if args.matlab_list is not None:
            print("starting %d matlab jobs" % args.jobs)
            print(["run_pworkers", "-m "+str(args.jobs)])
            subprocess.call(["run_pworkers", "-m ", str(args.jobs)])
    num_running=0
    task_list=update_task_list({'not_started':[], 'started': []})

    while os.path.isfile(boss_file):
        state=check_for_workers(state)
        comms_last= comms.copy()
        [state, comms]=check_for_new_comms(state, comms)
        while len(comms) > 0 and os.path.isfile(boss_file):
            if comms_last != comms:
                print("have communication from processes:")
                print(comms)
            if len(task_list['not_started']) == 0:
                task_list=update_task_list(task_list)
                if (len(task_list['not_started']) == 0) and args.keep_running is False:
                    # wait 10 seconds while the workers grab the jobs
                    time.sleep(3)
                    if args.wait is True:
                        last_num_running=num_running
                        num_running=len(glob.glob('par_run/running/task*'))
                        if num_running > 0:
                            if num_running != last_num_running:
                                print("waiting for %d jobs to finish\n" % num_running)
                            time.sleep(2)
                            comms_last=comms
                            state, comms=respond_to_comms(state, comms, task_list)
                            continue
                        else:
                            print("Exited at 181 after running out of tasks")
                            cleanup(boss_file, preserve=args.preserve)
                            return
                    else:
                        print("Exited at 185 after running out of tasks")
                        cleanup(boss_file, preserve=args.preserve)
                        return
                print("found %d tasks" % len(task_list['not_started']))
            else:
                print("have %d tasks"  % len(task_list['not_started']))
            state, comms=respond_to_comms(state, comms, task_list)
        time.sleep(1)
    print("exiting main loop at 190")
    cleanup(boss_file, preserve=args.preserve)
    return

if __name__ == '__main__':
    __main__()
