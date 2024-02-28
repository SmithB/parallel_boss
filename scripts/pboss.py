#! /usr/bin/env python3

import os, sys, stat, argparse, subprocess, glob, shutil
from parallel_boss import boss


def setup_directories():
    # setup the directories
    dir_list=['par_run','par_run/queue','par_run/running','par_run/done','par_run/comms', 'par_run/logs']
    for thedir in dir_list:
        if not(os.path.isdir(thedir)):
            os.mkdir(thedir)

def get_last_task():
    if os.path.isfile('par_run/last_task'):
        fh=open('par_run/last_task','r');
        for line in fh:
            temp=line;
        last_file_num=int(temp);
        fh.close()
    else:
        last_file_num=0;
    return last_file_num


def add_files_to_queue(task_list_file, matlab=False, sh=False, csh=False, bash=False, env=None):
    last_file_num=get_last_task()
    fh=open(task_list_file,'r');
    add_count=0
    for line in fh:
        last_file_num=last_file_num+1;
        this_file='par_run/queue/task_%d' % last_file_num
        out_file=open(this_file,'w');
        #print("adding %s to queue" % this_file)
        add_count +=1
        if sh is True:
            out_file.write('#! /usr/bin/env sh\n')
        elif bash is True:
            out_file.write('#! /usr/bin/env bash\n')
        elif csh is True:
            out_file.write('#! /usr/bin/env csh\n')
        if env is not None:
            out_file.write("source activate %s\n" % env)

        out_file.write('%s\n'% line.rstrip());
        out_file.close();
        if sh or csh or bash:
            os.chmod(this_file, os.stat(this_file).st_mode | stat.S_IEXEC)
    print(f"added {add_count} files to the queue")
    fh=open('par_run/last_task','w+')
    fh.write('%d\n'% last_file_num)
    fh.close()

def add_glob_to_queue(glob_str):
    last_file_num=get_last_task()
    file_list=glob.glob(glob_str)
    add_count=0
    for file in file_list:
        last_file_num += 1
        this_file='par_run/queue/task_%d' % last_file_num
        shutil.copyfile(file, this_file)
        os.chmod(this_file, os.stat(this_file).st_mode | stat.S_IEXEC)
        add_count += 1
    print(f"added {add_count} files to the queue")
    with open('par_run/last_task','w+') as fh:
        fh.write('%d\n'% last_file_num)

def __main__():
    parser = argparse.ArgumentParser(description='Start parallel boss (no arguments) or add jobs to the queue (-m or -s options).')
    parser.add_argument('--task_glob', '-g', type=str, default=None, help="directory containing task files")
    parser.add_argument('--matlab_list', '-m', type=str, default=None, help="filename containing matlab jobs")
    parser.add_argument('--bash_list', '-b', type=str, default=None, help="filename containing bash jobs")
    parser.add_argument('--sh_list', '-s', type=str, default=None, help="filename containing sh jobs")
    parser.add_argument('--csh_list', '-c', type=str, default=None, help="filename containing csh jobs")
    parser.add_argument('--environment','-e', type=str, default=None, help="environment that each job will activate")
    parser.add_argument('--log', action='store_true')
    parser.add_argument('--delay', type=float, help="time to wait between subsequent jobs")
    parser.add_argument('--jobs','-j', type=int, default=0, help="number of workers to run")
    parser.add_argument('--keep_running','-k', action='store_true', help= "if set, pboss will not exit after it runs out of jobs and will wait for more jobs to be added to the queue")
    parser.add_argument('--run','-r', action='store_true', help="if set, pboss will run (otherwise, jobs are added to the queue, and the process exits" )
    parser.add_argument('--wait', '-w', action='store_true', help="if set, pboss will wait for all jobs to fninsh before exiting")
    parser.add_argument('--preserve','-p', action='store_true', help="if set, when pboss exits, the comms directory will remain, so that workers do not exit")
    parser.add_argument('--quiet','-Q', action='store_true', help="if set, verbose output from pboss is suppressed")
    args=parser.parse_args()

    if args.jobs >0 :
        args.run=True

    setup_directories()

    # it looks like the environment argument only works in bash mode
    if args.environment is not None and args.sh_list is not None:
        args.bash_list=args.sh_list
        args.sh_list=None

    if args.matlab_list is not None:
        if not args.quiet:
            print("\t pboss: adding files from %s to queue in par_run/queue in Matlab mode.\n" % sys.argv[1])
        add_files_to_queue(args.matlab_list, matlab=True)
    if args.sh_list is not None:
        if not args.quiet:
            print("\t pBoss: adding files from %s to queue in par_run/queue in sh mode.\n" % sys.argv[1])
        add_files_to_queue(args.sh_list, sh=True, env=args.environment)

    if args.csh_list is not None:
        if not args.quiet:
            print("parallel_boss: adding files from %s to queue in par_run/queue in csh mode.\n" % sys.argv[1])
        add_files_to_queue(args.csh_list, csh=True, env=args.environment)

    if args.bash_list is not None:
        if not args.quiet:
            print("parallel_boss: adding files from %s to queue in par_run/queue in bash mode.\n" % sys.argv[1])
        add_files_to_queue(args.bash_list, bash=True, env=args.environment)

    if args.task_glob is not None:
        if not args.quiet:
            print("parallel_boss: adding files from %s to queue in par_run/queue in bash mode.\n" % args.task_glob)
        add_glob_to_queue(args.task_glob)
    for arg in [args.matlab_list, args.sh_list, args.csh_list, args.bash_list, args.task_glob]:
        if arg is not None and not args.run:
            return

    if args.jobs > 0:
        if args.sh_list is not None or args.csh_list is not None or args.bash_list is not None or args.task_glob is not None:
            if not args.quiet:
                print("starting %d shell jobs" % args.jobs)
            subprocess.call(["run_pworkers", "-s" , str(args.jobs)])
        if args.matlab_list is not None:
            if not args.quiet:
                print("starting %d matlab jobs" % args.jobs)
                print(["run_pworkers", "-m "+str(args.jobs)])
            subprocess.call(["run_pworkers", "-m ", str(args.jobs)])

    the_boss=boss(preserve=args.preserve, keep_running=args.keep_running,
                  wait_for_workers_to_finish=args.wait,
                  quiet=args.quiet, delay=args.delay, log=args.log)
    the_boss.run()

if __name__ == '__main__':
    __main__()
