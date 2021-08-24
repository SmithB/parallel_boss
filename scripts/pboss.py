#! /usr/bin/env python3

import os, sys, stat, argparse, subprocess
from parallel_boss import boss


def setup_directories():
    # setup the directories
    dir_list=['par_run','par_run/queue','par_run/running','par_run/done','par_run/comms', 'par_run/logs']
    for thedir in dir_list:
        if not(os.path.isdir(thedir)):
            os.mkdir(thedir)


def add_files_to_queue(task_list_file, matlab=False, shell=False, csh=False):
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
    parser.add_argument('--quiet','-Q', action='store_true')
    args=parser.parse_args()

    if args.jobs >0 :
        args.run=True

    setup_directories()

    if args.matlab_list is not None:
        if not args.quiet:
            print("\t pboss: adding files from %s to queue in par_run/queue in Matlab mode.\n" % sys.argv[1])
        add_files_to_queue(args.matlab_list, matlab=True)
        if not args.run:# or ( args.jobs is not None ):
                return
    if args.shell_list is not None:
        if not args.quiet:
            print("\t pBoss: adding files from %s to queue in par_run/queue in Shell mode.\n" % sys.argv[1])
        add_files_to_queue(args.shell_list, shell=True)
        if not args.run:
            return
    if args.csh_list is not None:
        if not args.quiet:
            print("parallel_boss: adding files from %s to queue in par_run/queue in Shell mode.\n" % sys.argv[1])
        add_files_to_queue(args.csh_list, csh=True)
        if not args.run:
            return


    if args.jobs > 0:
        if args.shell_list is not None or args.csh_list is not None:
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
                  quiet=args.quiet)
    the_boss.run()

if __name__ == '__main__':
    __main__()
