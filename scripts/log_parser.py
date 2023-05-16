#!/usr/bin/env python
# coding: utf-8
import glob
import re
import json
import os
#import matplotlib.pyplot as plt
import sys


def get_logs(log_dir):
    tile_re = re.compile('working on .*/E(.*)_N(.*).h5')
    trace_line_re = re.compile('(File "(.*)", line (\d+), in (\S+))')
    error_re = re.compile(r'^((\S+Error): (.*))')

    file_exception = {}
    file_trace = {}
    xy_exception = {}
    xy_trace = {}

    for log_file in glob.glob(os.path.join(log_dir,'*.log')):
        trace_line_info  = None
        this_xy = None
        with open(log_file,'r') as fh:
            for line in fh:
                m = tile_re.search(line)
                if m is not None:
                    this_xy=tuple([*map(int, m.groups())])
                m = trace_line_re.search(line)
                if m is not None:
                    trace_line_info = {'str':m.groups()[0], 'file':m.groups()[1], 'line':m.groups()[2], 'function':m.groups()[3]}
                m = error_re.search(line)
                if m is not None:
                    trace_line_info['exception']=m.groups()[0]
                    break
        if trace_line_info is None:
            continue
        if trace_line_info['exception'] not in file_exception:
            file_exception[trace_line_info['exception']] = []

        if trace_line_info['str'] not in file_trace:
            file_trace[trace_line_info['str']]=[]

        file_exception[trace_line_info['exception']] += [log_file]
        file_trace[trace_line_info['str']] += [log_file]
        if this_xy is not None:
            if trace_line_info['exception'] not in xy_exception:
                xy_exception[trace_line_info['exception']] = []
            if trace_line_info['str'] not in xy_trace:
                xy_trace[trace_line_info['str']]=[]
            xy_exception[trace_line_info['exception']] += [this_xy]
            xy_trace[trace_line_info['str']] += [this_xy]
    out={}
    out['counts']={}
    out['counts']['by_exception']={key:len(file_exception[key]) for key in file_exception}
    out['counts']['by_trace']={key:len(file_trace[key]) for key in file_trace}
    out['by_exception']=file_exception
    out['by_trace']=file_trace
    if len(xy_exception) > 0:
        out['xy_by_exception']=xy_exception
        out['xy_by_trace']=xy_trace
    return out

def main():
            
    par_run_dir=sys.argv[1]

    log_dir=os.path.join(par_run_dir,'logs')

    out=get_logs(log_dir)

    print('Unique exceptions and counts:')
    for exc, files in out['by_exception'].items():
        print(f'\t{exc} : {len(files)}')
    print('\nUnique trace lines and counts:')
    for tr, files in out['by_trace'].items():
        print(f'\t{tr} : {len(files)}')

    out_file=os.path.join(par_run_dir, 'log_summary.json')
    with open(out_file,'w') as fh:
        json.dump(out, fh, indent=4)


if __name__=='__main__':
    main()

