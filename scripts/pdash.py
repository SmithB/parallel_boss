#! /usr/bin/env python3
"""ASCII dashboard for monitoring parallel_boss job progress."""

import os, glob, re, time, sys, datetime, argparse

BOX_W        = 76   # total width including border characters
SUMMARY_BAR_W = 49  # width of the overall progress bar
HOST_BAR_W   = 41   # width of per-host bars (narrower to fit worker/running counts)
HOST_NAME_W  = 18   # hostname column width
# Per-host row: 2 + HOST_NAME_W + 2('[') + HOST_BAR_W + 2('] ') + 3('NNw') + 1 + 3('NNr') = 72


def read_hostname_from_log(par_run_dir, task_num):
    log_file = os.path.join(par_run_dir, f'active_logs/task_{task_num}.log')
    if not os.path.isfile(log_file):
        return None
    try:
        with open(log_file, 'r', errors='replace') as fh:
            for line in fh:
                m = re.match(r'#pworker_hostname:\s*(.*)', line)
                if m:
                    return m.group(1).strip()
    except OSError:
        pass
    return None


def get_stats(par_run_dir):
    # Build host table from comms directories so hosts persist while workers are alive.
    worker_dirs = glob.glob(os.path.join(par_run_dir, 'comms/worker_*'))
    hosts = {}   # hostname -> {'workers': N, 'tasks': [task_num_str, ...]}
    for d in worker_dirs:
        m = re.search(r'worker_(.+)\.\d+$', d)
        if m:
            hostname = m.group(1)
            hosts.setdefault(hostname, {'workers': 0, 'tasks': []})
            hosts[hostname]['workers'] += 1

    # running/ is always small (one entry per active job); glob it for task-to-host mapping.
    running_files = glob.glob(os.path.join(par_run_dir, 'running/task_*'))
    for f in running_files:
        bn = os.path.basename(f)
        old_m = re.match(r'task_(\d+)_([^_]+)_', bn)
        if old_m:
            hostname = old_m.group(2)
            task_num = old_m.group(1)
        else:
            new_m = re.match(r'task_(\d+)$', bn)
            task_num = new_m.group(1) if new_m else bn
            hostname = read_hostname_from_log(par_run_dir, task_num) or 'unknown'
        hosts.setdefault(hostname, {'workers': 0, 'tasks': []})
        hosts[hostname]['tasks'].append(task_num)

    boss_files = glob.glob(os.path.join(par_run_dir, 'boss_status_*'))

    # Queue count: read from boss status file written each loop iteration;
    # fall back to globbing when the boss is not running.
    boss_counts = {}
    status_file = os.path.join(par_run_dir, 'status')
    if os.path.isfile(status_file):
        try:
            with open(status_file) as fh:
                for line in fh:
                    m = re.match(r'(\w+)=(\d+)', line.strip())
                    if m:
                        boss_counts[m.group(1)] = int(m.group(2))
        except (OSError, ValueError):
            pass

    if 'queue' in boss_counts:
        n_queue = boss_counts['queue']
    else:
        n_queue = len(glob.glob(os.path.join(par_run_dir, 'queue/task_*')))

    # Done count: sum each worker's own done_count file (no write races, no large glob);
    # fall back to globbing done/ when workers have exited and cleaned up.
    done_count_files = glob.glob(os.path.join(par_run_dir, 'comms/worker_*/done_count'))
    if done_count_files:
        n_done = 0
        for f in done_count_files:
            try:
                with open(f) as fh:
                    n_done += int(fh.read().strip())
            except (OSError, ValueError):
                pass
    else:
        n_done = len(glob.glob(os.path.join(par_run_dir, 'done/task_*')))

    last_task = 0
    last_task_file = os.path.join(par_run_dir, 'last_task')
    if os.path.isfile(last_task_file):
        try:
            with open(last_task_file) as fh:
                lines = [l.strip() for l in fh if l.strip()]
            if lines:
                last_task = int(lines[-1])
        except (OSError, ValueError):
            pass

    return {
        'queue':        n_queue,
        'running':      len(running_files),
        'done':         n_done,
        'hosts':        hosts,
        'boss_running': len(boss_files) > 0,
        'n_workers':    len(worker_dirs),
        'last_task':    last_task,
    }


def summary_bar(done, running, total, width):
    """= for done, # for running, - for queued."""
    if total == 0:
        return '-' * width
    done_w = round(done / total * width)
    run_w  = min(round(running / total * width), width - done_w)
    return '=' * done_w + '#' * run_w + '-' * (width - done_w - run_w)


def host_bar(task_nums, scale, width):
    """Dots with a digit count at each running task's scaled position.

    Position of task N: int((N-1) * width / scale), clamped to [0, width-1].
    Digit shows how many of this host's jobs map to that column; + for >= 10.
    """
    counts = {}
    for t in task_nums:
        try:
            n = int(t)
        except (ValueError, TypeError):
            continue
        if scale > 0:
            pos = min(int((n - 1) * width / scale), width - 1)
            counts[pos] = counts.get(pos, 0) + 1
    row = ['.'] * width
    for pos, cnt in counts.items():
        row[pos] = str(cnt) if cnt < 10 else '+'
    return ''.join(row)


def _row(text):
    inner = BOX_W - 4
    if len(text) > inner:
        text = text[:inner - 3] + '...'
    return f'| {text:<{inner}} |'


def render(stats, interval):
    inner = BOX_W - 4
    now   = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    queue   = stats['queue']
    running = stats['running']
    done    = stats['done']
    total   = queue + running + done
    scale   = max(total, stats['last_task'])

    pct         = done / total * 100 if total > 0 else 0.0
    sbar        = summary_bar(done, running, total, SUMMARY_BAR_W)
    count_label = f'{done}/{total} ({pct:.0f}%)'
    boss_str    = 'RUNNING' if stats['boss_running'] else 'stopped'

    divider = '+' + '-' * (BOX_W - 2) + '+'
    rows = []
    rows.append(divider)
    rows.append(_row(f"{'Parallel Boss Dashboard':^{inner}}"))
    rows.append(_row(f"{'Updated: ' + now:^{inner}}"))
    rows.append(divider)
    rows.append(_row(f'[{sbar}] {count_label}'))
    rows.append(_row(f'  Queued: {queue:<8}  Running: {running:<8}  Done: {done}'))
    rows.append(_row(f'  Boss: {boss_str:<14}  Workers online: {stats["n_workers"]}'))
    rows.append(divider)
    rows.append(_row('  Workers by host:'))
    rows.append(_row(''))

    if stats['hosts']:
        for host in sorted(stats['hosts']):
            info = stats['hosts'][host]
            w    = info['workers']
            r    = len(info['tasks'])
            bar  = host_bar(info['tasks'], scale, HOST_BAR_W)
            rows.append(_row(f'  {host[:HOST_NAME_W]:<{HOST_NAME_W}} [{bar}] {w:>2}w {r:>2}r'))
    else:
        rows.append(_row('  (no workers active)'))

    rows.append(divider)
    rows.append(f'  Ctrl+C to exit  |  refreshes every {interval}s'
                f'  |  = done  # running  - queued  w=workers  r=running')
    return '\n'.join(rows)


def main():
    parser = argparse.ArgumentParser(description='Monitor parallel_boss job progress.')
    parser.add_argument('--dir', '-d', default='par_run',
                        help='path to par_run directory (default: par_run)')
    parser.add_argument('--interval', '-i', type=float, default=2.0,
                        help='refresh interval in seconds (default: 2)')
    parser.add_argument('--once', action='store_true',
                        help='print once and exit instead of looping')
    args = parser.parse_args()

    if not os.path.isdir(args.dir):
        print(f"Error: directory '{args.dir}' not found.", file=sys.stderr)
        sys.exit(1)

    try:
        while True:
            stats = get_stats(args.dir)
            if not args.once:
                sys.stdout.write('\033[2J\033[H')   # clear screen, home cursor
            print(render(stats, args.interval))
            sys.stdout.flush()
            if args.once:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
