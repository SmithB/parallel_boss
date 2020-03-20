#! /usr/bin/env bash

[ -d par_run ] && rm -rf par_run


> queue.txt
for i in {1..8}; do
    echo "./sleeper.py" >> queue.txt

done

pboss.py -s queue.txt

echo ""
echo "starting parallel boss as a background job"
pboss.py -r -w &

echo ""
echo "starting parallel workers in tmux"
run_pworkers -s 3

