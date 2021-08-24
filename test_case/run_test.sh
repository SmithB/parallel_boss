#! /usr/bin/env bash

[ -d par_run ] && rm -rf par_run


> first_queue.txt
for i in {1..8}; do
    echo "./sleeper.py $i" >> first_queue.txt
done

> second_queue.txt
for i in {9..15}; do
    echo "./sleeper.py $i" >> second_queue.txt
done

pboss.py -s first_queue.txt

echo ""
echo "starting parallel boss as a background job"
pboss.py -r -w &

echo ""
echo "starting two parallel workers in tmux"
run_pworkers -s 2

sleep 2
echo ""
echo "starting two more workers"
run_pworkers -s 2

sleep 5

echo "adding more jobs to the queue"
pboss.py -s second_queue.txt
