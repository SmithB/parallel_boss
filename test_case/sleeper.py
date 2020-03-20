#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 11:06:07 2020

@author: ben
"""

import os
import sys
import time
proc_num=os.getpid()

for count in range(10):
    print(f"sleeper: PID={proc_num}, count={count}", flush=True)
    time.sleep(2)

print("sleeper: PID={proc_num}. Done", flush=True)
