#!/usr/bin/env python2
from __future__ import print_function
assert __name__ == '__main__'

from collections import namedtuple

import marshal
import math
import multiprocessing
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import threading
import uuid

import ccerb
import schedaemon
import net_util

'''
Two main cases:
* Almost empty
* Almost full
---
workers pull when ready

workers start activated:
    loop:
        if poll_for_job:

---
workers update  expiring 'empty slot counts', which we monotonically decrease as we assign
jobs to workers
'''

####

service_map = dict()

conn_list_lock = threading.Lock()
conn_list = []

####

class Worker:
    def __init(self, addr, slots):
        self.addr = addr
        self.remaining_slots = slots
        return

job_queue_map = WeakValueDictionary()
global_queue_lock = threading.Lock()

class Job:
    id_counter = count(1)

    def __init__(self, key, info):
        self.id = next(id_counter)
        self.key = key
        self.worker = ccerb.Future()
        return

    def __lt__(self, x):
        return self.id < x.id


class JobQueue:
    def __init__(self):
        self.lock = threading.Lock()
        self.queue = ccerb.PriorityQueue()
        return


    def put(self, job):
        with self.lock:
            self.queue.insert(job)


    def next_id(self):
        try:
            return self.queue[0].id
        except IndexError:
            return float('+inf') # Sort empty queues to the back.


    def await_worker(self, info):
        job = Job(info)
        with global_queue_lock:
            if not self.queue:
                return None
            self.queue.append(job)

        try:
            return job.worker.await()
        except ccerb.Future.Rejection:
            return None


def get_next_job(job_queue_list):
    with global_queue_lock:
        next_job_queue = min(job_queue_list, key=JobQueue.next_id)
        try:
            return next_job_queue.popleft()
        except IndexError:
            return None


class JobServer:
    def __init__(self):
        self.lock = threading.Lock()
        self.queues_by_key = dict()
        return

    def put(self, job):
        with self.lock:
            queue = self.setdefault(job.key, JobQueue())

        queue.put(job)





####

Worker = namedtuple('Worker', 'info, payload')

####

def accept_worker(conn, addr):
    info = str(net_util.recv_buffer(conn))
    payload = net_util.recv_buffer(conn)
    worker = Worker(info, payload)

    job_keys = str(net_util.recv_buffer(conn))
    job_keys = job_keys.split('\0')

    job_queue_list = [job_queue_map.setdefault(x, JobQueue()) for x in job_keys]

    while True:
        net_util.recv_poke(conn)
        job = get_next_job(job_queue_list)
        if not job:
            continue;

        job.worker.accept(worker)
        continue

    return

####

def accept(conn, addr):
    info = str(net_util.recv_buffer(conn))
    cmd = str(net_util.recv_buffer(conn))
    if (cmd == 'put'):
        job_key = str(net_util.recv_buffer(conn))
        job_info = '{}@{}'.format(info, addr)


    try:
        job_queue = job_queue_map[job_key]
    except KeyError:
        return

    worker = job_queue.acquire_worker(job_info)
    if not worker:
        return

    net_util.send_buffer(conn, worker.info)
    net_util.send_buffer(conn, worker.payload)
    return

####

if __name__ == '__main__':
    net_util.spawn_thread(net_util.serve_forever, (DIRECTORY_ADDR, accept))

    ####

    net_util.sleep_until_keyboard()
    exit(0)
