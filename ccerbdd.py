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

    def __init__(self, info):
        self.id = next(id_counter)
        self.worker = ccerb.Future()
        return


class JobQueue:
    def __init__(self):
        self.queue = collections.deque()
        return


    def next_id(self):
        try:
            return self.queue[0].id
        except IndexError:
            return float('+inf') # Sort empty queues to the back.


    def __del__(self):
        for job in self.queue:
            job.worker.reject()
        return


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

####

Worker = namedtuple('Worker', 'info, payload')

####

def accept_worker(conn, addr):
    info = str(net_util.recv_buffer(conn))
    payload = net_util.recv_buffer(conn)
    worker = Worker(info, payload)

    job_keys = net_util.recv_buffer(conn)
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

def accept_job(conn, addr):
    info = str(net_util.recv_buffer(conn))
    job_key = net_util.recv_buffer(conn)
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
