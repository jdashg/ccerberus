#!/usr/bin/env python2
from __future__ import print_function
assert __name__ == '__main__'

from collections import namedtuple

import bisect
import itertools
import math
import multiprocessing
import os
import select
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import threading

import ccerb
import net_util

####################

SLOT_COUNT = multiprocessing.cpu_count()

CONFIG = ccerb.parse_ini(ccerb.CONFIG_PATH)
assert 'bin' in CONFIG
assert len(CONFIG['bin'])

try:
    PUBLIC_PORT = int(CONFIG[None]['port'])
except KeyError:
    (_, PUBLIC_PORT) = ccerb.CCERBD_LOCAL_ADDR
PUBLIC_ADDR = ('', PUBLIC_PORT)

####

JOB_MAP = dict()
JOB_MAP['wait'] = net_util.wait_on_beacon

####################

print_lock = threading.Lock()

def locked_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

ccerb.print_func = locked_print

####################

class ScopedTempDir:
    def __init__(self):
        return

    def __enter__(self):
        self.path = tempfile.mkdtemp()
        return self

    def __exit__(self, ex_type, ex_val, ex_traceback):
        shutil.rmtree(self.path)
        return

####

def run_in_temp_dir(input_files, args):
    with ScopedTempDir() as temp_dir:
        ccerb.write_files(temp_dir.path, input_files)

        p = subprocess.Popen(args, bufsize=-1, cwd=temp_dir.path, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, universal_newlines=True)
        (outdata, errdata) = p.communicate()
        returncode = p.returncode
        assert returncode != None # Should have exited.

        for (file_rel_path, _) in input_files:
            file_path = os.path.join(temp_dir.path, file_rel_path)
            os.remove(file_path)
            continue

        output_files = ccerb.read_files(temp_dir.path)

    return (returncode, outdata, errdata, output_files)

####

def run_remote_job_server(conn, job_bin):
    job_args = str(net_util.recv_buffer(conn))
    job_args = job_args.split('\0')

    input_files = ccerb.recv_files(conn)

    with net_util.WaitBeacon(conn):
        args = [job_bin] + job_args
        (returncode, outdata, errdata, output_files) = run_in_temp_dir(input_files, args)

    net_util.send_struct(conn, '<i', returncode)
    net_util.send_buffer(conn, outdata)
    net_util.send_buffer(conn, errdata)

    ccerb.send_files(conn, output_files)
    return

####

for (job_bin, _) in CONFIG['bin'].viewitems():
    def job_func(conn):
        return run_remote_job_server(conn, job_bin)

    job_key = ccerb.get_job_key(job_bin)
    JOB_MAP[job_key] = job_func
    continue

########################################


####

class Scheduler:
    def __init__(self, slots):
        self.max_slots = slots
        self.lock = threading.Lock()
        self.pending = PriorityQueue()
        self.active = set()
        return


    def _process(self):
        try:
            while len(self.active) < self.max_slots:
                cur = self.pending.pop()
                self.active.add(cur)
                cur.ready_event.set()
        except IndexError:
            pass


    def enqueue(self, priority, info):
        return Scheduler.TimeSlot(self, priority, info)


    ####

    class TimeSlot:
        def __init__(self, scheduler, priority, info):
            self.scheduler = scheduler
            self.priority = priority
            self.info = info
            self.ready_event = threading.Event()
            return


        def __lt__(self, x):
            return self.priority < x.priority


        def __enter__(self):
            with self.scheduler.lock:
                self.scheduler.pending.insert(self)
                self.scheduler._process()
            return self


        def acquire(self, timeout=None):
            return self.ready_event.wait(timeout)


        def __exit__(self, ex_type, ex_val, ex_traceback):
            with self.scheduler.lock:
                try:
                    self.scheduler.active.remove(self)
                except KeyError:
                    self.scheduler.pending.remove(self)
                self.scheduler._process()


####

SCHED = Scheduler(SLOT_COUNT)

########################################

def acquire_and_run(conn, info):
    try:
        job_key = str(net_util.recv_buffer(conn))
    except (net_util.ExSocketClosed, socket.error):
        # This is a graceful exit.
        return False

    try:
        job_func = JOB_MAP[job_key]
    except KeyError:
        locked_print('[{}] Unrecognized job_key: {}'.format(info, job_key))
        return False

    priority = net_util.recv_byte(conn)

    with SCHED.enqueue(priority, info) as timeslot:
        while not timeslot.acquire(conn.gettimeout() * 0.5):
            net_util.send_byte(conn, 0)
        net_util.send_byte(conn, 1)

        job_func(conn)
    return True

########################################

def accept(conn, host_info):
    start = time.time()
    while acquire_and_run(conn, host_info):
        continue

    diff = time.time() - start
    diff = int(diff * 1000)
    ccerb.v_log(2, '<~accept({}): {}ms>', host_info, diff)
    return


def accept_public(conn, addr):
    ccerb.v_log(2, 'accept_public({})', addr)

    conn.settimeout(ccerb.NET_TIMEOUT)

    host_info = str(net_util.recv_buffer(conn))
    host_info = '{}@{}'.format(host_info, addr)
    accept(conn, host_info)
    return


def accept_local(conn, addr):
    ccerb.v_log(3, 'accept_local({})', addr)

    conn.settimeout(ccerb.NET_TIMEOUT)

    ccerbdd_addr = None
    net_util.send_pickle(conn, ccerbdd_addr)

    host_info = 'localhost'
    accept(conn, host_info)
    return

########################################

log_counter = itertools.count(1)

def accept_log(conn, addr):
    conn.settimeout(None)

    lines = []

    try:
        while True:
            lines.append(unicode(net_util.recv_buffer(conn)))
    except (net_util.ExSocketClosed, socket.error):
        pass

    if not lines:
        return

    log_id = next(log_counter)
    prefix = u'[log {}] '.format(log_id)
    sep = u'\n' + prefix
    text = prefix + sep.join(lines)

    locked_print(text)
    return

########################################

ccerb.nice_down()

net_util.spawn_thread(net_util.serve_forever, (PUBLIC_ADDR, accept_public))
net_util.spawn_thread(net_util.serve_forever, (ccerb.CCERBD_LOCAL_ADDR, accept_local))
net_util.spawn_thread(net_util.serve_forever, (ccerb.CCERBD_LOG_ADDR, accept_log))

####

net_util.sleep_until_keyboard()
exit(0)
