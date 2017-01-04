#!/usr/bin/env python2

from __future__ import print_function

import multiprocessing
import socket
import sys
import threading
import time

import schedaemon_util

####

VERBOSE = 2

ADDR = ('localhost', 47955)
TIMEOUT = 0.100

PRIORITY_STRUCT = '<b'
INFO_LEN_STRUCT = '<B'

####

class PQueue:
    def __init__(self, fnGetKey):
        self.fnGetKey = fnGetKey
        self.list = []
        return


    def put_right(self, val):
        val_key = (self.fnGetKey)(val)
        insert_pos = 0
        for x in self.list:
            x_key = (self.fnGetKey)(x)
            if x_key >= val_key:
                insert_pos += 1
                continue
            break

        self.list.insert(insert_pos, val)
        return insert_pos


    def erase(self, val):
        try:
            self.list.remove(val)
        except ValueError:
            pass
        return


    def pop_left(self):
        return self.list.pop(0)


class ThreadedPQueue:
    def __init__(self, fnGetKey):
        self.pq = PQueue(fnGetKey)
        self.condvar = threading.Condition()
        return


    def put_right(self, val):
        with self.condvar:
            insert_pos = self.pq.put_right(val)
            self.condvar.notifyAll()
            pass

        return insert_pos


    def erase(self, val):
        with self.condvar:
            self.pq.erase(val)

        return


    def pop_left(self):
        with self.condvar:
            while True:
                try:
                    return self.pq.pop_left()
                except IndexError:
                    self.condvar.wait()
                    continue

########################3###############

class Job:
    def __init__(self, priority, info):
        self.priority = priority
        self.info = info

        self.begin_event = threading.Event()
        self.end_event = threading.Event()
        return

    def __str__(self):
        return 'Job({}, \'{}\')'.format(self.priority, self.info)


def get_job_priority(job):
    return job.priority


g_pq = ThreadedPQueue(get_job_priority)

####

def thread__slot():
    while True:
        job = g_pq.pop_left()
        job.begin_event.set()
        job.end_event.wait()
        continue

####

def sched_accept(conn, addr):
    if VERBOSE >= 2:
        print('new conn: ', addr)

    conn.settimeout(TIMEOUT)
    try:
        priority = schedaemon_util.recv_struct(conn, PRIORITY_STRUCT)
        info_len = schedaemon_util.recv_struct(conn, INFO_LEN_STRUCT)
        info = str(schedaemon_util.recv_n(conn, info_len))

    except (socket.timeout, socket.error, schedaemon_util.ExSocketClosed):
        return

    ####

    job = Job(priority, info)
    if VERBOSE >= 1:
        print('<new {}>'.format(job))
    g_pq.put_right(job)

    ####

    conn.setblocking(True)

    try:
        time.sleep(0.3) # XXX
        job.begin_event.wait()
        if VERBOSE >= 1:
            start = time.time()
            print('<+{}>'.format(job))

        conn.sendall( bytearray([1]) )
        conn.recv(1) # Blocks until recv or socket dies.
    except socket.error:
        pass

    if VERBOSE >= 1:
        time_diff = time.time() - start
        print('<-{}@{}ms>'.format(job, time_diff))

    job.end_event.set()
    g_pq.erase(job)
    return

########################################

class ExTimeout(Exception):
    pass
class ExError(Exception):
    pass

class ScheduledJob:
    def __init__(self, priority=0, info='', timeout=TIMEOUT):
        self.priority = priority
        self.info = info
        self.conn = None

        try:
            conn = socket.create_connection(ADDR, timeout)
            schedaemon_util.send_struct(conn, PRIORITY_STRUCT, self.priority)
            schedaemon_util.send_struct(conn, INFO_LEN_STRUCT, len(self.info))
            conn.sendall(self.info)
        except socket.error:
            raise ExTimeout()

        self.conn = conn
        return


    def _acquire(self, timeout=None):
        assert self.conn
        self.conn.settimeout(timeout)
        try:
            read = self.conn.recv(1)
        except socket.timeout:
            return False

        if not read: # Socket must be dead
            return False

        return True


    def __enter__(self):
        if not self._acquire(None):
            raise ExError()
        return self


    def __exit__(self, ex_type, ex_val, ex_traceback):
        if self.conn:
            schedaemon_util.kill_socket(self.conn)
        self.conn = None
        return

########################################

if __name__ == '__main__':
    cpu_count = multiprocessing.cpu_count()
    for x in range(cpu_count):
        name = 'slot{}'.format(x)
        t = threading.Thread(target=thread__slot, name=name)
        t.daemon = True
        t.start()
        continue

    schedaemon_util.spawn_thread(schedaemon_util.serve_forever, (ADDR, sched_accept))

    schedaemon_util.sleep_until_keyboard()
    exit(0)
