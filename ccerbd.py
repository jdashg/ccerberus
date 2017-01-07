#!/usr/bin/env python2

from __future__ import print_function

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

import schedaemon
import schedaemon_util

####

VERBOSE = 2
LOCAL_ADDR = ('localhost', 14304)
REMOTE_ADDR = ('', 14305)
REMOTE_NET_TIMEOUT = 0.300
RECONN_TOKEN_BYTES = 16

REMOTE_COMPILE_PRIORITY = -3

####

cc_key_map = dict()

####

def get_cc_key(cc_bin):
    p = subprocess.Popen([cc_bin, '-v'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (_, errdata) = p.communicate()

    cc_key = errdata.splitlines()[0]
    if VERBOSE >= 2:
        print('cc_key:', cc_key, file=sys.stderr)

    return cc_key

####

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

def read_files(root_dir):
    ret = []
    for cur_root, cur_dirs, cur_files in os.walk(root_dir):
        for x in cur_files:
            path = os.path.join(cur_root, x)
            print('reading', path)

            with open(path, 'rb') as f:
                data = f.read()

            rel_path = os.path.relpath(path, root_dir)
            ret.append((rel_path, data))
    return ret


def write_files(root_dir, files):
    for (file_rel_path, file_data) in files:
        dir_name = os.path.dirname(file_rel_path)
        if dir_name:
            os.makedirs(dir_name)
        file_path = os.path.join(root_dir, file_rel_path)
        with open(file_path, 'wb') as f:
            f.write(file_data)

####

def run_in_temp_dir(input_files, args):
    with ScopedTempDir() as temp_dir:
        write_files(temp_dir.path, input_files)

        p = subprocess.Popen(args, cwd=temp_dir.path, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        (outdata, errdata) = p.communicate()
        returncode = p.returncode
        assert returncode != None # Should have exited.

        for (file_rel_path, _) in input_files:
            file_path = os.path.join(temp_dir.path, file_rel_path)
            os.remove(file_path)
            continue

        output_files = read_files(temp_dir.path)

    return (returncode, outdata, errdata, output_files)

####

def remote_compile_server(conn, cc_bin):
    conn.settimeout(None)

    try:
        source_file_name = str(schedaemon_util.recv_buffer(conn))
        compile_args_str = str(schedaemon_util.recv_buffer(conn))
        compile_args = compile_args_str.split('\0')
        source_data = schedaemon_util.recv_buffer(conn)
    except schedaemon_util.ExSocketClosed:
        return

    input_files = [(source_file_name, source_data)]
    args = [cc_bin] + compile_args + [source_file_name]
    (returncode, outdata, errdata, output_files) = run_in_temp_dir(input_files, args)

    schedaemon_util.send_struct(conn, '<i', returncode)
    schedaemon_util.send_buffer(conn, outdata)
    schedaemon_util.send_buffer(conn, errdata)
    schedaemon_util.send_struct(conn, '<Q', len(output_files))
    for (name, data) in output_files:
        schedaemon_util.send_buffer(conn, name)
        schedaemon_util.send_buffer(conn, data)

    return

########################################

PendingReconnect = namedtuple('PendingReconnect', 'job, cc_bin, addr')

pending_reconnects_lock = threading.Lock()
pending_reconnects = dict()

def pop_pending_reconnect(reconn_token):
    with pending_reconnects_lock:
        ret = pending_reconnects[reconn_token]
        del pending_reconnects[reconn_token]

    return ret

####

def thread__pending_compile_timeout(reconn_token):
    time.sleep(REMOTE_NET_TIMEOUT)

    try:
        cur = pop_pending_reconnect(reconn_token)
    except KeyError:
        return

    addr = cur.addr
    if VERBOSE >= 1:
        print('[{}] Pending reconnect timed out: {}'.format(addr, reconn_token))
    cur.job.cancel()
    return

####

def accept_remote_notify(conn, addr):
    if VERBOSE >= 2:
        print('accept_remote_notify({})'.format(addr))

    cc_key = str(schedaemon_util.recv_buffer(conn))
    try:
        cc_bin = cc_key_map[cc_key]
    except KeyError:
        print('[{}] Unrecognized cc_key: {}'.format(addr, cc_key))
        return

    info = 'ccerbd-compile@{}: {}'.format(addr, cc_key)
    print(info)

    while True:
        try:
            job = schedaemon.ScheduledJob(REMOTE_COMPILE_PRIORITY, info)
        except schedaemon.ExTimeout:
            return

        if not job.acquire():
            return

        reconn_token = os.urandom(RECONN_TOKEN_BYTES)
        with pending_reconnects_lock:
            assert reconn_token not in pending_reconnects
            pending_reconnects[reconn_token] = PendingReconnect(job, cc_bin, addr)

        schedaemon_util.send_buffer(conn, reconn_token)

        schedaemon_util.spawn_thread(thread__pending_compile_timeout, (reconn_token,))

        conn.settimeout(None)
        if not schedaemon_util.recv_poke(conn):
            return

####

def accept_remote(conn, addr):
    if VERBOSE >= 2:
        print('accept_remote({})'.format(addr))

    conn.settimeout(REMOTE_NET_TIMEOUT)
    reconn_token = str(schedaemon_util.recv_buffer(conn))
    if not reconn_token:
        return accept_remote_notify(conn, addr)

    ####

    try:
        cur = pop_pending_reconnect(reconn_token)
    except KeyError:
        return

    cc_bin = cur.cc_bin
    job = cur.job

    ####

    remote_compile_server(conn, cc_bin)
    job.cancel()

    return

########################################

def acquire_remote(cc_key):
    addr_list = [REMOTE_ADDR]

    gai_list = []
    for (host, port) in addr_list:
        gai_list += socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)

    for gai in gai_list:
        print('gai:', gai)
        (family, socktype, proto, _, sockaddr) = gai
        remote_conn = None
        try:
            remote_conn = socket.socket(family, socktype, proto)
            remote_conn.settimeout(REMOTE_NET_TIMEOUT)
            remote_conn.connect(sockaddr)
        except (socket.error, socket.timeout) as e:
            if remote_conn:
                schedaemon_util.kill_socket(remote_conn)
            print('gai failed:', e)
            continue
        break
    else:
        print('out of gai')
        return (None, None)

    try:
        schedaemon_util.send_buffer(remote_conn, '') # no token
        schedaemon_util.send_buffer(remote_conn, cc_key)

        remote_conn.settimeout(None)
        reconn_token = str(schedaemon_util.recv_buffer(remote_conn))
        return (gai, reconn_token)
    except schedaemon_util.ExSocketClosed:
        print('no token')
        return (None, None)
    finally:
        schedaemon_util.kill_socket(remote_conn)

####

def accept_local(conn, addr):
    if VERBOSE >= 2:
        print('accept_local({})'.format(addr))

    cc_key = str(schedaemon_util.recv_buffer(conn))
    try:
        cc_bin = cc_key_map[cc_key]
    except KeyError:
        print('[{}] Unrecognized cc_key: {}'.format(addr, cc_key))
        return

    ####

    (remote_gai, reconn_token) = acquire_remote(cc_key)
    if not remote_gai:
        print('[{}] Failed to find remotes for: {}'.format(addr, cc_key))
        return

    remote_gai_str = marshal.dumps(remote_gai)
    schedaemon_util.send_buffer(conn, remote_gai_str)
    schedaemon_util.send_buffer(conn, reconn_token)
    return

########################################

if __name__ == '__main__':
    cc_list = ['cl']
    for cc_bin in cc_list:
        cc_key = get_cc_key(cc_bin)
        cc_key_map[cc_key] = cc_bin
        continue

    ####

    p = subprocess.Popen(('python2.7', 'schedaemon.py'), shell=True)
    # Interestingly, spawning schedaemon here will always succeed.
    # This extra schedaemon will fail to bind to addresses until another schedaemon exits.

    ####

    schedaemon_util.spawn_thread(schedaemon_util.serve_forever, (REMOTE_ADDR, accept_remote))
    schedaemon_util.spawn_thread(schedaemon_util.serve_forever, (LOCAL_ADDR, accept_local))

    ####

    schedaemon_util.sleep_until_keyboard()
    exit(0)
