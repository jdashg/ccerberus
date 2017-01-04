#!/usr/bin/env python2

from __future__ import print_function

import math
import multiprocessing
import os
import shutil
import subprocess
import sys
import tempfile
import threading

import schedaemon
import schedaemon_util

####

VERBOSE = 2
LOCAL_ADDR = ('localhost', 14304)
REMOTE_ADDR = ('', 14305)
COMPILE_PRIORITY = -2

####

cc_key_map = dict()

####

def compile_temp(cc_bin, compile_args, source_file_name, source_data):
    compile_dir = tempfile.mkdtemp()

    try:
        source_file_path = os.path.join(compile_dir, source_file_name)
        with file(source_file_path, 'wb') as f:
            f.write(source_data)

        args = [cc_bin] + compile_args + [source_file_name]
        p = subprocess.Popen(args, cwd=compile_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (outdata, errdata) = p.communicate()
        returncode = p.returncode
        assert returncode != None

        os.remove(source_file_path)

        result_files = []
        if returncode == 0:
            for root, dirs, files in os.walk(compile_dir):
                for x in files:
                    path = os.path.join(root, x)
                    print('reading', path)

                    with open(path, 'rb') as f:
                        data = f.read()

                    rel_path = os.path.relpath(path, compile_dir)
                    result_files.append((rel_path, data))

    finally:
        shutil.rmtree(compile_dir)

    return (returncode, outdata, errdata, result_files)

####

def accept_local(conn, addr):
    conn.settimeout(None)

    try:
        cc_key = str(schedaemon_util.recv_buffer(conn))
        print('cc_key:', cc_key)
        try:
            cc_bin = 'cl'#cc_key_map[cc_key]
        except KeyError:
            return

        source_file_name = str(schedaemon_util.recv_buffer(conn))
        num_compile_args = schedaemon_util.recv_struct(conn, '<Q')
        compile_args = []
        while len(compile_args) != num_compile_args:
            compile_arg = str(schedaemon_util.recv_buffer(conn))
            compile_args.append(compile_arg)
            continue

        preproc_data = schedaemon_util.recv_buffer(conn)
    except schedaemon_util.ExSocketClosed:
        return

    (returncode, outdata, errdata, files) = compile_temp(cc_bin, compile_args,
                                                         source_file_name, preproc_data)

    conn.sendall(bytearray([returncode]))
    schedaemon_util.send_buffer(conn, outdata)
    schedaemon_util.send_buffer(conn, errdata)
    if returncode == 0:
        schedaemon_util.send_struct(conn, '<Q', len(files))
        for (name, data) in files:
            schedaemon_util.send_buffer(conn, name)
            schedaemon_util.send_buffer(conn, data)

    return

####

def accept_remote(conn, addr):
    return

####

if __name__ == '__main__':
    schedaemon_util.spawn_thread(schedaemon_util.serve_forever, (LOCAL_ADDR, accept_local))
    schedaemon_util.spawn_thread(schedaemon_util.serve_forever, (REMOTE_ADDR, accept_remote))

    p = subprocess.Popen(('python2.7', 'schedaemon.py'), shell=True)
    # Interestingly, spawning schedaemon here will always succeed.
    # This extra schedaemon will fail to bind to addresses until another schedaemon exits.

    '''
    cpu_count = multiprocessing.cpu_count()
    for x in range(cpu_count):
        name = 'slot{}'.format(x)
        t = threading.Thread(target=thread__slot, name=name)
        t.daemon = True
        t.start()
        continue
    '''

    schedaemon_util.sleep_until_keyboard()
    exit(0)
