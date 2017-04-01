assert __name__ != '__main__'

from __future__ import print_function

import os
import socket
import subprocess
import sys
import threading

import net_util

####

VERBOSE = 2
CONFIG_PATH = os.path.expanduser('~/.ccerb.ini')
CCERBD_LOCAL_ADDR = ('localhost', 14305)
NET_TIMEOUT = 0.300

####

def get_job_key(job_bin):
    args = [job_bin, '-v']
    print(args)
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (outdata, errdata) = p.communicate()

    job_key = errdata.splitlines()[0]
    if VERBOSE >= 2:
        print('get_job_key({}):'.format(args), job_key, file=sys.stderr)

    return job_key

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

def recv_files(conn):
    file_count = net_util.recv_struct(conn, '<Q')
    files = []
    for _ in range(file_count):
        name = str(net_util.recv_buffer(conn))
        data = net_util.recv_buffer(conn)
        files.append((name, data))
    return files


def send_files(conn, files):
    net_util.send_struct(conn, '<Q', len(files))
    for (name, data) in files:
        net_util.send_buffer(conn, name)
        net_util.send_buffer(conn, data)

####

class ExMalformedIni(Exception):
    pass


def parse_ini(path):
    headings = dict()
    cur_heading = None
    headings[cur_heading] = dict()

    with open(path, 'rb') as f:
        line_num = -1
        for line in f:
            line_num += 1
            line = line.strip()
            if not line or line[0] == '#':
                continue

            if line[0] == '[':
                if line[-1] != ']':
                    text = 'Unmatched \'[\' on line {}'
                    raise ExMalformedIni(text.format(line_num))

                cur_heading = line[1:-1]
                if cur_heading in headings:
                    text = 'Duplicate heading \'{}\' on line {}'
                    raise ExMalformedIni(text.format(cur_heading, line_num))

                headings[cur_heading] = dict()
                continue

            try:
                (k, v) = line.split('=', 1)
            except ValueError:
                (k, v) = (line, '')

            if k in headings[cur_heading]:
                text = 'Duplicate key \'{}\' in heading \'{}\' on line {}'
                raise ExMalformedIni(text.format(k, cur_heading, line_num))

            headings[cur_heading][k] = v
            continue
    return headings

####

def nice_down():
    if sys.platform == 'win32':
        import psutil
        p = psutil.Process()
        p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
    else:
        os.nice(10)

####

def acquire_remote_job(conn, job_key, priority):
    net_util.send_buffer(conn, job_key)
    net_util.send_byte(conn, priority)

    while net_util.recv_byte(conn) == 0:
        continue

####

class Future:
    class Rejection(Exception):
        pass


    def __init__(self):
        self.lock = threading.Lock()
        self.event = threading.Event()
        self.val = ()
        return


    def is_resolved(self):
        return self.event.is_set()


    def accept(self, val):
        with self.lock:
            if self.event.is_set():
                return False
            self.val = (val, )
            self.event.set()
        return True


    def reject(self):
        with self.lock:
            if self.event.is_set():
                return False
            self.event.set()
        return True


    def await(self):
        self.event.wait()
        try:
            return self.val[0]
        except IndexError:
            raise Rejection()
