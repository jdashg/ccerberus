#!/usr/bin/env python2
from __future__ import print_function
assert __name__ == '__main__'

import os
import socket
import subprocess
import sys
import threading
import time

import ccerb
import net_util

####################

SHIM_OUT_PRIORITY=10
PREPROC_PRIORITY=100
LOCAL_COMPILE_PRIORITY=110
DEDICATED_COMPILE_PRIORITY=120
REMOTE_COMPILE_PRIORITY=130

####################

CONFIG = ccerb.parse_ini(ccerb.CONFIG_PATH)
assert 'dedicated_remotes' in CONFIG

HOST_INFO = CONFIG[None]['host_info']
NO_LOCAL = int(CONFIG[None].get('no_local', 0))

####################

class ExShimOut(Exception):
    def __init__(self, reason):
        self.reason = reason
        return

####

SOURCE_EXTS = ['c', 'cc', 'cpp']
BOTH_ARGS = ['nologo', '-Tc', '-TC', '-Tp', '-TP']

def process_args(args):
    args = args[:]
    if not args:
        raise ExShimOut('no args')

    source_file_name = None
    is_compile_only = False

    preproc = ['-E']
    compile = ['-c']
    while args:
        cur = args.pop(0)

        if cur == '-E':
            raise ExShimOut('preproc-only')

        if cur == '-c':
            is_compile_only = True
            continue

        if cur == '-showIncludes':
            preproc.append(cur)
            preproc.append('-nologo')
            continue

        if cur in BOTH_ARGS:
            preproc.append(cur)
            compile.append(cur)
            continue

        if cur == '-I':
            preproc.append(cur)
            try:
                next = args.pop(0)
            except:
                raise ExShimOut('missing arg after -I')
            preproc.append(next)
            continue

        if cur.startswith('-D') or cur.startswith('-I'):
            preproc.append(cur)
            continue

        if cur.startswith('-Tc') or cur.startswith('-Tp'):
            raise ExShimOut('-Tp,-Tc unsupported')

        if cur == '-FI':
            preproc.append(cur)
            try:
                next = args.pop(0)
            except:
                raise ExShimOut('missing arg after -FI')
            preproc.append(next)
            continue

        if cur.startswith('-Fo'):
            if os.path.dirname(cur[2:]):
                raise ExShimOut('-Fo target is a path')
            compile.append(cur)
            continue

        split = cur.rsplit('.', 1)
        if len(split) == 2 and split[1].lower() in SOURCE_EXTS:
            if source_file_name:
                raise ExShimOut('multiple source files')

            #if os.path.dirname(cur[2:]):

            source_file_name = os.path.basename(cur)
            preproc.append(cur)
            compile.append(source_file_name)
            continue

        compile.append(cur)
        continue

    if not is_compile_only:
        raise ExShimOut('not compile-only')

    if not source_file_name:
        raise ExShimOut('no source file')

    return (preproc, compile, source_file_name)

####

def run_remote_job_client(conn, job_args, input_files):
    job_args = '\0'.join(job_args)
    net_util.send_buffer(conn, job_args)

    ccerb.send_files(conn, input_files)

    net_util.wait_on_beacon(conn)

    returncode = net_util.recv_struct(conn, '<i')
    outdata = net_util.recv_buffer(conn)
    errdata = net_util.recv_buffer(conn)

    sys.stderr.write(errdata)
    #ccerb.v_log(1, 'errdata: {}', errdata)
    sys.stdout.write(outdata)
    #ccerb.v_log(1, 'outdata: {}', outdata)

    output_files = ccerb.recv_files(conn)

    ccerb.write_files('', output_files)
    return returncode

####

def preproc(cc_bin, preproc_args):
    preproc_args = [cc_bin] + preproc_args
    p = subprocess.Popen(preproc_args, bufsize=-1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

    (outdata, errdata) = p.communicate()
    if p.returncode != 0:
        sys.stderr.write(errdata)
        sys.stdout.write(outdata)
        exit(p.returncode)

    return (outdata, errdata)

####
'''
EXAMPLE_CL_ARGS = [
    'cl.EXE', '-FoUnified_cpp_dom_canvas1.obj', '-c',
    '-Ic:/dev/mozilla/gecko-cinn3-obj/dist/stl_wrappers', '-DDEBUG=1', '-DTRACING=1',
    '-DWIN32_LEAN_AND_MEAN', '-D_WIN32', '-DWIN32', '-D_CRT_RAND_S',
    '-DCERT_CHAIN_PARA_HAS_EXTRA_FIELDS', '-DOS_WIN=1', '-D_UNICODE', '-DCHROMIUM_BUILD',
    '-DU_STATIC_IMPLEMENTATION', '-DUNICODE', '-D_WINDOWS', '-D_SECURE_ATL',
    '-DCOMPILER_MSVC', '-DSTATIC_EXPORTABLE_JS_API', '-DMOZ_HAS_MOZGLUE',
    '-DMOZILLA_INTERNAL_API', '-DIMPL_LIBXUL', '-Ic:/dev/mozilla/gecko-cinn3/dom/canvas',
    '-Ic:/dev/mozilla/gecko-cinn3-obj/dom/canvas',
    '-Ic:/dev/mozilla/gecko-cinn3/js/xpconnect/wrappers',
    '-Ic:/dev/mozilla/gecko-cinn3-obj/ipc/ipdl/_ipdlheaders',
    '-Ic:/dev/mozilla/gecko-cinn3/ipc/chromium/src',
    '-Ic:/dev/mozilla/gecko-cinn3/ipc/glue', '-Ic:/dev/mozilla/gecko-cinn3/dom/workers',
    '-Ic:/dev/mozilla/gecko-cinn3/dom/base', '-Ic:/dev/mozilla/gecko-cinn3/dom/html',
    '-Ic:/dev/mozilla/gecko-cinn3/dom/svg', '-Ic:/dev/mozilla/gecko-cinn3/dom/workers',
    '-Ic:/dev/mozilla/gecko-cinn3/dom/xul', '-Ic:/dev/mozilla/gecko-cinn3/gfx/gl',
    '-Ic:/dev/mozilla/gecko-cinn3/image', '-Ic:/dev/mozilla/gecko-cinn3/js/xpconnect/src',
    '-Ic:/dev/mozilla/gecko-cinn3/layout/generic',
    '-Ic:/dev/mozilla/gecko-cinn3/layout/style',
    '-Ic:/dev/mozilla/gecko-cinn3/layout/xul',
    '-Ic:/dev/mozilla/gecko-cinn3/media/libyuv/include',
    '-Ic:/dev/mozilla/gecko-cinn3/gfx/skia',
    '-Ic:/dev/mozilla/gecko-cinn3/gfx/skia/skia/include/config',
    '-Ic:/dev/mozilla/gecko-cinn3/gfx/skia/skia/include/core',
    '-Ic:/dev/mozilla/gecko-cinn3/gfx/skia/skia/include/gpu',
    '-Ic:/dev/mozilla/gecko-cinn3/gfx/skia/skia/include/utils',
    '-Ic:/dev/mozilla/gecko-cinn3-obj/dist/include',
    '-Ic:/dev/mozilla/gecko-cinn3-obj/dist/include/nspr',
    '-Ic:/dev/mozilla/gecko-cinn3-obj/dist/include/nss', '-MD', '-FI',
    'c:/dev/mozilla/gecko-cinn3-obj/mozilla-config.h', '-DMOZILLA_CLIENT', '-Oy-', '-TP',
    '-nologo', '-wd5026', '-wd5027', '-Zc:sizedDealloc-', '-Zc:threadSafeInit-',
    '-wd4091', '-wd4577', '-D_HAS_EXCEPTIONS=0', '-W3', '-Gy', '-Zc:inline', '-utf-8',
    '-FS', '-Gw', '-wd4251', '-wd4244', '-wd4267', '-wd4345', '-wd4351', '-wd4800',
    '-wd4595', '-we4553', '-GR-', '-Z7', '-Oy-', '-WX',
    '-Ic:/dev/mozilla/gecko-cinn3-obj/dist/include/cairo', '-wd4312',
    'c:/dev/mozilla/gecko-cinn3-obj/dom/canvas/Unified_cpp_dom_canvas1.cpp'
]
'''
####################

def ccerbd_connect(addr):
    remote_conn = socket.create_connection(addr)
    net_util.send_buffer(remote_conn, HOST_INFO)
    return remote_conn

####################

def add_remote_addr(addr, job_key, priority):
    def thread():
        try:
            remote_conn = ccerbd_connect(addr)
        except socket.timeout:
            return
        try_remote_conn(remote_conn, job_key, priority)
        return

    t = threading.Thread(target=thread)
    t.daemon = True
    t.start()
    return

####################

remotes_lock = threading.Lock()
remotes_set = set()
remotes_future = ccerb.Future()

def try_remote_conn(remote_conn, job_key, priority):
    with remotes_lock:
        if remotes_future.is_resolved():
            return # bail without waiting
        remotes_set.add(remote_conn)

    try:
        ccerb.acquire_remote_job(remote_conn, job_key, priority)
    except (socket.timeout, socket.error):
        with remotes_lock:
            remotes_set.remove(remote_conn)
            if not remotes_set:
                remotes_future.reject()

        net_util.kill_socket(remote_conn)
        return

    if remotes_future.accept(remote_conn):
        with remotes_lock:
            for x in remotes_set:
                if x != remote_conn:
                    net_util.kill_socket(x)
    return

####################

if ccerb.VERBOSE:
    log_conn = socket.create_connection(ccerb.CCERBD_LOG_ADDR)
    log_conn.settimeout(None)
    def log_to_ccerbd(msg):
        net_util.send_buffer(log_conn, msg)

    ccerb.log_func = log_to_ccerbd

####################

# sys.argv: [ccerb.py, cl, foo.c]

ccerb.nice_down()

args = sys.argv[1:]
#args = EXAMPLE_CL_ARGS
#print('args:', args)

ccerb.log_time_split(11)

conn = socket.create_connection(ccerb.CCERBD_LOCAL_ADDR)
ccerb.log_time_split(12)

ccerbdd_addr = net_util.recv_pickle(conn)
ccerb.log_time_split(13)

try:
    if not args:
        raise ExShimOut('no args')

    ccerb.v_log(3, '<args: {}>>', args)

    cc_bin = args[0]
    cc_args = args[1:]

    cc_key = ccerb.get_job_key(cc_bin)

    ccerb.log_time_split(21)

    ####

    (preproc_args, compile_args, source_file_name) = process_args(cc_args)
    info = 'ccerb-preproc: {}'.format(source_file_name)

    ccerb.v_log(3, '<<preproc_args: {}>>', preproc_args)
    ccerb.v_log(3, '<<compile_args: {}>>', compile_args)

    has_show_includes = '-showIncludes' in preproc_args

    ####

    ccerb.acquire_remote_job(conn, 'wait', PREPROC_PRIORITY)

    with net_util.WaitBeacon(conn):
        (preproc_data, show_includes) = preproc(cc_bin, preproc_args)

    ########

    if not NO_LOCAL:
        t = threading.Thread(target=try_remote_conn,
                             args=(conn, cc_key, LOCAL_COMPILE_PRIORITY))
        t.daemon = True
        t.start()

    for (host, port) in CONFIG['dedicated_remotes'].viewitems():
        if not port:
            (_, port) = ccerb.CCERBD_LOCAL_ADDR
        add_remote_addr((host, port), cc_key, DEDICATED_COMPILE_PRIORITY)

    ####

    remote_conn = remotes_future.await()
    ccerb.v_log(2, 'compiler addr: {}', remote_conn.getpeername())

    ########

    input_files = [(source_file_name, preproc_data)]
    try:
        returncode = run_remote_job_client(remote_conn, compile_args, input_files)
    except (socket.timeout, socket.error) as e:
        raise ExShimOut('{}({})'.format(type(e), e))

    if has_show_includes:
        try:
            (file_name, rest) = show_includes.split('\n', 1)
            assert file_name == source_file_name
            sys.stdout.write(rest)
            #ccerb.v_log(1, 'show_includes: {}', show_includes)
        except ValueError:
            pass

    net_util.kill_socket(remote_conn)
    exit(returncode)

except ExShimOut as e:
    ccerb.log_time_split(51)
    ccerb.v_log(1, '<shimming out: \'{}\'>', e.reason)
    ccerb.v_log(2, '<<shimming out args: {}>>', args)
    pass

####
ccerb.log_time_split(61)

ccerb.acquire_remote_job(conn, 'wait', SHIM_OUT_PRIORITY)
ccerb.log_time_split(62)

with net_util.WaitBeacon(conn):
    ccerb.log_time_split(63)
    p = subprocess.Popen(args, buf_size=-1)
    ccerb.log_time_split(64)
    p.communicate()
    ccerb.log_time_split(65)

ccerb.log_time_split(66)
net_util.kill_socket(conn)
ccerb.log_time_split(67)
exit(p.returncode)
