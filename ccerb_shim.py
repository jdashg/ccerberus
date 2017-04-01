#!/usr/bin/env python2
assert __name__ == '__main__'

from __future__ import print_function

import os
import socket
import subprocess
import sys
import threading

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

####################

class ExShimOut(Exception):
    def __init__(self, reason):
        self.reason = reason
        return

####

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

        if cur == '-c':
            is_compile_only = True
            continue

        if cur.startswith('-D') or cur.startswith('-I'):
            preproc.append(cur)
            continue

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

        if cur.endswith('.c') or cur.endswith('.cc') or cur.endswith('.cpp'):
            if source_file_name:
                raise ExShimOut('multiple source files')

            if os.path.dirname(cur[2:]):
                raise ExShimOut('source file is a path')

            source_file_name = cur
            pass

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
    sys.stdout.write(outdata)

    output_files = ccerb.recv_files(conn)

    ccerb.write_files('', output_files)
    return returncode

####

def preproc(cc_bin, preproc_args, source_file_name):
    preproc_args = [cc_bin] + preproc_args + [source_file_name]
    p = subprocess.Popen(preproc_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    (outdata, errdata) = p.communicate()
    if p.returncode != 0:
        sys.stderr.write(errdata)
        sys.stdout.write(outdata)
        exit(p.returncode)

    return outdata

####

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

####################

def ccerbd_connect(addr):
    remote_conn = socket.create_connection(addr, ccerb.NET_TIMEOUT)
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

# sys.argv: [ccerb.py, cl, foo.c]

ccerb.nice_down()

args = sys.argv[1:]
#args = EXAMPLE_CL_ARGS
print('args:', args)

conn = socket.create_connection(ccerb.CCERBD_LOCAL_ADDR, 0.100) # Fail local connect fast.
conn.settimeout(ccerb.NET_TIMEOUT)
ccerbdd_addr = net_util.recv_pickle(conn)

try:
    cc_bin = args[0]
    cc_args = args[1:]

    cc_key = ccerb.get_job_key(cc_bin)

    ####

    (preproc_args, compile_args, source_file_name) = process_args(cc_args)
    info = 'ccerb-preproc: {}'.format(source_file_name)
    #print('\npreproc_args:', preproc_args)
    #print('\ncompile_args:', compile_args)

    ####

    ccerb.acquire_remote_job(conn, 'wait', PREPROC_PRIORITY)

    with net_util.WaitBeacon(conn):
        preproc_data = preproc(cc_bin, preproc_args, source_file_name)

    ########

    t = threading.Thread(target=try_remote_conn,
                         args=(conn, cc_key, LOCAL_COMPILE_PRIORITY))
    t.daemon = True
    t.start()

    for (addr, _) in CONFIG['dedicated_remotes']:
        add_remote_addr(addr, cc_key, DEDICATED_COMPILE_PRIORITY)

    ####

    remote_conn = remotes_future.await()

    ########

    input_files = [(source_file_name, preproc_data)]
    returncode = run_remote_job_client(remote_conn, compile_args, input_files)
    exit(returncode)

except ExShimOut as e:
    if ccerb.VERBOSE >= 1:
        print('<shimming out: \'{}\'>'.format(e.reason), file=sys.stderr)
    if ccerb.VERBOSE >= 2:
        print('<shimming out args: {}>'.format(args), file=sys.stderr)
    pass

####

ccerb.acquire_remote_job(conn, 'wait', SHIM_OUT_PRIORITY)

with net_util.WaitBeacon(conn):
    p = subprocess.Popen(args)
    p.communicate()
    exit(p.returncode)
