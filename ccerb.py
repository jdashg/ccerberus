#!/usr/bin/env python2

from __future__ import print_function

import marshal
import os
import socket
import subprocess
import sys

import ccerbd
import schedaemon
import schedaemon_util


VERBOSE = 2
LOCAL_TIMEOUT = 0.100
SHIM_OUT_PRIORITY = 0
PREPROC_PRIORITY = -1


class ExShimOut(Exception):
    def __init__(self, reason):
        self.reason = reason
        return


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

####

def process_args(args):
    args = args[:]
    if not args:
        raise ExShimOut()

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
            continue

        compile.append(cur)
        continue

    if not is_compile_only:
        raise ExShimOut('not compile-only')

    if not source_file_name:
        raise ExShimOut('no source file')

    return (preproc, compile, source_file_name)

####

def main(args):
    assert len(args) >= 1

    cc_bin = args[0]
    cc_args = args[1:]

    (preproc_args, compile_args, source_file_name) = process_args(cc_args)
    #print('\npreproc_args:', preproc_args)
    #print('\ncompile_args:', compile_args)

    ####

    try:
        info = 'ccerb-preproc: {}'.format(source_file_name)
        with schedaemon.ScheduledJob(PREPROC_PRIORITY, info):
            cc_key = ccerbd.get_cc_key(cc_bin)
            preproc_args = [cc_bin] + preproc_args + [source_file_name]
            p = subprocess.Popen(preproc_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            (outdata, errdata) = p.communicate()
            if p.returncode != 0:
                sys.stderr.write(errdata)
                sys.stdout.write(outdata)
                exit(p.returncode)

            preproc_data = outdata
    except schedaemon.ExTimeout:
        print('ERROR: Missing `schedaemon`.', file=sys.stderr)
        return 1 # Will oversubscribe heavily without schedaemon.

    #print(preproc_data)
    #exit(0)

    ####

    while True:
        try:
            local_conn = socket.create_connection(ccerbd.LOCAL_ADDR, LOCAL_TIMEOUT)
        except socket.timeout:
            print('ERROR: Missing `ccerbd`.', file=sys.stderr)
            return 1 # Exit, since even shimming out is strictly slower than normal.

        try:
            schedaemon_util.send_buffer(local_conn, cc_key)

            local_conn.settimeout(None)
            remote_gai_str = str(schedaemon_util.recv_buffer(local_conn))
            reconn_token = schedaemon_util.recv_buffer(local_conn)
        except  (socket.error, schedaemon_util.ExSocketClosed) as e:
            if VERBOSE >= 1:
                print('Warning: Failed to request remote compile addr. ({})'.format(e),
                      file=sys.stderr)
            raise ExShimOut('no remote compile addr')
        finally:
            schedaemon_util.kill_socket(local_conn)

        ####

        remote_gai = marshal.loads(remote_gai_str)
        (family, socktype, proto, _, sockaddr) = remote_gai

        ####

        try:
            remote_conn = socket.socket(family, socktype, proto)
            remote_conn.connect(sockaddr)
        except (socket.error, socket.timeout) as e:
            print('Failed to connect to remote addr ({}): {}'.format(remote_gai, e))
            continue

        try:
            schedaemon_util.send_buffer(remote_conn, reconn_token)
            returncode = remote_compile_client(remote_conn, source_file_name,
                                               compile_args, preproc_data)
        except (socket.error, socket.timeout, schedaemon_util.ExSocketClosed) as e:
            print('Remote compile failed ({}): {}'.format(remote_gai, e))
            continue
        finally:
            schedaemon_util.kill_socket(remote_conn)

        break

    ####

    return returncode

####

def remote_compile_client(conn, source_file_name, compile_args, source_data):
    conn.settimeout(None)
    schedaemon_util.send_buffer(conn, source_file_name)
    compile_args_str = '\0'.join(compile_args)
    schedaemon_util.send_buffer(conn, compile_args_str)
    schedaemon_util.send_buffer(conn, source_data)

    try:
        returncode = schedaemon_util.recv_struct(conn, '<i')
        outdata = schedaemon_util.recv_buffer(conn)
        errdata = schedaemon_util.recv_buffer(conn)

        sys.stderr.write(errdata)
        sys.stdout.write(outdata)

        num_files = schedaemon_util.recv_struct(conn, '<Q')
        output_files = []
        for i in range(num_files):
            file_name = str(schedaemon_util.recv_buffer(conn))
            file_data = schedaemon_util.recv_buffer(conn)
            output_files.append((file_name, file_data))
            continue

        ccerbd.write_files('', output_files)
        return returncode
    except schedaemon_util.ExSocketClosed:
        print('ERROR: `ccerb` socket closed early.', file=sys.stderr)
        return 1

####

def exit_after_shim(args):
    p = subprocess.Popen(args)
    p.communicate()
    exit(p.returncode)

####

if __name__ == '__main__':
    # sys.argv: [ccerb.py, cl, foo.c]

    args = sys.argv[1:]
    #args = EXAMPLE_CL_ARGS
    #print('args:', args)

    try:
        exit(main(args))
    except ExShimOut as e:
        if VERBOSE >= 1:
            print('<shimming out: \'{}\'>'.format(e.reason), file=sys.stderr)
        if VERBOSE >= 2:
            print('<shimming out args: {}>'.format(args), file=sys.stderr)
        pass

    info = 'ccerb-shim-out: {}'.format(e.reason)
    try:
        with schedaemon.ScheduledJob(SHIM_OUT_PRIORITY, info):
            exit_after_shim(args)
    except schedaemon.ExTimeout:
        print('ERROR: Missing `schedaemon`.', file=sys.stderr)
        exit(1)
    except schedaemon.ExError:
        print('Warning: Unexpected disconnect from schedaemon.', file=sys.stderr)
        pass

    exit_after_shim(args)
