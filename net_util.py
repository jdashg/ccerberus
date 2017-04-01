from __future__ import print_function
assert __name__ != '__main__'

import pickle
import socket
import struct
import threading
import time

####

VERBOSE = 0

GAI_POLL_INTERVAL = 3.0

####

print_lock = threading.Lock()

def debug_print(*args):
    if VERBOSE >= 2:
        with print_lock:
            print(*args)

########################################

def spawn_thread(target, args, as_daemon=True):
    t = threading.Thread(target=target, args=args)
    if as_daemon:
        t.daemon = True
    t.start()
    return

####

def sleep_until_keyboard():
    while True:
        try:
            time.sleep(24 * 60 * 60) # float('inf') is 'too long'. :(
        except KeyboardInterrupt:
            return

########################################

class ExSocketClosed(Exception):
    pass

def recv_n(conn, size):
    data = bytearray(size)
    view = memoryview(data)
    pos = 0
    while pos != size:
        subview = view[pos:]
        read = conn.recv_into(subview, len(subview))
        if not read and conn.gettimeout() != 0:
            raise ExSocketClosed()

        pos += read
        continue
    return data

####

def send_struct(conn, format, val):
    data = struct.pack(format, val)
    conn.sendall(data)
    return


def recv_struct(conn, format):
    data_len = struct.calcsize(format)
    data = recv_n(conn, data_len)
    val = struct.unpack(format, data)[0]
    return val

####

def send_byte(conn, val):
    send_struct(conn, '<B', val)

def recv_byte(conn):
    return recv_struct(conn, '<B')

####

def send_buffer(conn, data):
    send_struct(conn, '<Q', len(data))
    conn.sendall(data)
    return

def recv_buffer(conn):
    data_len = recv_struct(conn, '<Q')
    data = recv_n(conn, data_len)
    return data

####

def send_pickle(conn, data):
    pdata = pickle.dumps(data)
    send_buffer(conn, pdata)
    return

def recv_pickle(conn):
    pdata = recv_buffer(conn)
    data = pickle.loads(pdata)
    return data

####

def send_poke(conn):
    conn.sendall(bytearray(1))
    return


def recv_poke(conn):
    got = conn.recv(1)
    return len(got) != 0

####

def kill_socket(s):
    try:
        s.shutdown(socket.SHUT_RDWR)
    except socket.error:
        pass

    try:
        s.close()
    except socket.error:
        pass
    return

########################################

def wait_on_beacon(conn):
    assert conn.gettimeout() != None

    while not recv_n(conn, 1)[0]:
        continue
    return


class WaitBeacon:
    def __init__(self, conn):
        assert conn.gettimeout() != None

        self.conn = conn
        self.lock = threading.Lock()
        self.signaled = False

        t = threading.Thread(name='WaitBeacon', target=WaitBeacon._thread,
                             args=(self,))
        t.daemon = True
        t.start()
        return

    def _thread(self):
        while True:
            with self.lock:
                if self.signaled:
                    return

                self.conn.sendall( bytearray([0]) )
                wait_interval = self.conn.gettimeout() * 0.7

            #if wait_interval >= 1.0:
            #    wait_interval = 1.0
            time.sleep(wait_interval)
            continue

    def signal(self):
        with self.lock:
            self.signaled = True
            self.conn.sendall( bytearray([1]) )
            return

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex_val, ex_traceback):
        self.signal()

########################################

import traceback
def accept_thread(conn, addr, accept_func):
    debug_print('accept_thread', conn, addr)
    try:
        accept_func(conn, addr)
    except (socket.error, socket.timeout) as e:
        print('Uncaught error on accept_thread: {}({})'.format(type(e), e))
        traceback.print_exc()
        pass

    kill_socket(conn)
    return

####

def listen_thread(s, accept_func, gai, gai_set):
    debug_print('listen_thread', accept_func, gai)
    try:
        while True:
            try:
                (conn, addr) = s.accept()
            except socket.timeout:
                continue

            spawn_thread(accept_thread, (conn, addr, accept_func))
            continue
    except socket.error as e:
        print('Failed to bind:', e, file=sys.stderr)
        pass

    gai_set.remove(gai)
    return

####

def serve_forever(addr, accept_func, gai_poll_interval=GAI_POLL_INTERVAL):
    debug_print('serve_forever', addr, accept_func)
    gai_set = set()
    while True:
        (host, port) = addr
        for gai in socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM):
            if gai in gai_set:
                continue

            (family, socktype, proto, _, sockaddr) = gai
            s = socket.socket(family, socktype, proto)
            try:
                s.bind(sockaddr)
                s.listen(5)
            except socket.error:
                kill_socket(s)
                continue

            gai_set.add(gai)

            spawn_thread(listen_thread, (s, accept_func, gai, gai_set))
            continue

        try:
            time.sleep(gai_poll_interval)
        except:
            pass
        continue
