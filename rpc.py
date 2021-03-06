import errno
import os
import socket
import xdrlib
import arrow
from enum import Enum
from os import getuid, getgid


# Sun RPC version 2 -- RFC1057.
RPCVERSION = 2


# Port mapper interface
PMAP_PROG = 100000
PMAP_VERS = 2
PMAP_PORT = 111

# A mapping is (prog, vers, prot, port) and prot is one of:
IPPROTO_TCP = 6
IPPROTO_UDP = 17

class MsgType(Enum):
    CALL = 0
    REPLY = 1


class AuthFlavor(Enum):
    AUTH_NULL = 0
    AUTH_UNIX = 1
    AUTH_SHORT = 2
    AUTH_DES = 3


class ReplyStat(Enum):
    MSG_ACCEPTED = 0
    MSG_DENIED = 1


class AcceptStat(Enum):
    SUCCESS = 0                             # RPC executed successfully
    PROG_UNAVAIL = 1                        # remote hasn't exported program
    PROG_MISMATCH = 2                       # remote can't support version #
    PROC_UNAVAIL = 3                        # program can't support procedure
    GARBAGE_ARGS = 4                        # procedure can't decode params


class RejectStat(Enum):
    RPC_MISMATCH = 0                        # RPC version number != 2
    AUTH_ERROR = 1                          # remote can't authenticate caller


class AuthStat(Enum):
    AUTH_BADCRED = 1                        # bad credentials (seal broken)
    AUTH_REJECTEDCRED = 2                   # client must begin new session
    AUTH_BADVERF = 3                        # bad verifier (seal broken)
    AUTH_REJECTEDVERF = 4                   # verifier expired or replayed
    AUTH_TOOWEAK = 5                        # rejected for security reasons


class PmapProg(Enum):
    # Procedure numbers
    PMAPPROC_NULL = 0                       # (void) -> void
    PMAPPROC_SET = 1                        # (mapping) -> bool
    PMAPPROC_UNSET = 2                      # (mapping) -> bool
    PMAPPROC_GETPORT = 3                    # (mapping) -> unsigned int
    PMAPPROC_DUMP = 4                       # (void) -> pmaplist
    PMAPPROC_CALLIT = 5                     # (call_args) -> call_result


# A pmaplist is a variable-length list of mappings, as follows:
# either (1, mapping, pmaplist) or (0).

# A call_args is (prog, vers, proc, args) where args is opaque;
# a call_result is (port, res) where res is opaque.

# Exceptions
class BadRPCFormat(Exception):
    pass


class BadRPCVersion(Exception):
    pass


class GarbageArgs(Exception):
    pass


class Packer(xdrlib.Packer):

    def pack_auth(self, auth):
        flavor, stuff = auth
        self.pack_enum(flavor)
        self.pack_opaque(stuff)

    def pack_auth_unix(self, stamp, machine_name, uid, gid, gids):
        self.pack_uint(stamp)
        self.pack_string(machine_name.encode())
        self.pack_uint(uid)
        self.pack_uint(gid)
        self.pack_uint(len(gids))
        for i in gids:
            self.pack_uint(i)

    def pack_callheader(self, xid, prog, vers, proc, cred, verf):
        self.pack_uint(xid)
        self.pack_enum(MsgType.CALL.value)
        self.pack_uint(RPCVERSION)
        self.pack_uint(prog)
        self.pack_uint(vers)
        self.pack_uint(proc)
        self.pack_auth(cred)
        self.pack_auth(verf)
        # Caller must add procedure-specific part of call

    def pack_replyheader(self, xid, verf):
        self.pack_uint(xid)
        self.pack_enum(MsgType.REPLY.value)
        self.pack_uint(ReplyStat.MSG_ACCEPTED.value)
        self.pack_auth(verf)
        self.pack_enum(AcceptStat.SUCCESS.value)
        # Caller must add procedure-specific part of reply


class Unpacker(xdrlib.Unpacker):

    def unpack_auth(self):
        flavor = self.unpack_enum()
        stuff = self.unpack_opaque()
        return flavor, stuff

    def unpack_callheader(self):
        xid = self.unpack_uint()
        temp = self.unpack_enum()
        if temp != AuthFlavor.CALL.value:
            raise BadRPCFormat(f'No CALL but {temp}'
                               )
        temp = self.unpack_uint()
        if temp != RPCVERSION:
            raise BadRPCVersion(f'Bad RPC version {temp}')

        prog = self.unpack_uint()
        vers = self.unpack_uint()
        proc = self.unpack_uint()
        cred = self.unpack_auth()
        verf = self.unpack_auth()
        return xid, prog, vers, proc, cred, verf
        # Caller must add procedure-specific part of call

    def unpack_replyheader(self):
        xid = self.unpack_uint()
        mtype = self.unpack_enum()
        if mtype != MsgType.REPLY.value:
            raise RuntimeError(f'no REPLY but {mtype}')
        stat = self.unpack_enum()
        if stat == ReplyStat.MSG_DENIED.value:
            stat = self.unpack_enum()
            if stat == RejectStat.RPC_MISMATCH.value:
                low = self.unpack_uint()
                high = self.unpack_uint()
                raise RuntimeError(f'MSG_DENIED: RPC_MISMATCH: {low, high}')
            if stat == RejectStat.AUTH_ERROR:
                stat = self.unpack_uint()
                raise RuntimeError(f'MSG_DENIED: AUTH_ERROR: {stat}')
            raise RuntimeError (f'MSG_DENIED: {stat}')
        if stat != ReplyStat.MSG_ACCEPTED.value:
            raise RuntimeError (f'Neither MSG_DENIED nor MSG_ACCEPTED: {stat}')
        verf = self.unpack_auth()
        stat = self.unpack_enum()
        if stat == AcceptStat.PROG_UNAVAIL.value:
            raise RuntimeError('Call failed: PROG_UNAVAIL')
        if stat == AcceptStat.PROG_MISMATCH.value:
            low = self.unpack_uint()
            high = self.unpack_uint()
            raise RuntimeError(f'call failed: PROG_MISMATCH: {low, high}')
        if stat == AcceptStat.PROC_UNAVAIL.value:
            raise RuntimeError('call failed: PROC_UNAVAIL')
        if stat == AcceptStat.GARBAGE_ARGS.value:
            raise RuntimeError('call failed: GARBAGE_ARGS')
        if stat != AcceptStat.SUCCESS.value:
            raise RuntimeError(f'call failed: {stat}')
        return xid, verf
        # Caller must get procedure-specific part of reply


# Subroutines to create opaque authentication objects

def make_auth_null():
    return b''


def make_auth_unix(seed, host, uid, gid, groups):
    packer = Packer()
    packer.pack_auth_unix(seed, host, uid, gid, groups)
    return packer.get_buf()


def make_auth_unix_default():
    try:
        uid = getuid()
        gid = getgid()
    except ImportError:
        uid = gid = 0
    return make_auth_unix(arrow.now().timestamp, socket.gethostname(), uid, gid, [])


# Common base class for clients

class Client:
    def __init__(self, host, prog, vers, port):
        self.host = host
        self.prog = prog
        self.vers = vers
        self.port = port
        self.sock = None
        self.packer = None
        self.unpacker = None
        self.make_socket() # Assigns to self.sock
        self.bind_socket()
        self.connect_socket()
        self.last_xid = 0
        self.addpackers()
        self.cred = None
        self.verf = None

    def close(self):
        self.sock.close()

    def make_socket(self):
        # This MUST be overridden
        raise RuntimeError('Makesocket not defined')

    def connect_socket(self):
        # Override this if you don't want/need a connection
        self.sock.connect((self.host, self.port))

    def bind_socket(self):
        # Override this to bind to a different port (e.g. reserved)
        self.sock.bind(('', 0))

    def addpackers(self):
        # Override this to use derived classes from Packer/Unpacker
        self.packer = Packer()
        self.unpacker = Unpacker('')

    def make_call(self, proc, args, pack_func, unpack_func):
        if pack_func is None and args is not None:
            raise TypeError('non-null args with null pack_func')
        self.start_call(proc)
        if pack_func:
            pack_func(args)
        self.do_call()
        if unpack_func:
            result = unpack_func()
        else:
            result = None
        self.unpacker.done()
        return result

    def start_call(self, proc):
        self.last_xid = xid = self.last_xid + 1
        cred = self.mkcred()
        verf = self.mkverf()
        packer = self.packer
        packer.reset()
        packer.pack_callheader(xid, self.prog, self.vers, proc, cred, verf)

    def do_call(self):
        raise RuntimeError('do_call not defined')

    def mkcred(self):
        # Override this to use more powerful credentials
        if self.cred is None:
            self.cred = (AuthFlavor.AUTH_NULL.value, make_auth_null())
        return self.cred

    def mkverf(self):
        # Override this to use a more powerful verifier
        if self.verf is None:
            self.verf = (AuthFlavor.AUTH_NULL.value, make_auth_null())
        return self.verf

    def call_0(self):               # Procedure 0 is always like this
        return self.make_call(0, None, None, None)


# Record-Marking standard support

def sendfrag(sock, last, frag):
    x = len(frag)
    if last:
        x = x | 0x80000000
    header = bytes(
        chr(int(x >> 24 & 0xff)) + chr(int(x >> 16 & 0xff)) + chr(int(x >> 8 & 0xff)) + chr(
            int(x & 0xff)), 'raw_unicode_escape')
    sock.send(header + frag)


def sendrecord(sock, record):
    sendfrag(sock, 1, record)


def recvfrag(sock):
    header = sock.recv(4)
    if len(header) < 4:
        raise EOFError
    x = int(header[0]) << 24 | header[1] << 16 | header[2] << 8 | header[3]
    last = ((x & 0x80000000) != 0)
    n = int(x & 0x7fffffff)
    frag = b''
    while n > 0:
        buf = sock.recv(n)
        if not buf:
            raise EOFError
        n = n - len(buf)
        frag = frag + buf
    return last, frag


def recvrecord(sock):
    record = b''
    last = 0
    while not last:
        last, frag = recvfrag(sock)
        record = record + frag
    return record


# Try to bind to a reserved port (must be root)

last_resv_port_tried = None
def bindresvport(sock, host):
    global last_resv_port_tried
    FIRST, LAST = 600, 1024 # Range of ports to try
    if last_resv_port_tried is None:
        last_resv_port_tried = FIRST + os.getpid() % (LAST-FIRST)
    for i in list(range(last_resv_port_tried, LAST)) + list(range(FIRST, last_resv_port_tried)):
        last_resv_port_tried = i
        try:
            sock.bind((host, i))
            return last_resv_port_tried
        except socket.error(errno):
            if errno != 114:
                raise socket.error(errno)
    raise RuntimeError('Cannot assign reserved port')


# Client using TCP to a specific port

class RawTCPClient(Client):
    def make_socket(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def do_call(self):
        call = self.packer.get_buf()
        sendrecord(self.sock, call)
        reply = recvrecord(self.sock)
        u = self.unpacker
        u.reset(reply)
        xid, verf = u.unpack_replyheader()
        if xid != self.last_xid:
            raise RuntimeError (f'wrong xid in reply {xid} instead of {self.last_xid}')


# Client using UDP to a specific port

class RawUDPClient(Client):

    def make_socket(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def do_call(self):
        call = self.packer.get_buf()
        self.sock.send(call)
        try:
            from select import select
        except ImportError:
            print ('WARNING: select not found, RPC may hang')
            select = None
        BUFSIZE = 8192 # Max UDP buffer size
        timeout = 1
        count = 5
        while 1:
            r, w, x = [self.sock], [], []
            if select:
                r, w, x = select(r, w, x, timeout)
            if self.sock not in r:
                count = count - 1
                if count < 0:
                    raise RuntimeError('timeout')
                if timeout < 25: timeout = timeout *2
                self.sock.send(call)
                continue
            reply = self.sock.recv(BUFSIZE)
            u = self.unpacker
            u.reset(reply)
            xid, verf = u.unpack_replyheader()
            if xid != self.last_xid:
                continue
            break


# Client using UDP broadcast to a specific port

class RawBroadcastUDPClient(RawUDPClient):

    def __init__(self, bcastaddr, prog, vers, port):
        RawUDPClient.__init__(self, bcastaddr, prog, vers, port)
        self.reply_handler = None
        self.timeout = 30

    def connect_socket(self):
        # Don't connect -- use sendto
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    def set_reply_handler(self, reply_handler):
        self.reply_handler = reply_handler

    def set_timeout(self, timeout):
        self.timeout = timeout # Use None for infinite timeout

    def make_call(self, proc, args, pack_func, unpack_func):
        if pack_func is None and args is not None:
            raise TypeError('non-null args with null pack_func')
        self.start_call(proc)
        if pack_func:
            pack_func(args)
        call = self.packer.get_buf()
        self.sock.sendto(call, (self.host, self.port))
        try:
            from select import select
        except ImportError:
            print ('WARNING: select not found, broadcast will hang')
            select = None
        BUFSIZE = 8192 # Max UDP buffer size (for reply)
        replies = []
        if unpack_func is None:
            def dummy(): pass
            unpack_func = dummy
        while 1:
            r, w, x = [self.sock], [], []
            if select:
                if self.timeout is None:
                    r, w, x = select(r, w, x)
                else:
                    r, w, x = select(r, w, x, self.timeout)
            if self.sock not in r:
                break
            reply, fromaddr = self.sock.recvfrom(BUFSIZE)
            u = self.unpacker
            u.reset(reply)
            xid, verf = u.unpack_replyheader()
            if xid != self.last_xid:
                continue
            reply = unpack_func()
            self.unpacker.done()
            replies.append((reply, fromaddr))
            if self.reply_handler:
                self.reply_handler(reply, fromaddr)
        return replies


class PortMapperPacker(Packer):

    def pack_mapping(self, mapping):
        prog, vers, prot, port = mapping
        self.pack_uint(prog)
        self.pack_uint(vers)
        self.pack_uint(prot)
        self.pack_uint(port)

    def pack_pmaplist(self, list):
        self.pack_list(list, self.pack_mapping)

    def pack_call_args(self, ca):
        prog, vers, proc, args = ca
        self.pack_uint(prog)
        self.pack_uint(vers)
        self.pack_uint(proc)
        self.pack_opaque(args)


class PortMapperUnpacker(Unpacker):

    def unpack_mapping(self):
        prog = self.unpack_uint()
        vers = self.unpack_uint()
        prot = self.unpack_uint()
        port = self.unpack_uint()
        return prog, vers, prot, port

    def unpack_pmaplist(self):
        return self.unpack_list(self.unpack_mapping)

    def unpack_call_result(self):
        port = self.unpack_uint()
        res = self.unpack_opaque()
        return port, res


class PartialPortMapperClient:

    def addpackers(self):
        self.packer = PortMapperPacker()
        self.unpacker = PortMapperUnpacker('')

    def set_mapping(self, mapping):
        return self.make_call(PmapProg.PMAPPROC_SET.value,
                              mapping,
                              self.packer.pack_mapping,
                              self.unpacker.unpack_uint)

    def unset(self, mapping):
        return self.make_call(PmapProg.PMAPPROC_UNSET.value,
                              mapping,
                              self.packer.pack_mapping,
                              self.unpacker.unpack_uint)

    def get_port(self, mapping):
        return self.make_call(PmapProg.PMAPPROC_GETPORT.value,
                              mapping,
                              self.packer.pack_mapping,
                              self.unpacker.unpack_uint)

    def dump(self):
        return self.make_call(PmapProg.PMAPPROC_DUMP.value,
                              None,
                              None,
                              self.unpacker.unpack_pmaplist)

    def call_it(self, ca):
        return self.make_call(PmapProg.PMAPPROC_CALLIT.value,
                              ca,
                              self.packer.pack_call_args,
                              self.unpacker.unpack_call_result)


class TCPPortMapperClient(PartialPortMapperClient, RawTCPClient):

    def __init__(self, host):
        RawTCPClient.__init__(self, host, PMAP_PROG, PMAP_VERS, PMAP_PORT)


class UDPPortMapperClient(PartialPortMapperClient, RawUDPClient):

    def __init__(self, host):
        RawUDPClient.__init__(self, host, PMAP_PROG, PMAP_VERS, PMAP_PORT)


class BroadcastUDPPortMapperClient(PartialPortMapperClient, RawBroadcastUDPClient):

    def __init__(self, bcastaddr):
        RawBroadcastUDPClient.__init__(self, bcastaddr, PMAP_PROG, PMAP_VERS, PMAP_PORT)


# Generic clients that find their server through the Port mapper

class TCPClient(RawTCPClient):

    def __init__(self, host, prog, vers):
        pmap = TCPPortMapperClient(host)
        port = pmap.get_port((prog, vers, IPPROTO_TCP, 0))
        pmap.close()
        if port == 0:
            raise RuntimeError('program not registered')
        RawTCPClient.__init__(self, host, prog, vers, port)


class UDPClient(RawUDPClient):

    def __init__(self, host, prog, vers):
        pmap = UDPPortMapperClient(host)
        port = pmap.get_port((prog, vers, IPPROTO_UDP, 0))
        pmap.close()
        if port == 0:
            raise RuntimeError('program not registered')
        RawUDPClient.__init__(self, host, prog, vers, port)