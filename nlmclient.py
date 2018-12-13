from enum import Enum

import rpc
from nfsclient import NFSPacker, NFSUnpacker
from rpc import TCPClient

NLM_VERSION = 4
NLM_PROGRAM = 100021


class NLM4_Stats(Enum):
    NLM4_GRANTED = 0
    NLM4_DENIED = 1
    NLM4_DENIED_NOLOCKS = 2
    NLM4_BLOCKED = 3
    NLM4_DENIED_GRACE_PERIOD = 4
    NLM4_DEADLCK = 5
    NLM4_ROFS = 6
    NLM4_STALE_FH = 7
    NLM4_FBIG = 8
    NLM4_FAILED = 9


class NLMPacker(NFSPacker):
    def pack_cookie(self, cookie):
        length, contents = cookie
        self.pack_uint(length)
        self.pack_opaque(contents.encode())

    def pack_lock_attrs(self, lock_attrs):
        self.pack_string(lock_attrs["caller_name"].encode())
        self.pack_fhandle(lock_attrs["fh"])
        self.pack_owner_data(lock_attrs["owner"])
        self.pack_uint(lock_attrs["svid"])
        self.pack_uhyper(lock_attrs["l_offset"])
        self.pack_uhyper(lock_attrs["l_len"])

    def pack_owner_data(self, owner):
        self.pack_uint(len(owner))
        self.pack_fopaque(len(owner), owner.encode())

    def pack_lock_call(self, data):
        self.pack_cookie(data["cookie"])
        self.pack_bool(data["block"])
        self.pack_bool(data["exclusive"])
        self.pack_lock_attrs(data["lock"])
        self.pack_bool(data["reclaim"])
        self.pack_uhyper(data["state"])

    def pack_unlock_call(self, data):
        self.pack_cookie(data["cookie"])
        self.pack_lock_attrs(data["lock"])



class NLMUnpacker(NFSUnpacker):
    def __init__(self):
        NFSUnpacker.__init__(self, '')

    def unpack_cookie(self):
        self.unpack_uint()
        self.unpack_string()

    def unpack_lock_unlock_reply(self):
        self.unpack_cookie()
        return self.unpack_enum()


class NLMClient(TCPClient):
    def __init__(self, host):
        TCPClient.__init__(self, host, NLM_PROGRAM, NLM_VERSION)

    def addpackers(self):
        self.packer = NLMPacker()
        self.unpacker = NLMUnpacker()

    def mkcred(self):
        if self.cred is None:
            self.cred = rpc.AuthFlavor.AUTH_UNIX.value, rpc.make_auth_unix_default()
        return self.cred

    def lock(self, data):
        return self.make_call(2, data,
                              self.packer.pack_lock_call,
                              self.unpacker.unpack_lock_unlock_reply)


    def unlock(self, data):
        return self.make_call(4, data,
                              self.packer.pack_unlock_call,
                              self.unpacker.unpack_lock_unlock_reply)


