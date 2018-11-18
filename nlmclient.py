import rpc
from rpc import TCPClient
from nfsclient import NFSPacker, NFSUnpacker


NLM_VERSION = 4
NLM_PROGRAM = 100021

NLM4_GRANTED = 0
NLM4_DENIED = 1
NLM4_BLOCKED = 3
NLM4_DENIED_GRACE_PERIOD = 4
NLM4_DEADLCK = 5


class NLMPacker(NFSPacker):
    def pack_cookie(self, cookie):
        length, contents = cookie
        self.pack_uint(length)
        self.pack_opaque(contents.encode())

    def pack_lock_attrs(self, lock_attrs):
        caller_name, fh, owner, svid, l_offset, l_len = lock_attrs
        self.pack_string(caller_name.encode())
        self.pack_fhandle(fh)
        self.pack_owner_data(owner)
        self.pack_uint(svid)
        self.pack_uint(l_offset)
        self.pack_uhyper(l_len)

    def pack_owner_data(self, owner):
        self.pack_uint(len(owner))
        self.pack_fopaque(len(owner), owner.encode())

    def pack_lock_call(self, lock_data):
        cookie, block, exclusive, lock, reclaim, state = lock_data
        self.pack_cookie(cookie)
        self.pack_bool(block)
        self.pack_bool(exclusive)
        self.pack_lock_attrs(lock)
        self.pack_bool(reclaim)
        self.pack_uhyper(state)


class NLMUnpacker(NFSUnpacker):
    def __init__(self):
        NFSUnpacker.__init__(self, '')

    def unpack_cookie(self):
        self.unpack_uint()
        self.unpack_string()


    def unpack_lock_reply(self):
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
                              self.unpacker.unpack_lock_reply)

    def lock_wrapper(self, fh, owner, exclusive, block):
        cookie = (4, '')
        caller_name = 'kernel2222'
        owner = owner
        svid = 4
        l_offset = 0
        l_len = 0
        lock = (caller_name, fh, owner, svid, l_offset, l_len)
        reclaim = False
        state = 3
        data = (cookie, block, exclusive, lock, reclaim, state)
        status = self.lock(data)
        print(status)
        if status == NLM4_GRANTED:
            print("NLM_GRANTED")
        elif status == NLM4_BLOCKED:
            print("NLM_BLOCKED")
        elif status == NLM4_DENIED:
            print("NLM4_DENIED")



