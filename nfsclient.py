from enum import Enum

import rpc
from mountclient import MountPacker, MountUnpacker
from rpc import TCPClient

NFS_PROGRAM = 100003
NFS_VERSION = 3

class NfsStat3(Enum):
    NFS3_OK = 0
    NFS3ERR_PERM = 1
    NFS3ERR_NOENT = 2
    NFS3ERR_IO = 5
    NFS3ERR_NXIO = 6
    NFS3ERR_ACCES = 13
    NFS3ERR_EXIST = 17
    NFS3ERR_XDEV = 18
    NFS3ERR_NODEV = 19
    NFS3ERR_NOTDIR = 20
    NFS3ERR_ISDIR = 21
    NFS3ERR_INVAL = 22
    NFS3ERR_FBIG = 27
    NFS3ERR_NOSPC = 28
    NFS3ERR_ROFS = 30
    NFS3ERR_MLINK = 31
    NFS3ERR_NAMETOOLONG = 63
    NFS3ERR_NOTEMPTY = 66
    NFS3ERR_DQUOT = 69
    NFS3ERR_STALE = 70
    NFS3ERR_REMOTE = 71
    NFS3ERR_BADHANDLE = 10001


class UnexpectedNfsStatus(Exception):
    pass


class NFSPacker(MountPacker):

    def pack_sattrargs(self, sa):
        file, attributes = sa
        self.pack_fhandle(file)
        self.pack_sattr(attributes)

    def pack_sattr(self, sa):
        mode, uid, gid, size, atime, mtime = sa
        self.pack_uint(1)
        self.pack_uint(mode)
        self.pack_uint(uid)
        self.pack_uint(gid)
        self.pack_uint(1)
        self.pack_uhyper(size)
        if atime == 0:
            self.pack_uint(0)
        else:
            self.pack_uint(1)
            self.pack_timeval(atime)
        if mtime == 0:
            self.pack_uint(0)
        else:
            self.pack_uint(1)
            self.pack_timeval(mtime)

    def pack_diropargs(self, what):
        dir_handle, file_name = what
        self.pack_fhandle(dir_handle)
        self.pack_string(file_name.encode())


    def pack_readdirargs(self, ra):
        """
        struct READDIRPLUS3args {
           nfs_fh3      dir;
           cookie3      cookie;
           cookieverf3  cookieverf;
           count3       dircount;
           count3       maxcount;
      };
        """
        dir, cookie, cookie_version, count = ra
        self.pack_fhandle(dir)
        self.pack_uhyper(cookie)
        self._handle_cookie_ver(cookie_version)
        self.pack_uint(count)

    def _handle_cookie_ver(self, cookie_version):
        if cookie_version == 0:
            self.pack_uhyper(cookie_version)
        else:
            self.pack_fopaque(8, cookie_version)

    def pack_readdirplus(self, ra):
        """
        struct READDIRPLUS3args {
           nfs_fh3      dir;
           cookie3      cookie;
           cookieverf3  cookieverf;
           count3       dircount;
           count3       maxcount;
          };
        """
        _dir, cookie, cookieverf, dircount, maxcount = ra
        ra_readdir = ra[:-1]
        self.pack_readdirargs(ra_readdir)
        self.pack_uint(maxcount)

    def pack_write_args(self, wa):
        fh, offset, count, stable, data = wa
        self.pack_fhandle(fh)
        self.pack_uhyper(offset)
        self.pack_uint(count)
        self.pack_uint(stable)
        self.pack_data(data.encode())

    def pack_create_args(self, ca):
        """
         struct CREATE3args {
           diropargs3   where;
           createhow3   how;

        };
        diropargs3

          struct diropargs3 {
             nfs_fh3     dir;
             filename3   name;
          };

           enum createmode3 {
           UNCHECKED = 0,
           GUARDED   = 1,
           EXCLUSIVE = 2
      };

        """
        dir_handle, file_name, create_mode, mode, uid, gid, size, atime, mtime = ca
        self.pack_diropargs((dir_handle, file_name))
        self.pack_enum(create_mode)
        self.pack_sattr((mode, uid, gid, size, atime, mtime))


    def pack_commitargs(self, ca):
        fh, offset, count = ca
        self.pack_fhandle(fh)
        self.pack_uhyper(offset)
        self.pack_uint(count)

    def pack_fs_info_args(self, fh):
        self.pack_fhandle(fh)

    def pack_data(self, data):
        data_length = len(data)
        self.pack_uint(data_length)
        self.pack_fopaque(data_length, data)

    def pack_timeval(self, tv):
        secs, usecs = tv
        self.pack_uint(secs)
        self.pack_uint(usecs)


class NFSUnpacker(MountUnpacker):

    def unpack_readdirres(self):
        status = verify_nfs_status(self.unpack_enum(), [NfsStat3.NFS3_OK])
        self.unpack_obj_attributes()
        _ = self.unpack_uhyper()
        entries = self.unpack_list(self.unpack_entry)
        eof = self.unpack_bool()
        rest = (entries, eof)
        return status, rest

    def unpack_readdirplus(self):
        status = verify_nfs_status(self.unpack_enum(), [NfsStat3.NFS3_OK])
        attr = self.unpack_obj_attributes()
        self.unpack_uhyper() # Verifier
        entries = self.unpack_list(self.unpack_entry_plus)
        eof = self.unpack_bool()
        rest = (entries, eof)
        return status, attr, rest

    def unpack_obj_attributes(self):
        attributes_follow = self.unpack_bool()
        if attributes_follow == 1:
            fattr3 = self.unpack_fattr3()
            return fattr3

    def unpack_dir_or_file_wcc(self):
        before_follows = self.unpack_bool()
        if before_follows:
            self.unpack_wcc_attr()
        after_follows = self.unpack_bool()
        if after_follows:
            self.unpack_fattr3()

    def unpack_wcc_attributes(self):
        op_attr = self.unpack_uint()
        if op_attr == 1:
            wcc_attrs = self.unpack_wcc_attr()
            return wcc_attrs

    def unpack_fh_attributes(self):
        fh = self.unpack_fhandle()
        return fh

    def unpack_fsinfo_res(self):
        """
        struct FSINFO3resok {
           post_op_attr obj_attributes;
           uint32       rtmax;
           uint32       rtpref;
           uint32       rtmult;
           uint32       wtmax;
           uint32       wtpref;
           uint32       wtmult;
           uint32       dtpref;
           size3        maxfilesize;
           nfstime3     time_delta;
           uint32       properties;
        :return:
        """
        verify_nfs_status(self.unpack_enum(), [NfsStat3.NFS3_OK])
        attr = self.unpack_obj_attributes()
        rtmax = self.unpack_uint()
        rtpref = self.unpack_uint()
        rtmult= self.unpack_uint()
        wtmax = self.unpack_uint()
        wtpref = self.unpack_uint()
        wtmult = self.unpack_uint()
        dtpref = self.unpack_uint()
        time_delta = self.unpack_uhyper()
        properties = self.unpack_uint()
        max_file_size = self.unpack_uhyper()
        return attr, rtmax, rtpref, rtmult, wtmax, wtpref, wtmult, dtpref, time_delta, \
               properties, max_file_size

    def unpack_entry(self):
        file_id = self.unpack_uhyper()
        name = self.unpack_string()
        cookie = self.unpack_uhyper()
        return file_id, name, cookie

    def unpack_entry_plus(self):
        fh = None
        fileid, name, cookie = self.unpack_entry()
        entry_attr = self.unpack_obj_attributes()
        handle_follow = self.unpack_bool()
        if handle_follow:
            fh = self.unpack_fh_attributes()
        return fileid, name, cookie, entry_attr, fh

    def unpack_write_res(self):
        status = verify_nfs_status(self.unpack_enum(), [NfsStat3.NFS3_OK])
        self.unpack_dir_or_file_wcc()
        # count
        self.unpack_uint()
        # committed
        self.unpack_uint()
        # committed:
        self.unpack_uhyper()
        return status

    def unpack_create_res(self):
        fh = None
        status = verify_nfs_status(self.unpack_enum(), [NfsStat3.NFS3_OK])
        handle_follow = self.unpack_bool()
        if handle_follow:
            fh = self.unpack_fh_attributes()
        self.unpack_obj_attributes()
        self.unpack_dir_or_file_wcc()
        return status, fh


    def unpack_dirop_res(self):
        file_handle = None
        status = verify_nfs_status(self.unpack_enum(), [NfsStat3.NFS3_OK, NfsStat3.NFS3ERR_NOENT])
        if status == NfsStat3.NFS3_OK:
            file_handle = self.unpack_fhandle()
            self.unpack_obj_attributes()
            self.unpack_obj_attributes()
        elif status == NfsStat3.NFS3ERR_NOENT:
            self.unpack_obj_attributes()
        return status, file_handle

    def unpack_attribute_status(self):
        status = verify_nfs_status(self.unpack_enum(), [NfsStat3.NFS3_OK])
        attributes = self.unpack_fattr3()
        return status, attributes

    def unpack_fattr3(self):
        """
        struct fattr3 {
         ftype3     type;
         mode3      mode;
         uint32     nlink;
         uid3       uid;
         gid3       gid;
         size3      size;
         size3      used;
         specdata3  rdev;
         uint64     fsid;
         fileid3    fileid;
         nfstime3   atime;
         nfstime3   mtime;
         nfstime3   ctime;
      };
        """
        _type = self.unpack_enum()
        mode = self.unpack_uint()
        nlink = self.unpack_uint()
        uid = self.unpack_uint()
        gid = self.unpack_uint()
        size = self.unpack_uhyper()
        used = self.unpack_uhyper()
        rdev = self.unpack_uhyper()
        fsid = self.unpack_uhyper()
        fileid = self.unpack_uhyper()
        atime = self.unpack_timeval()
        mtime = self.unpack_timeval()
        ctime = self.unpack_timeval()
        return _type, mode, nlink, uid, gid, size, used, rdev, fsid, fileid, atime, mtime, ctime

    def unpack_wcc_attr(self):
        """
        struct wcc_attr {
         size3       size;
         nfstime3    mtime;
         nfstime3    ctime;
        };
        """
        size = self.unpack_uhyper()
        mtime = self.unpack_timeval()
        ctime = self.unpack_timeval()
        return size, mtime, ctime

    def unpack_timeval(self):
        secs = self.unpack_uint()
        usecs = self.unpack_uint()
        return (secs, usecs)


class NFSClient(TCPClient):

    def __init__(self, host):
        TCPClient.__init__(self, host, NFS_PROGRAM, NFS_VERSION)

    def addpackers(self):
        self.packer = NFSPacker()
        self.unpacker = NFSUnpacker('')

    def mkcred(self):
        if self.cred is None:
            self.cred = rpc.AuthFlavor.AUTH_UNIX.value, rpc.make_auth_unix_default()
        return self.cred

    def getattr(self, fh):
        return self.make_call(1, fh,
                              self.packer.pack_fhandle,
                              self.unpacker.unpack_attribute_status)

    def setattr(self, sa):
        return self.make_call(2, sa,
                              self.packer.pack_sattrargs,
                              self.unpacker.unpack_attribute_status)

    def lookup(self, da):
        return self.make_call(3, da,
                              self.packer.pack_diropargs,
                              self.unpacker.unpack_dirop_res)

    def read_dir(self, ra):
        return self.make_call(16, ra,
                self.packer.pack_readdirargs,
                self.unpacker.unpack_readdirres)

    def read_dir_plus(self, ra):
        return self.make_call(17, ra,
                              self.packer.pack_readdirplus,
                              self.unpacker.unpack_readdirplus)

    def write(self, wa):
        return self.make_call(7, wa,
                              self.packer.pack_write_args,
                              self.unpacker.unpack_write_res)

    def create(self, ca):
        return self.make_call(8, ca,
                              self.packer.pack_create_args,
                              self.unpacker.unpack_create_res)

    def fsinfo(self, fh):
        return self.make_call(19, fh,
                              self.packer.pack_fs_info_args,
                              self.unpacker.unpack_fsinfo_res)


    def listdir_wrapper(self, dir_handle):
        list_dir = []
        ra = (dir_handle, 0, 0, 2000, 2000)
        while 1:
            status, dir_params, rest = self.read_dir_plus(ra)
            if status != NfsStat3.NFS3_OK:
                print ("Server returned {}".format(status))
                break
            entries, eof = rest
            last_cookie = None
            for file_id, dir_or_file_name, dir_params, cookie, fh in entries:
                list_dir.append((file_id, dir_or_file_name))
                last_cookie = cookie
            if eof or last_cookie is None:
                break
            ra = (ra[0], last_cookie, ra[1], ra[2])
        return list_dir

    def readdir_wrapper(self, dir_handle):
        list_dir = []
        ra = (dir_handle, 0, 0, 2000)
        while 1:
            status, rest = self.read_dir(ra)
            if status != NfsStat3.NFS3_OK:
                print ("Server returned {}".format(status))
                break
            entries, eof = rest
            last_cookie = None
            for fileid, name, cookie in entries:
                list_dir.append((fileid, name))
                last_cookie = cookie
            if eof or last_cookie is None:
                break
            ra = (ra[0], last_cookie, ra[2])
        return list_dir

    def lookup_wrapper(self, dir_handle, file_name):
        what = dir_handle, file_name
        status, fh = self.lookup(what)
        if status == NfsStat3.NFS3_OK:
            print ("file {} was found".format(file_name))
            return fh
        else:
            print("file {} was not found".format(file_name))

    def create_file_wrapper(self, dir_handle, file_name):
        UNCHECKED = 0
        create_args = dir_handle, file_name, UNCHECKED, 0, 0, 0, 0, 0, 0
        status, fh = self.create(create_args)
        if status == NfsStat3.NFS3_OK:
            print("file {} was successfully created".format(file_name))
        return fh


def verify_nfs_status(status, allowed_statuses):
    """
    Check if the current matches the given allowed status. Raise an exception otherwise
    :type status: int
    :type allowed_statuses: list
    """
    if NfsStat3(status) not in allowed_statuses:
        raise UnexpectedNfsStatus(f"{NfsStat3(status).name} ({NfsStat3(status).value})")
    return NfsStat3(status)
