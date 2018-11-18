from mountclient import TCPMountClient
from nfsclient import NFSClient, NfsStat3, CreateMode, UnexpectedNfsStatus
from nlmclient import NLMClient, NLM4_Stats
from packer_arguments import get_packer_arguments


# HOST = '172.16.41.76'
# FILE_SYSTEM = "/export_632983b86c614beba64e52f8df03bb43"

HOST = '172.16.76.173'
FILE_SYSTEM = "/export/users"


class NFSClientWrapper(object):
    FILE_SYNC = 2

    def __init__(self, host, export):
        self.mount_client = TCPMountClient(host)
        self.nfs_client = NFSClient(host=host)
        self.nlm_client = NLMClient(host=host)
        self.export = export
        self.export_handle = self._get_export_handle()

    def _get_export_handle(self):
        sf = self.mount_client.mount(self.export)
        export_handle = sf[1]
        if export_handle:
            return export_handle

    def write_to_file(self, file_name="another_file2.txt", write_buffer="kakapipikaka", offset=0):
        file_handle = self.lookup_file(file_name)
        if not file_handle:
            file_handle = self.create_file(file_name)
        write_arguments = get_packer_arguments("WRITE",
                                               file=file_handle,
                                               offset=offset,
                                               count=len(write_buffer),
                                               stable=NFSClientWrapper.FILE_SYNC,
                                               data=write_buffer)
        self.nfs_client.write(write_arguments)
        return file_handle

    def lookup_file(self, file_name="new_file.txt"):
        lookup_args = get_packer_arguments("LOOKUP", dir=self.export_handle, name=file_name)
        status, fh = self.nfs_client.lookup(lookup_args['what'])
        if status == NfsStat3.NFS3_OK:
            print ("file {} was found".format(file_name))
        else:
            print("file {} was not found".format(file_name))
        return fh

    def create_file(self, file_name="new_file2.txt"):
        create_args = get_packer_arguments(action_name="CREATE", dir=self.export_handle,
                                           name=file_name, create_mode=CreateMode.UNCHECKED.value)
        status, fh = self.nfs_client.create(create_args)
        if status == NfsStat3.NFS3_OK:
            print("file {} was successfully created".format(file_name))
        return fh

    def read_dirs(self):
        list_dir = []
        read_dir_arguments = get_packer_arguments("READDIR", dir=self.export_handle)
        eof = False
        while not eof:
            status, rest = self.nfs_client.read_dir(read_dir_arguments)
            if status != NfsStat3.NFS3_OK:
                raise UnexpectedNfsStatus(NfsStat3(status))
            entries, eof = rest
            for fileid, name, cookie in entries:
                list_dir.append((fileid, name))
        return list_dir

    def list_dir(self):
        list_dir = []
        read_dir_plus_arguments = get_packer_arguments("READDIRPLUS", dir=self.export_handle)
        eof = False
        while not eof:
            status, attributes, rest = self.nfs_client.read_dir_plus(read_dir_plus_arguments)
            if status != NfsStat3.NFS3_OK:
                raise UnexpectedNfsStatus(NfsStat3(status))
            entries, eof = rest
            for file_id, dir_or_file_name, dir_params, cookie, fh in entries:
                list_dir.append((file_id, dir_or_file_name))
        return list_dir

    def lock(self, file_name="stam.txt", owner="yael", client_name="kernel-panic",
             exclusive=True, block=False, offset=0, l_len=0):
        file_handle = self._get_file_handle(file_name)
        lock_arguments = get_packer_arguments("LOCK",
                                              caller_name=client_name,
                                              block=block,
                                              exclusive=exclusive,
                                              fh=file_handle,
                                              owner=owner,
                                              l_offset=offset,
                                              l_len=l_len)
        status = self.nlm_client.lock(lock_arguments)
        return NLM4_Stats(status)

    def unlock(self, file_name="stam.txt", owner="yael", client_name="kernel-panic", offset=0,
               l_len=0):
        file_handle = self._get_file_handle(file_name)
        unlock_arguments = get_packer_arguments("UNLOCK",
                                                caller_name=client_name,
                                                owner=owner,
                                                fh=file_handle,
                                                l_offset=offset,
                                                l_len=l_len)
        status = self.nlm_client.unlock(unlock_arguments)
        return NLM4_Stats(status)



    def _get_file_handle(self, file_name):
        file_handle = self.lookup_file(file_name)
        if not file_handle:
            return self.create_file(file_name)
        return file_handle

def test_all():
    nfs_client_wrapper = NFSClientWrapper(host=HOST, export=FILE_SYSTEM)
    nfs_client_wrapper.write_to_file()
    nfs_client_wrapper.create_file()
    nfs_client_wrapper.lookup_file()
    nfs_client_wrapper.read_dirs()
    nfs_client_wrapper.list_dir()
    nfs_client_wrapper.lock()
