from mountclient import TCPMountClient
from nfsclient import NFSClient
from nlmclient import NLMClient

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

    def write_to_file(self, file_name="another_file.txt", write_buffer="kakapipikaka", offset=0):
        file_handle = self.lookup_file(file_name)
        if not file_handle:
            file_handle = self.create_file(file_name)
            self.nfs_client.write((file_handle,
                                   offset,
                                   len(write_buffer),
                                   NFSClientWrapper.FILE_SYNC,
                                   write_buffer))
        return file_handle

    def lookup_file(self, file_name="new_file.txt"):
        return self.nfs_client.lookup_wrapper(self.export_handle, file_name)

    def create_file(self, file_name="new_file.txt"):
        return self.nfs_client.create_file_wrapper(self.export_handle, file_name)

    def read_dirs(self):
        for item in self.nfs_client.readdir_wrapper(self.export_handle):
            print(item)

    def list_dir(self):
        dir_list = self.nfs_client.listdir_wrapper(self.export_handle)
        for item in dir_list:
            print(item)

    def lock_file(self, file_name="stam.txt", owner="yael", exclusive=True, block=False):
        file_handle = self.write_to_file(file_name)
        self.nlm_client.lock_wrapper(file_handle, owner=owner, exclusive=exclusive, block=block)


def test_all():
    nfs_client_wrapper = NFSClientWrapper(host=HOST, export=FILE_SYSTEM)
    nfs_client_wrapper.write_to_file()
    nfs_client_wrapper.create_file()
    nfs_client_wrapper.lookup_file()
    nfs_client_wrapper.read_dirs()
    nfs_client_wrapper.list_dir()
    nfs_client_wrapper.lock_file()
