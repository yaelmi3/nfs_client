from mountclient import TCPMountClient
from nfsclient import NFSClient
from nlmclient import NLMClient

HOST = '172.16.41.76'
FILE_SYSTEM = "/export_632983b86c614beba64e52f8df03bb43"
mount_client = TCPMountClient(HOST)
nfs_client = NFSClient(host=HOST)
nlm_client = NLMClient(host=HOST)


def _get_dir_handle(file_system=FILE_SYSTEM):
    sf = mount_client.mount(file_system)
    dir_handle = sf[1]
    if dir_handle:
        return dir_handle


def write_to_file(file_system=FILE_SYSTEM, file_name="another_file.txt", data="kakapipikaka", offset=0):
    file_handle = lookup_file(file_system, file_name)
    if not file_handle:
        file_handle = create_file(file_system, file_name)
    return nfs_client.write_to_file_wrapper(file_handle, data, offset)


def lookup_file(file_system =FILE_SYSTEM, file_name="new_file.txt"):
    dir_handler = _get_dir_handle(file_system=file_system)
    return nfs_client.lookup_wrapper(dir_handler, file_name)


def create_file(file_system=FILE_SYSTEM, file_name="new_file.txt"):
    dir_handle = _get_dir_handle(file_system=file_system)
    return nfs_client.create_file_wrapper(dir_handle, file_name)


def read_dirs():
    dir_handle = _get_dir_handle()
    for item in nfs_client.readdir_wrapper(dir_handle):
        print (item)


def list_dir(host=HOST, filesys=FILE_SYSTEM):
    mount_client = TCPMountClient(host)
    if filesys is None:
        export_list = mount_client.Export()
        for item in export_list:
            print(item)
    else:
        dir_handle = _get_dir_handle()
        dir_list = nfs_client.listdir_wrapper(dir_handle)
        for item in dir_list:
            print(item)


def lock_file(file_system=FILE_SYSTEM, file_name="stam.txt", owner="yael", exclusive=True, block=False):
    file_handle = write_to_file(file_system, file_name)
    nlm_client.lock_wrapper(file_handle, owner=owner, exclusive=exclusive, block=block)




