import rpyc
from rpyc.utils.server import ThreadedServer

from mountclient import TCPMountClient
from nfsclient import NFSClient, NfsStat3, CreateMode, UnexpectedNfsStatus
from nlmclient import NLMClient, NLM4_Stats
from packer_arguments import get_packer_arguments


class NFSClientWrapper(rpyc.Service):
    FILE_SYNC = 2


    def _get_export_handle(self, host, export):
        mount_client = TCPMountClient(host)
        sf = mount_client.mount(export)
        export_handle = sf[1]
        if export_handle:
            return export_handle

    def exposed_write_to_file(self, host, export, file_name, write_buffer, offset=0):
        nfs_client = NFSClient(host=host)
        file_handle = self.exposed_lookup_file(host, export, file_name)
        if not file_handle:
            file_handle = self.exposed_create_file(host, export, file_name)
        write_arguments = get_packer_arguments("WRITE",
                                               file=file_handle,
                                               offset=offset,
                                               count=len(write_buffer),
                                               stable=NFSClientWrapper.FILE_SYNC,
                                               data=write_buffer)
        nfs_client.write(write_arguments)
        return file_handle

    def exposed_lookup_file(self, host, export, file_name="new_file.txt"):
        nfs_client = NFSClient(host=host)
        lookup_args = get_packer_arguments("LOOKUP", dir=self._get_export_handle(host, export), name=file_name)
        status, fh = nfs_client.lookup(lookup_args['what'])
        if status == NfsStat3.NFS3_OK:
            print ("file {} was found".format(file_name))
        else:
            print("file {} was not found".format(file_name))
        return fh

    def exposed_create_file(self, host, export, file_name):
        nfs_client = NFSClient(host=host)
        create_args = get_packer_arguments(action_name="CREATE",
                                           dir=self._get_export_handle(host, export),
                                           name=file_name,
                                           create_mode=CreateMode.UNCHECKED.value)
        status, fh = nfs_client.create(create_args)
        if status == NfsStat3.NFS3_OK:
            print("file {} was successfully created".format(file_name))
        return fh

    def read_dirs(self, host, export):
        nfs_client = NFSClient(host=host)
        list_dir = []
        read_dir_arguments = get_packer_arguments("READDIR",
                                                  dir=self._get_export_handle(host, export))
        eof = False
        while not eof:
            status, rest = nfs_client.read_dir(read_dir_arguments)
            if status != NfsStat3.NFS3_OK:
                raise UnexpectedNfsStatus(NfsStat3(status))
            entries, eof = rest
            for fileid, name, cookie in entries:
                list_dir.append((fileid, name))
        return list_dir

    def list_dir(self, host, export):
        nfs_client = NFSClient(host=host)
        list_dir = []
        read_dir_plus_arguments = get_packer_arguments("READDIRPLUS",
                                                       dir=self._get_export_handle(host, export))
        eof = False
        while not eof:
            status, attributes, rest = nfs_client.read_dir_plus(read_dir_plus_arguments)
            if status != NfsStat3.NFS3_OK:
                raise UnexpectedNfsStatus(NfsStat3(status))
            entries, eof = rest
            for file_id, dir_or_file_name, dir_params, cookie, fh in entries:
                list_dir.append((file_id, dir_or_file_name))
        return list_dir

    def exposed_lock(self, host, export, file_name, owner, client_name, **kwargs):
        exclusive = kwargs.get("exclusive", True)
        block = kwargs.get("block", False)
        offset = kwargs.get("offset", 0)
        l_len = kwargs.get("length", 0)
        nlm_client = NLMClient(host)
        file_handle = self._get_file_handle(host, export, file_name)
        lock_arguments = get_packer_arguments("LOCK",
                                              caller_name=client_name,
                                              block=block,
                                              exclusive=exclusive,
                                              fh=file_handle,
                                              owner=owner,
                                              l_offset=offset,
                                              l_len=l_len)
        status = nlm_client.lock(lock_arguments)
        return NLM4_Stats(status)

    def exposed_unlock(self, host, export, file_name, owner, client_name, offset=0, l_len=0):
        file_handle = self._get_file_handle(host, export, file_name)
        nlm_client = NLMClient(host)
        unlock_arguments = get_packer_arguments("UNLOCK",
                                                caller_name=client_name,
                                                owner=owner,
                                                fh=file_handle,
                                                l_offset=offset,
                                                l_len=l_len)
        status = nlm_client.unlock(unlock_arguments)
        return NLM4_Stats(status)

    def _get_file_handle(self, host, export, file_name):
        file_handle = self.exposed_lookup_file(host, export, file_name)
        return file_handle


if __name__ == "__main__":
    t = ThreadedServer(NFSClientWrapper, port=9999)
    t.daemon = True
    t.start()