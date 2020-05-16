from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

import random
import threading
import time

class FileInfo():
    def __init__(self, deleted=False, time=None, chunk_list=[]):
        self.deleted = deleted
        self.deleted_time = time # float deletion time
        self.chunk_list = chunk_list # list[int] chunkIds

class Master():
    def __init__(self):
        self.num_replicas = 3
        self.lease_duration_secs = 30
        self.deleted_file_duration_secs = 30
        self.chunkserver_url_to_proxy = {}
        self.filename_to_chunks = {} # string filename -> FileInfo
        self.chunk_to_filename = {} # int chunkId -> string filename
        self.chunk_to_urls = {} # int chunkId -> list[string] urls of replicas
        self.chunk_to_primary = {} # int chunkId -> string url of primary
        self.chunk_id_counter = 1000 # start at 1000 to differentiate from chunk indexes
        self.thread_interval = 30
        background_thread = threading.Thread(target=self.background_thread, \
                args=[self.thread_interval])
        background_thread.daemon = True
        background_thread.start()
        print("master initialized")

    def background_thread(self, interval):
        while True:
            self.garbage_collect()
            #TODO check what replicas have sent heartbeats recently
            # if any have stopped for a certain period, re-replicate their chunks
            time.sleep(interval)

    def heartbeat(self, chunk_ids):
        deleted_chunk_ids = []
        for chunk_id in chunk_ids:
            if chunk_id not in self.chunk_to_filename:
                deleted_chunk_ids.append(chunk_id)
        return deleted_chunk_ids

    def garbage_collect(self):
        #iterate through file namespace and remove old deleted files
        filenames_to_delete = []
        for filename in self.filename_to_chunks:
            f = self.filename_to_chunks[filename]
            if f.deleted:
                if time.time() > f.deleted_time + self.deleted_file_duration_secs:
                    filenames_to_delete.append(filename)
        for filename in filenames_to_delete:
            del self.filename_to_chunks[filename]

        #iterate through chunk namespace and remove orphaned chunks
        chunks_to_delete = []
        for chunkId in self.chunk_to_filename:
            f = self.chunk_to_filename[chunkId]
            deleted_f = 'DELETED_' + f
            if filename not in self.filename_to_chunks and \
                    deleted_f not in self.filename_to_chunks:
                chunks_to_delete.append(chunkId)
        for chunkId in chunks_to_delete:
            del self.chunk_to_filename[chunkId]
            del self.chunk_to_urls[chunkId]
            del self.chunk_to_primary[chunkId]
        print('after garbage collection:', self.filename_to_chunks, self.chunk_to_filename)

    def link_with_master(self, host, port):
        url = 'http://' + host + ':' + port
        self.chunkserver_url_to_proxy[url] = ServerProxy(url)
        print('chunkserver_url_to_proxy:', self.chunkserver_url_to_proxy)

    def create(self, filename):
        #randomly sample self.num_replicas servers to host replicas on
        all_urls = self.chunkserver_url_to_proxy.keys()
        proxy_urls = random.sample(all_urls, self.num_replicas)
        #assign next chunkId to each chunk sequentially
        chunk_id = self.chunk_id_counter
        replica_urls = []
        for url in proxy_urls:
            proxy = self.chunkserver_url_to_proxy[url]
            proxy.create(filename, chunk_id)
            replica_urls.append(url)
        #store chunkId->list[server] mapping in chunk_to_url
        self.chunk_to_urls[chunk_id] = replica_urls
        self.chunk_to_filename[chunk_id] = filename
        self.chunk_id_counter += 1
        #append this chunkId to list of chunkIds in filename_to_chunks
        if filename not in self.filename_to_chunks:
            self.filename_to_chunks[filename] = FileInfo(chunk_list=[])
        self.filename_to_chunks[filename].chunk_list.append(chunk_id)
        print('after CREATE filename_to_chunks:', self.filename_to_chunks)
        print('chunk_to_urls:', self.chunk_to_urls)
        return 'successfully created ' + filename

    def delete(self, filename):
        if filename not in self.filename_to_chunks:
            return 'file not found'
        chunk_list = self.filename_to_chunks[filename].chunk_list[:]
        del self.filename_to_chunks[filename]
        t = time.time()
        deleted_filename = 'DELETED_' + filename
        self.filename_to_chunks[deleted_filename] = FileInfo(True, t, chunk_list)
        print('after DELETE filename_to_chunks:', self.filename_to_chunks)
        return 'successfully deleted ' + filename

    def read(self, filename, chunk_idx):
        if filename not in self.filename_to_chunks:
            return 'file not found'
        chunk_id = self.filename_to_chunks[filename].chunk_list[chunk_idx]
        replica_urls = self.chunk_to_urls[chunk_id]
        print('READ returning:', chunk_id, replica_urls)
        return (chunk_id, replica_urls)

    def write(self, filename, chunk_idx):
        if filename not in self.filename_to_chunks:
            return 'file not found'
        chunk_id = self.filename_to_chunks[filename].chunk_list[chunk_idx]
        replica_urls = self.chunk_to_urls[chunk_id]

        pick_new_primary = True
        if chunk_id in self.chunk_to_primary:
            res = self.chunk_to_primary[chunk_id]
            original_cache_timeout = res[1]
            if time.time() <= original_cache_timeout:
                pick_new_primary = False
                # leases are refreshed through heartbeat messages, not write requests
        if pick_new_primary:
            print('picking new primary for chunk id', chunk_id)
            primary_url = random.choice(replica_urls)
            primary_proxy = self.chunkserver_url_to_proxy[primary_url]
            timeout = time.time() + self.lease_duration_secs
            primary_proxy.assign_primary(chunk_id, timeout)
            self.chunk_to_primary[chunk_id] = [primary_url, timeout]
        else:
            print('reused old primary for chunk id', chunk_id)

        return (chunk_id, self.chunk_to_primary[chunk_id][0], replica_urls)

def main():
    master_server = SimpleXMLRPCServer(('localhost', 9000), allow_none=True)
    master_server.register_introspection_functions()
    master_server.register_instance(Master())
    master_server.serve_forever()

if __name__ == '__main__':
    main()

