from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

import random
import time

class Master():
    def __init__(self):
        self.num_replicas = 3
        self.lease_timeout_secs = 60 #TODO timeout of lease
        self.deleted_file_duration_secs = 60 #TODO duration to keep a deleted file
        self.chunkserver_urls = []
        self.chunkserver_proxies = []
        self.filename_to_chunks = {} # string filename -> list[int] chunkIds
        self.chunk_to_filename = {} # int chunkId -> string filename
        self.chunk_to_urls = {} # int chunkId -> list[string] urls of replicas
        self.chunk_to_primary = {} # int chunkId -> string url of primary
        self.chunk_id_counter = 1000 # start at 1000 to differentiate from chunk indexes
        print("master initialized")

    def link_with_master(self, host, port):
        url = 'http://' + host + ':' + port
        self.chunkserver_urls.append(url)
        self.chunkserver_proxies.append(ServerProxy(url))
        print('chunkserver_proxies:', self.chunkserver_proxies)

    def create(self, filename):
        #randomly sample self.num_replicas servers to host replicas on
        proxy_idxs = random.sample(range(len(self.chunkserver_urls)), self.num_replicas)
        #assign next chunkId to each chunk sequentially
        chunk_id = self.chunk_id_counter
        replica_urls = []
        for i in proxy_idxs:
            proxy = self.chunkserver_proxies[i]
            proxy.create(filename, chunk_id)
            replica_urls.append(self.chunkserver_urls[i])
        #store chunkId->list[server] mapping in chunk_to_url
        self.chunk_to_urls[chunk_id] = replica_urls
        self.chunk_to_filename[chunk_id] = filename
        self.chunk_id_counter += 1
        #append this chunkId to list of chunkIds in filename_to_chunks
        if filename not in self.filename_to_chunks:
            self.filename_to_chunks[filename] = []
        self.filename_to_chunks[filename].append(chunk_id)
        print('filename_to_chunks:', self.filename_to_chunks)
        print('chunk_to_urls:', self.chunk_to_urls)
        return 'successfully created ' + filename

    def delete(self, filename):
        if filename not in self.filename_to_chunks:
            return 'file not found'
        chunk_list = self.filename_to_chunks[filename][:]
        del self.filename_to_chunks[filename]
        curr_time = str(time.time())
        deleted_filename = 'DELETED_' + filename + curr_time
        self.filename_to_chunks[deleted_filename] = chunk_list
        return 'successfully deleted ' + filename

    def read(self, filename, chunk_idx):
        if filename not in self.filename_to_chunks:
            return 'file not found'
        chunk_id = self.filename_to_chunks[filename][chunk_idx]
        replica_urls = self.chunk_to_urls[chunk_id]
        return (chunk_id, replica_urls)

    def write(self, filename, chunk_idx):
        if filename not in self.filename_to_chunks:
            return 'file not found'
        chunk_id = self.filename_to_chunks[filename][chunk_idx]
        replica_urls = self.chunk_to_urls[chunk_id]
        if chunk_id not in self.chunk_to_primary:
            self.chunk_to_primary[chunk_id] = random.choice(replica_urls)
        return (chunk_id, self.chunk_to_primary[chunk_id], replica_urls)

def main():
    master_server = SimpleXMLRPCServer(('localhost', 9000), allow_none=True)
    master_server.register_introspection_functions()
    master_server.register_instance(Master())
    master_server.serve_forever()

if __name__ == '__main__':
    main()

