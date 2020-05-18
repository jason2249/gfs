from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

import os
import sys
import threading
import time

class ChunkServer():
    def __init__(self, host, port):
        self.master_proxy = ServerProxy('http://localhost:9000')
        self.root_dir = '/Users/jason/temp/' + port + '/'
        self.url = 'http://' + host + ':' + port
        self.chunk_id_to_filename = {} # int chunk_id -> str filename+chunk_id
        self.chunk_id_to_new_data = {} # int chunk_id -> list[str] new data written
        self.chunk_id_to_timeout = {} # int chunk_id -> float lease timeout

        self.init_from_files()
        chunk_list = self.get_chunks()
        self.master_proxy.link_with_master(host, port, chunk_list)

        self.thread_interval = 30
        background_thread = threading.Thread(target=self.background_thread, args=())
        background_thread.daemon = True
        background_thread.start()
        print('server initialized and linked with master')

    def init_from_files(self):
        #initialize self.chunk_id_to_filename based on files existing on disk
        chunk_files = os.listdir(self.root_dir)
        for filename in chunk_files:
            if filename == '.DS_Store':
                continue
            underscore_idx = filename.rfind('_')
            chunk_id = int(filename[underscore_idx+1:])
            self.chunk_id_to_filename[chunk_id] = filename
        print('initialized from files:', self.chunk_id_to_filename)

    def background_thread(self):
        while True:
            chunk_ids = self.get_chunks()
            while True:
                try:
                    deleted_chunk_ids = self.master_proxy.heartbeat(chunk_ids, self.url)
                    break
                except:
                    print('error connecting to master, retrying in 10 seconds...')
                    time.sleep(10)
            for chunk_id in deleted_chunk_ids:
                filename = self.chunk_id_to_filename[chunk_id]
                os.remove(self.root_dir + filename)
                del self.chunk_id_to_filename[chunk_id]
            print('deleted chunk ids:', deleted_chunk_ids)
            time.sleep(self.thread_interval)

    def replicate_data(self, chunk_id, replica_url):
        replica_proxy = ServerProxy(replica_url)
        res = replica_proxy.get_data_for_chunk(chunk_id)
        filename = res[0]
        data = res[1]
        with open(self.root_dir + filename, 'w') as f:
            f.write(data)
        self.chunk_id_to_filename[chunk_id] = filename
        print('copied chunk', chunk_id, 'from', replica_url, 'to myself')

    def get_data_for_chunk(self, chunk_id):
        filename = self.chunk_id_to_filename[chunk_id]
        res = None
        with open(self.root_dir + filename) as f:
            res = f.read()
        print('sending data for', filename, chunk_id, 'to other replica')
        return (filename, res)

    def get_chunks(self):
        print('sending list of owned chunks to master')
        chunks = list(self.chunk_id_to_filename.keys())
        return chunks

    def create(self, filename, chunk_id):
        chunk_filename = filename + '_' + str(chunk_id)
        #TODO change to open 'x' so it fails if already exists?
        with open(self.root_dir + chunk_filename, 'w') as f:
            pass
        self.chunk_id_to_filename[chunk_id] = chunk_filename
        print('chunk_id_to_filename:', self.chunk_id_to_filename)

    def read(self, chunk_id, chunk_offset, amount):
        filename = self.root_dir + self.chunk_id_to_filename[chunk_id]
        with open(filename) as f:
            f.seek(chunk_offset)
            res = f.read(amount)
            return res
        #TODO return error if file doesn't exist, use try/catch

    def send_data(self, chunk_id, data, idx, replica_urls):
        if chunk_id not in self.chunk_id_to_new_data:
            self.chunk_id_to_new_data[chunk_id] = []
        self.chunk_id_to_new_data[chunk_id].append(data)
        if idx < len(replica_urls):
            next_url = replica_urls[idx]
            next_proxy = ServerProxy(next_url)
            idx += 1
            try:
                res = next_proxy.send_data(chunk_id, data, idx, replica_urls)
            except:
                res = 'chunkserver failure_' + next_url
            # if next chunkserver or some chunkserver down the line failed, delete data
            if res != 'success':
                del self.chunk_id_to_new_data[chunk_id]
            return res
        print('done storing and sending data:', self.chunk_id_to_new_data)
        return 'success'

    def assign_primary(self, chunk_id, lease_timeout):
        print('assigning as primary for chunk id', chunk_id)
        self.chunk_id_to_timeout[chunk_id] = lease_timeout

    def apply_mutations(self, chunk_id, secondary_urls, primary, new_mutations):
        if self.url == primary:
            if chunk_id not in self.chunk_id_to_timeout:
                print('not primary, chunk id not in dict:', chunk_id)
                return 'not primary'
            if time.time() > self.chunk_id_to_timeout[chunk_id]:
                print('not primary, lease timed out:', chunk_id)
                del self.chunk_id_to_timeout[chunk_id]
                return 'not primary'
            print('is primary for chunk id:', chunk_id)
            new_mutations = self.chunk_id_to_new_data[chunk_id][:]
        del self.chunk_id_to_new_data[chunk_id]
        filename = self.chunk_id_to_filename[chunk_id]
        with open(self.root_dir + filename, 'a') as f:
            for data in new_mutations:
                f.write(data)
        print('applied mutations:', new_mutations)
        if self.url != primary:
            return 'success'
        for secondary_url in secondary_urls:
            proxy = ServerProxy(secondary_url)
            try:
                res = proxy.apply_mutations(chunk_id, [], primary, new_mutations)
            except:
                # if chunkserver is down when applying mutations, just skip
                # and let the master re-replication process take care of it
                continue
            if res != 'success':
                return 'failed applying mutation to secondary'
        return 'success'

def main():
    host = sys.argv[1]
    port = sys.argv[2]
    server = SimpleXMLRPCServer((host, int(port)), allow_none=True)
    server.register_introspection_functions()
    server.register_instance(ChunkServer(host,port))
    server.serve_forever()

if __name__ == '__main__':
    main()

