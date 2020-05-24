from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

import os
import pickle
import sys
import threading
import time

class ChunkServer():
    def __init__(self, host, port):
        self.master_proxy = ServerProxy('http://localhost:9000')
        self.root_dir = '/Users/jason/temp/' + port + '/'
        self.url = 'http://' + host + ':' + port
        self.chunk_id_to_filename = {} # int chunk_id -> str filename
        self.chunk_id_to_version = {} # int chunk_id -> int version
        self.chunk_id_to_new_data = {} # int chunk_id -> list[str] new data written
        self.chunk_id_to_timeout = {} # int chunk_id -> float lease timeout

        self.init_from_files()
        chunk_list = self.get_chunks()
        stale_chunks = self.master_proxy.link_with_master(self.url, chunk_list)
        if len(stale_chunks) > 0:
            print('removing stale chunks')
            self.remove_chunks(stale_chunks)

        self.thread_interval = 30
        background_thread = threading.Thread(target=self.background_thread, args=())
        background_thread.daemon = True
        background_thread.start()
        print('server initialized and linked with master')

    def init_from_files(self):
        #initialize self.chunk_id_to_filename based on files existing on disk
        if os.path.isfile(self.root_dir + 'chunk_versions.pickle'):
            with open(self.root_dir + 'chunk_versions.pickle', 'rb') as f:
                self.chunk_id_to_version = pickle.load(f)
        chunk_files = os.listdir(self.root_dir)
        for filename in chunk_files:
            if filename == '.DS_Store' or filename == 'chunk_versions.pickle':
                continue
            underscore_idx = filename.rfind('_')
            chunk_id = int(filename[underscore_idx+1:])
            self.chunk_id_to_filename[chunk_id] = filename
        print('initialized from files:', self.chunk_id_to_filename)

    def remove_chunks(self, deleted_chunk_ids):
        for chunk_id in deleted_chunk_ids:
            filename = self.chunk_id_to_filename[chunk_id]
            os.remove(self.root_dir + filename)
            del self.chunk_id_to_filename[chunk_id]
            del self.chunk_id_to_version[chunk_id]
        if len(deleted_chunk_ids) > 0:
            print('deleted chunk ids:', deleted_chunk_ids)
            with open(self.root_dir + 'chunk_versions.pickle', 'wb') as f:
                pickle.dump(self.chunk_id_to_version, f)

    def background_thread(self):
        while True:
            time.sleep(self.thread_interval)
            chunk_ids = self.get_chunks()
            while True:
                try:
                    deleted_chunk_ids = self.master_proxy.heartbeat(self.url, chunk_ids)
                    break
                except:
                    print('error connecting to master, retrying in 10 seconds...')
                    time.sleep(10)
            self.remove_chunks(deleted_chunk_ids)

    def replicate_data(self, chunk_id, version, replica_url):
        replica_proxy = ServerProxy(replica_url)
        res = replica_proxy.get_data_for_chunk(chunk_id, version)
        if res == 'stale replica':
            return res
        filename = res[0]
        data = res[1]
        version = res[2]
        with open(self.root_dir + filename, 'w') as f:
            f.write(data)
        self.update_version(chunk_id, version)
        self.chunk_id_to_filename[chunk_id] = filename
        print('copied chunk', chunk_id, 'from', replica_url, 'to myself')
        return 'success'

    def get_data_for_chunk(self, chunk_id, version):
        if version > self.chunk_id_to_version[chunk_id]:
            return 'stale replica'
        filename = self.chunk_id_to_filename[chunk_id]
        res = None
        with open(self.root_dir + filename) as f:
            res = f.read()
        version = self.chunk_id_to_version[chunk_id]
        print('sending data for', filename, chunk_id, 'to other replica')
        return (filename, res, version)

    def get_chunks(self):
        print('sending list of owned chunks to master')
        cids = list(self.chunk_id_to_filename.keys())
        chunks = [(cid, self.chunk_id_to_version[cid]) for cid in cids]
        return chunks

    def update_version(self, chunk_id, new_version):
        if chunk_id in self.chunk_id_to_version:
            cur_version = self.chunk_id_to_version[chunk_id]
            print('updating version of', chunk_id, 'from', cur_version, 'to', new_version)
        else:
            print('creating new version', new_version, 'for chunk', chunk_id)
        self.chunk_id_to_version[chunk_id] = new_version
        with open(self.root_dir + 'chunk_versions.pickle', 'wb') as f:
            pickle.dump(self.chunk_id_to_version, f)

    def create(self, filename, chunk_id):
        chunk_filename = filename + '_' + str(chunk_id)
        #TODO change to open 'x' so it fails if already exists?
        with open(self.root_dir + chunk_filename, 'w') as f:
            pass
        self.chunk_id_to_filename[chunk_id] = chunk_filename
        self.update_version(chunk_id, 0)
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

