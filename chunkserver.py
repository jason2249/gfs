from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

import sys

class ChunkServer():
    def __init__(self, host, port):
        self.master_proxy = ServerProxy('http://localhost:9000')
        res = self.master_proxy.link_with_master(host, port)
        self.root_dir = '/Users/jason/temp/' + port + '/'
        self.url = 'http://' + host + ':' + port
        self.chunk_id_to_filename = {} # int chunk_id -> str filename+chunk_id
        self.chunk_id_to_new_data = {} # int chunk_id -> list[str] new data written
        print("server linked with master")

    def create(self, filename, chunk_id):
        chunk_filename = filename + str(chunk_id)
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
            return next_proxy.send_data(chunk_id, data, idx, replica_urls)
        print('done storing and sending data:', self.chunk_id_to_new_data)
        return 'success'

    def apply_mutations(self, chunk_id, secondary_urls, primary, new_mutations):
        if self.url == primary:
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
            res = proxy.apply_mutations(chunk_id, [], primary, new_mutations)
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

