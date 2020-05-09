from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

import sys

class ChunkServer():
    def __init__(self, host, port):
        self.master_proxy = ServerProxy('http://localhost:9000')
        res = self.master_proxy.link_with_master(host, port)
        self.root_dir = '/Users/jason/temp/' + port + '/'
        self.host = host
        self.port = port
        self.chunk_id_to_filename = {}
        print("server linked with master")

    def create(self, filename, chunk_id):
        chunk_filename = filename + str(chunk_id)
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
        #TODO return error if file doesn't exist

def main():
    host = sys.argv[1]
    port = sys.argv[2]
    server = SimpleXMLRPCServer((host, int(port)), allow_none=True)
    server.register_introspection_functions()
    server.register_instance(ChunkServer(host,port))
    server.serve_forever()

if __name__ == '__main__':
    main()

