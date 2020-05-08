from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

import sys

class ChunkServer():
    def __init__(self, host, port):
        self.master_proxy = ServerProxy('http://localhost:9000')
        res = self.master_proxy.link_with_master(host, port)
        self.host = host
        self.port = port
        print("server linked with master")

    def create(self, filename):
        with open('/Users/jason/temp/'+self.port+'/'+filename, 'w') as f:
            pass

def main():
    host = sys.argv[1]
    port = sys.argv[2]
    server = SimpleXMLRPCServer((host, int(port)), allow_none=True)
    server.register_introspection_functions()
    server.register_instance(ChunkServer(host,port))
    server.serve_forever()

if __name__ == '__main__':
    main()

