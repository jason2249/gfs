from xmlrpc.client import ServerProxy

import random

class Client():
    def __init__(self):
        self.chunk_size = 64
        self.master_proxy = ServerProxy('http://localhost:9000')
        self.read_cache = {} # (filename,chunk_idx) -> [chunk_id, [replica urls]]

    def create(self, filename):
        self.master_proxy.create(filename)

    def read(self, filename, byte_offset, amount):
        chunk_idx = byte_offset // self.chunk_size
        chunk_offset = byte_offset % self.chunk_size
        if (filename, chunk_idx) in self.read_cache:
            print("cached read")
            res = self.read_cache[(filename, chunk_idx)]
        else:
            print("asking master for read")
            res = self.master_proxy.read(filename, chunk_idx)
            self.read_cache[(filename,chunk_idx)] = res
        chunk_id = res[0]
        replica_urls = res[1]
        replica_url = random.choice(replica_urls)
        chunkserver_proxy = ServerProxy(replica_url)
        res = chunkserver_proxy.read(chunk_id, chunk_offset, amount)
        return res

def main():
    c = Client()
    c.create("hello.txt")
    res = c.read("hello.txt", 0, 4)
    print(res)

if __name__ == '__main__':
    main()

