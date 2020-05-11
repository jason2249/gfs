from xmlrpc.client import ServerProxy

import random
import sys
import time

class Client():
    def __init__(self):
        self.chunk_size = 64
        self.master_proxy = ServerProxy('http://localhost:9000')
        # read_cache: (filename,chunk_idx) -> [chunk_id, [replica urls]]
        self.read_cache = {}
        # primary_cache: (filename,chunk_idx) -> [chunk_id, primary, [replica urls]]
        self.primary_cache = {}

    def create(self, filename):
        self.master_proxy.create(filename)

    def read(self, filename, byte_offset, amount):
        chunk_idx = byte_offset // self.chunk_size
        chunk_offset = byte_offset % self.chunk_size

        total_to_read = amount
        amount_to_read_in_chunk = min(self.chunk_size - chunk_offset, total_to_read)
        s = ''
        while total_to_read > 0:
            # locate replica urls of chunk
            if (filename, chunk_idx) in self.read_cache:
                res = self.read_cache[(filename, chunk_idx)]
            else:
                res = self.master_proxy.read(filename, chunk_idx)
                self.read_cache[(filename,chunk_idx)] = res
            chunk_id = res[0]
            replica_urls = res[1]

            # pick random replica to read from
            replica_url = random.choice(replica_urls)
            chunkserver_proxy = ServerProxy(replica_url)
            s += chunkserver_proxy.read(chunk_id, chunk_offset, amount_to_read_in_chunk)

            total_to_read -= amount_to_read_in_chunk
            amount_to_read_in_chunk = min(self.chunk_size, total_to_read)
            chunk_idx += 1
            chunk_offset = 0
        return s

    def write(self, filename, data, byte_offset):
        chunk_idx = byte_offset // self.chunk_size
        chunk_offset = byte_offset % self.chunk_size

        data_idx = 0
        total_to_write = len(data)
        amount_to_write_in_chunk = min(self.chunk_size - chunk_offset, total_to_write)

        first = True
        backoff_secs = 1
        while total_to_write > 0:
            if first:
                first = False
            else:
                chunk_idx += 1
                self.create(filename)
            data_piece = data[data_idx:data_idx+amount_to_write_in_chunk]
            res = self.write_helper(filename, data_piece, chunk_idx)
            if res != 'success':
                # error, wait for exponential backoff and retry
                print(res, 'for ' + filename + ', backing off and retrying in ' \
                        + str(backoff_secs) + ' seconds')
                time.sleep(backoff_secs)
                backoff_secs *= 2
            else:
                data_idx += amount_to_write_in_chunk
                total_to_write -= amount_to_write_in_chunk
                amount_to_write_in_chunk = min(self.chunk_size, total_to_write)
        print('Wrote ' + str(len(data)) + ' bytes to ' + filename)


    def write_helper(self, filename, data, chunk_idx):
        # locate primary and replica urls
        if (filename, chunk_idx) in self.primary_cache:
            res = self.primary_cache[(filename,chunk_idx)]
        else:
            res = self.master_proxy.write(filename, chunk_idx)
            self.primary_cache[(filename,chunk_idx)] = res
        chunk_id = res[0]
        primary = res[1]
        replica_urls = res[2]

        # send data to first replica, which sends data to all other replicas
        replica_url = replica_urls[0]
        chunkserver_proxy = ServerProxy(replica_url)
        res = chunkserver_proxy.send_data(chunk_id, data, 1, replica_urls)
        if res != 'success':
            return 'failure sending data to chunkservers'

        # apply mutations on primary, then secondaries
        primary_proxy = ServerProxy(primary)
        secondary_urls = replica_urls[:]
        secondary_urls.remove(primary)
        res = primary_proxy.apply_mutations(chunk_id, secondary_urls, primary, [])
        if res != 'success':
            return 'failure applying mutations to replicas'

        return 'success'

def main():
    #TODO move testing functionality into another file
    c = Client()
    if sys.argv[1] == '0':
        c.create("hello.txt")
    elif sys.argv[1] == '1':
        for i in range(10):
            time.sleep(random.uniform(.01, .1))
            c.write("hello.txt", "hello")
            print('hello iteration', i)
    else:
        for i in range(10):
            time.sleep(random.uniform(.01, .1))
            c.write("hello.txt", "bye")
            print('bye iteration', i)

if __name__ == '__main__':
    main()

