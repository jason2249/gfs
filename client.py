from xmlrpc.client import ServerProxy

import random
import time

class Client():
    def __init__(self, chunk_size=64, master_url='http://localhost:9000', \
            cache_timeout=60):
        self.chunk_size = chunk_size
        self.master_proxy = ServerProxy(master_url)
        # read_cache: (filename,chunk_idx) -> [chunk_id,[replica urls],time]
        self.read_cache = {}
        self.read_cache_timeout_secs = cache_timeout
        # primary_cache: (filename,chunk_idx) -> [chunk_id,primary,[replica urls]]
        self.primary_cache = {}

    def create(self, filename):
        return self.master_proxy.create(filename)

    def delete(self, filename):
        return self.master_proxy.delete(filename)

    def read(self, filename, byte_offset, amount):
        chunk_idx = byte_offset // self.chunk_size
        chunk_offset = byte_offset % self.chunk_size

        total_to_read = amount
        amount_to_read_in_chunk = min(self.chunk_size - chunk_offset, total_to_read)
        s = ''
        while total_to_read > 0:
            # locate replica urls of chunk
            should_contact_master = True
            if (filename, chunk_idx) in self.read_cache:
                res = self.read_cache[(filename, chunk_idx)]
                original_cache_time = res[2]
                if time.time() <= original_cache_time + self.read_cache_timeout_secs:
                    #refresh original cache time
                    self.read_cache[(filename, chunk_idx)][2] = time.time()
                    should_contact_master = False

            if should_contact_master:
                res = self.master_proxy.read(filename, chunk_idx)
                if res == 'file not found':
                    return res
                res.append(time.time())
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
                if res == 'file not found':
                    return res
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
            if res == 'file not found':
                return res
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



