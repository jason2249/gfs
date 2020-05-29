'''
client.py
@author Jason Lin

Client for simplified Google File System implementation. Handles creating, reading,
writing, and deleting files.
'''
from xmlrpc.client import ServerProxy

import random
import time

class Client():
    def __init__(self, chunk_size=64, master_url='http://localhost:9000', \
            cache_timeout=60, debug=False):
        random.seed(0)
        self.debug = debug
        self.chunk_size = chunk_size
        self.master_proxy = ServerProxy(master_url)
        # read_cache: (filename,chunk_idx) -> [chunk_id,[replica urls],time]
        # used for caching the replica urls of a given file and chunk index when reading
        self.read_cache = {}
        self.read_cache_timeout_secs = cache_timeout
        # primary_cache: (filename,chunk_idx) -> [chunk_id,primary,[replica urls]]
        # used for caching the primary of a given file and chunk index when writing
        self.primary_cache = {}

    '''
    create:
    Tell the master to create a file in GFS with the given filename.

    @param filename(string): desired filename to create
    @return string: result message
    '''
    def create(self, filename):
        return self.master_proxy.create(filename)

    '''
    delete:
    Delete cached entries for filename, then tell the master to delete the file
    with the given filename from GFS.

    @param filename(string): desired filename to delete
    @return string: result message
    '''
    def delete(self, filename):
        to_delete = []
        for f, chunk_idx in self.primary_cache:
            if f == filename:
                to_delete.append((f, chunk_idx))
        for tup in to_delete:
            del self.primary_cache[tup]
        return self.master_proxy.delete(filename)

    '''
    read:
    Read amount bytes from thespecified filename, starting at a given offset.
    Splits the amount into different chunks if the read overlaps chunks.

    @param filename(string): desired filename to read
    @param byte_offset(int): offset within file to start reading from
    @param amount(int): number of bytes to read starting from byte_offset
    @return string: bytes read from filename, or 'file not found'
    '''
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
                if res == 'file not found' or res == 'requested chunk idx out of range':
                    return res
                res.append(time.time())
                self.read_cache[(filename,chunk_idx)] = res
            chunk_id = res[0]
            replica_urls = res[1]

            # pick random replica to read from
            while len(replica_urls) > 0:
                replica_url = random.choice(replica_urls)
                chunkserver_proxy = ServerProxy(replica_url)
                try:
                    read_res = chunkserver_proxy.read( \
                            chunk_id, chunk_offset, amount_to_read_in_chunk)
                    if read_res == None:
                        if self.debug:
                            print('invalid checksum found in replica, trying another')
                        replica_urls.remove(replica_url)
                        continue
                    s += read_res
                    break
                except Exception as e:
                    if self.debug:
                        print('exception reading from replica:', e)
                    replica_urls.remove(replica_url)
            if len(replica_urls) == 0:
                return 'no replicas remaining for chunk'

            total_to_read -= amount_to_read_in_chunk
            amount_to_read_in_chunk = min(self.chunk_size, total_to_read)
            chunk_idx += 1
            chunk_offset = 0
        return s

    '''
    write:
    Writes the given data to the desired file. The client specifies the byte_offset,
    which must be the last byte written so far in the file.
    Splits writes that overlap chunks into multiple writes.

    @param filename(string): desired filename to write
    @param data(string): data to write to file
    @param byte_offset(int): offset within file that corresponds to the last byte written
    @return int: offset within file that the data was actually written to.
    This may differ from the provided byte_offset when there are multiple concurrent writers
    to the same file.
    '''
    def write(self, filename, data, byte_offset):
        chunk_idx = byte_offset // self.chunk_size
        first_chunk_idx = chunk_idx
        chunk_offset = byte_offset % self.chunk_size
        
        data_idx = 0
        total_to_write = len(data)
        amount_to_write_in_chunk = min(self.chunk_size - chunk_offset, total_to_write)

        backoff_secs = 1
        offset_written_at = None
        while total_to_write > 0:
            if chunk_idx != first_chunk_idx:
                # if this isn't the first chunk, create another chunk
                self.create(filename)
            data_piece = data[data_idx:data_idx+amount_to_write_in_chunk]
            res = self.write_helper(filename, data_piece, chunk_idx)
            if type(res) != int:
                if res == 'file not found':
                    return res
                # error, wait for exponential backoff and retry
                print(res, 'for ' + filename + ', backing off and retrying in ' \
                        + str(backoff_secs) + ' seconds')
                time.sleep(backoff_secs)
                backoff_secs *= 2
            else:
                if chunk_idx == first_chunk_idx:
                    offset_written_at = res
                chunk_idx += 1
                data_idx += amount_to_write_in_chunk
                total_to_write -= amount_to_write_in_chunk
                amount_to_write_in_chunk = min(self.chunk_size, total_to_write)
        return offset_written_at

    '''
    write_helper:
    Helper method to handle sending data to replicas as well as actually applying the
    mutations to those replicas. Handles lease management and requesting new primaries
    if the current primary is down or the lease has expired.

    @param filename(string): desired filename to write
    @param data(string): data to write to file
    @param chunk_idx(int): chunk index of file to write to
    @return int: offset within file that the data was actually written to.
    '''
    def write_helper(self, filename, data, chunk_idx):
        # locate primary and replica urls
        if (filename, chunk_idx) in self.primary_cache:
            res = self.primary_cache[(filename,chunk_idx)]
            if self.debug:
                print('using from primary cache:', res)
        else:
            res = self.master_proxy.get_primary(filename, chunk_idx, False)
            if res == 'file not found':
                return res
            self.primary_cache[(filename,chunk_idx)] = res
            if self.debug:
                print('asked master for primary:', res)
        chunk_id = res[0]
        primary = res[1]
        replica_urls = res[2]

        # send data to first replica, which sends data to all other replicas
        while len(replica_urls) > 0:
            replica_url = replica_urls[0]
            chunkserver_proxy = ServerProxy(replica_url)
            try:
                send_res = chunkserver_proxy.send_data(chunk_id, data, 1, replica_urls)
            except:
                if self.debug:
                    print('exception when sending data to', replica_url)
                send_res = 'chunkserver failure_' + replica_url
            # if any replica failed, reselect primary and replicas
            if send_res != 'success':
                if self.debug:
                    print('replica failed when sending data, asking for primary again')
                    print('res=', send_res)
                if send_res == 'timed out':
                    time.sleep(2)
                    continue
                failed_url = send_res[send_res.rfind('_')+1:]
                self.master_proxy.remove_chunkserver(failed_url)
                new_lease_res = self.master_proxy.get_primary(filename, chunk_idx, True)
                self.primary_cache[(filename, chunk_idx)] = new_lease_res
                chunk_id = new_lease_res[0]
                primary = new_lease_res[1]
                replica_urls = new_lease_res[2]
            else:
                break
        if len(replica_urls) == 0:
            return 'no remaining replicas'

        # tell the primary to apply mutations to all secondary replicas
        while True:
            primary_proxy = ServerProxy(primary)
            secondary_urls = replica_urls[:]
            secondary_urls.remove(primary)
            # to simplify state machine, assume that primary doesn't fail between selecting
            # it a few lines above and here
            mutation_res = primary_proxy.apply_mutations( \
                    chunk_id, secondary_urls, primary, [])

            # if lease expired, reselect primary and replicas
            if mutation_res == 'not primary':
                if self.debug:
                    print('lease expired, asking for primary again')
                new_lease_res = self.master_proxy.get_primary(filename, chunk_idx, True)
                old_urls = replica_urls[:]
                self.primary_cache[(filename, chunk_idx)] = new_lease_res
                chunk_id = new_lease_res[0]
                primary = new_lease_res[1]
                replica_urls = new_lease_res[2]

                # resend data if the replicas changed
                send_again_urls = [url for url in replica_urls if url not in old_urls]
                if len(send_again_urls) > 0:
                    if self.debug:
                        print('sending data to new replicas')
                    resend_proxy = ServerProxy(send_again_urls[0])
                    resend_proxy.send_data(chunk_id, data, 1, send_again_urls)
                continue

            # find offset of written data in mutation_res
            offset_within_chunk = mutation_res[data]
            written_offset = (chunk_idx * self.chunk_size) + offset_within_chunk
            return written_offset

