import client
import threading
import time
import random

def main():
    random.seed(0)
    time_read_throughput()
    time_write_separate_throughput()
    basic_test()
    multi_chunk_files_test()
    multi_client_same_file_sequential_test()
    multi_client_same_file_parallel_test()

    print('all tests passed!')

def basic_test():
    c = client.Client(cache_timeout=1)
    c.create('hello.txt')
    offset = c.write('hello.txt', 'hello', 0)
    assert(offset == 0)
    res = c.read('hello.txt', 0, 5)
    assert(res == 'hello')
    c.delete('hello.txt')
    time.sleep(1)
    res = c.read('hello.txt', 0, 5)
    assert(res == 'file not found')
    print('passed basic_test')

def multi_chunk_files_test():
    c = client.Client()
    expected1 = ''
    expected2 = ''
    c.create('hello.txt')
    c.create('goodbye.txt')
    
    offset = 0
    for i in range(20):
        time.sleep(random.uniform(.01, .1))
        s = 'hello' + str(i)
        res_offset = c.write('hello.txt', s, offset)
        assert(res_offset == offset)
        offset += len(s)
        expected1 += s
    res = c.read('hello.txt', 0, offset)
    if res != expected1:
        print('res:', res)
        print('expected:', expected1)
    assert(res == expected1)

    offset = 0
    for i in range(20):
        time.sleep(random.uniform(.01, .1))
        s = 'bye' + str(i)
        res_offset = c.write('goodbye.txt', s, offset)
        assert(res_offset == offset)
        offset += len(s)
        expected2 += s
    res = c.read('goodbye.txt', 0, offset)
    assert(res == expected2)
    c.delete('hello.txt')
    c.delete('goodbye.txt')
    print('passed multi_chunk_files_test')

def multi_client_same_file_sequential_test():
    c = client.Client()
    c2 = client.Client()

    c.create('test3.txt')
    offset = 0
    expected = ''
    for i in range(10):
        if i % 2 == 0:
            res_offset = c.write('test3.txt', 'hello', offset)
            assert(res_offset == offset)
            offset += len('hello')
            expected += 'hello'
        else:
            res_offset = c2.write('test3.txt', 'bye', offset)
            assert(res_offset == offset)
            offset += len('bye')
            expected += 'bye'
    res = c2.read('test3.txt', 0, offset)
    assert(res == expected)
    c.delete('test3.txt')
    print('passed multi_client_same_file_sequential_test')

numbers = '1234567890!@#$%^&*()-=_+{}'
def multi_client_same_file_parallel_test():
    c = client.Client()
    c2 = client.Client()
    c.create('parallel_test.txt')
    data_to_offset = {}
    data_to_offset2 = {}
    parallel_thread = threading.Thread(target=run_parallel_client_write, \
            args=[c2, data_to_offset2])
    parallel_thread.start()
    offset = 0
    for num in numbers:
        res_offset = c.write('parallel_test.txt', num, offset)
        data_to_offset[num] = res_offset
        offset = res_offset + len(num)
    parallel_thread.join()
    for data in data_to_offset:
        offset = data_to_offset[data]
        res = c2.read('parallel_test.txt', offset, len(data))
        assert(res == data)
    for data in data_to_offset2:
        offset = data_to_offset2[data]
        res = c.read('parallel_test.txt', offset, len(data))
        assert(res == data)
    c2.delete('parallel_test.txt')
    print('passed multi_client_same_file_parallel_test')

alphabet = 'abcdefghijklmnopqrstuvwxyz'
def run_parallel_client_write(c, data_to_offset):
    offset = 0
    for letter in alphabet:
        res_offset = c.write('parallel_test.txt', letter, offset)
        data_to_offset[letter] = res_offset
        offset = res_offset + len(letter)

def client_read():
    c = client.Client()
    for i in range(250):
        rand_start = random.randint(0, 99996)
        read_res = c.read('read_throughput.txt', rand_start, 4000)

def read_throughput_test():
    num_readers = 1
    all_threads = []
    for i in range(num_readers):
        parallel_thread = threading.Thread(target=client_read, \
                args=[])
        all_threads.append(parallel_thread)
        parallel_thread.start()
    for t in all_threads:
        t.join()

def time_read_throughput():
    c = client.Client()
    c.create('read_throughput.txt')
    res = c.write('read_throughput.txt', 'a'*10000000, 0)
    assert(res == 0)
    start_time = time.time()
    read_throughput_test()
    end_time = time.time()
    print('read_throughput_test took', end_time-start_time, 'seconds')

def client_write_separate(i):
    c = client.Client()
    fname = 'client_write_separate' + str(i) + '.txt'
    c.create(fname)
    offset = 0
    c.write(fname, 'a'*1000000, offset)
    offset += 1000000

def write_separate_throughput_test():
    num_writers = 1
    all_threads = []
    for i in range(num_writers):
        parallel_thread = threading.Thread(target=client_write_separate, \
                args=[i])
        all_threads.append(parallel_thread)
        parallel_thread.start()
    for t in all_threads:
        t.join()

def time_write_separate_throughput():
    start_time = time.time()
    write_separate_throughput_test()
    end_time = time.time()
    print('write_separate_throughput_test took', end_time-start_time, 'seconds')

if __name__ == '__main__':
    main()

