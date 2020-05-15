import client
import time
import random

def main():

    basic_test()
    multi_chunk_files_test()
    multi_client_same_file_sequential_test()

    print('all tests passed!')

def basic_test():
    c = client.Client(cache_timeout=1)
    c.create('hello.txt')
    c.write('hello.txt', 'hello', 0)
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
        c.write('hello.txt', s, offset)
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
        c.write('goodbye.txt', s, offset)
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
            c.write('test3.txt', 'hello', offset)
            offset += len('hello')
            expected += 'hello'
        else:
            c2.write('test3.txt', 'bye', offset)
            offset += len('bye')
            expected += 'bye'
    res = c2.read('test3.txt', 0, offset)
    assert(res == expected)
    c.delete('test3.txt')
    print('passed multi_client_same_file_sequential_test')

if __name__ == '__main__':
    main()

