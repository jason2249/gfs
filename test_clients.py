import client
import time
import random

def main():

    basic_test()
    multi_chunk_files_test()

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
    res1 = ''
    res2 = ''
    c.create('hello.txt')
    c.create('goodbye.txt')
    
    offset = 0
    for i in range(20):
        time.sleep(random.uniform(.01, .1))
        s = 'hello' + str(i)
        c.write('hello.txt', s, offset)
        offset += len(s)
        res1 += s
    res = c.read('hello.txt', 0, offset)
    assert(res == res1)

    offset = 0
    for i in range(20):
        time.sleep(random.uniform(.01, .1))
        s = 'bye' + str(i)
        c.write('goodbye.txt', s, offset)
        offset += len(s)
        res2 += s
    res = c.read('goodbye.txt', 0, offset)
    assert(res == res2)
    print('passed multi_chunk_files_test')

if __name__ == '__main__':
    main()

