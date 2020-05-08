from xmlrpc.client import ServerProxy

class Client():
    def __init__(self):
        self.master_proxy = ServerProxy('http://localhost:9000')

def main():
    c = Client()
    c.master_proxy.create('hello.txt')

if __name__ == '__main__':
    main()

