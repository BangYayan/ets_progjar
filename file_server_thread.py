from socket import *
import socket
import logging
import sys
from concurrent.futures import ThreadPoolExecutor
from file_protocol import FileProtocol

MAX_BUFFER_SIZE = 8*1024  # 8 KiB, typical

class ProcessTheClient:
    def __init__(self, connection, address):
        self.connection = connection
        self.address = address
        self.fp = FileProtocol()

    def run(self):
        d = ''
        while True:
            data = self.connection.recv(MAX_BUFFER_SIZE)
            if data:
                d += data.decode()
                if "\r\n\r\n" in d:
                    hasil=self.fp.proses_string(d.strip())
                    hasil=hasil+"\r\n\r\n"
                    self.connection.sendall(hasil.encode())
                    break
            else:
                break
        self.connection.close()
        return True

class FileServerThread:
    def __init__(self, ipaddress='0.0.0.0', port=8889, max_workers=5):
        self.ipinfo = (ipaddress, port)
        self.max_workers = max_workers
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.thread_pool = ThreadPoolExecutor(max_workers=self.max_workers)

    def run(self):
        logging.warning(f"threadpool server berjalan di ip address {self.ipinfo} dengan {self.max_workers} worker")
        self.my_socket.bind(self.ipinfo)
        self.my_socket.listen(5)
        while True:
            self.connection, self.client_address = self.my_socket.accept()
            logging.warning(f"connection from {self.client_address}")   
            client_handler = ProcessTheClient(self.connection, self.client_address)
            self.thread_pool.submit(client_handler.run)

def main():
    max_workers = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    svr = FileServerThread(ipaddress='0.0.0.0', port=9999, max_workers=max_workers)
    svr.run()

if __name__ == "__main__":
    main()