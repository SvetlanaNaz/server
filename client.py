import socket
import time

class ClientError(Exception):
    pass


class Client:
    def __init__(self, host, port, timeout=None):
        self.host = host
        self.port = port
        self.tm = timeout
        try:
            self.conn = socket.create_connection((host, port), self.tm)
        except socket.error:
            raise ClientError()

    def answer(self):
        data = b""

        while not data.endswith(b"\n\n"):
            try:
                data += self.conn.recv(1024)
            except socket.error :
                raise ClientError()
        data = data.decode()
        if data[:data.find('\n')] == 'error':
            raise ClientError()
        return data[data.find('\n') + 1:].strip()

    def get(self,key):
        try:
            self.conn.sendall('get {}\n'.format(key).encode())
        except socket.error:
            raise ClientError()

        data1=self.answer()
        data={}
        if data1 == '':
            return data

        for i in data1.split('\n'):
            list1=i.split()

            key=list1[0]
            val=list1[1]
            tmp=list1[2]
            if key not in data:
                data[key]=[]
            data[key].append((int(tmp), float(val)))
        return data

    def put(self,key,val,tmp=None):
        if tmp is None:
            tmp=str(int(time.time()))
        else: pass
        try:
            self.conn.sendall('put {} {} {}\n'.format(key,val,tmp).encode())
        except socket.error:
            raise ClientError()
        self.answer()
        
    def close(self):
        try:
            self.conn.close()
        except socket.error:
            raise ClientError()