import asyncio

class My_Error(Exception):
    pass

class Protocol:

    def encode(self, serv_ans):
        list1 = []
        for i in serv_ans:
            if i is None:
                continue

            k_list = i.keys()
            for key in k_list:
                values = i[key]

                for _tuple in values:
                    _tuple = list(_tuple)
                    list1.append("{} {} {}".format(key, _tuple[1], _tuple[0]))

        res = 'ok\n'

        if list1 != []:
            for i in list1:
                res += str(i) + '\n'

        res += '\n'
        return res

    def decode(self, data):
        request = []
        try:
            _list = data.strip().split()
            if _list[0] == "put":
                request.append((_list[0], _list[1], float(_list[2]), int(_list[3])))
            elif _list[0] == "get":
                request.append((_list[0], _list[1]))
            else:
                raise My_Error()
        except:
            raise My_Error()
        return request

class Doer:
    def __init__(self, _obj):
        self._obj = _obj

    def run(self, command, *params):
        if command == "put":
            return self._obj.put(*params)
        elif command == "get":
            return self._obj.get(*params)
        else:
            raise My_Error()


class Metrics:
    def __init__(self):
        self.data = {}

    def put(self, key, value, timestamp):
        if key not in self.data:
            self.data[key] = {}
        self.data[key][timestamp] = value

    def get(self, key):
        data1 = self.data
        if key != "*":
            val = data1.get(key, {})
            data1 = {key: val}
        k_list = data1.keys()
        result = {}
        for key in k_list:
            tmp = data1[key]
            result[key] = sorted(tmp.items())
        return result


class ClientServerProtocol(asyncio.Protocol):
    metric = Metrics()

    def __init__(self):
        super().__init__()

        self.protocol = Protocol()
        self.executor = Doer(self.metric)
        self.str_byte = b''

    def process_data(self, data):
        commands = self.protocol.decode(data)

        answer = []
        for c in commands:
            answer1 = self.executor.run(*c)
            answer.append(answer1)

        _str = self.protocol.encode(answer)
        return _str

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self.str_byte += data
        try:
            str_byte = self.str_byte.decode()
        except:
            return

        if not str_byte.endswith('\n'):
            return

        self.str_byte = b''

        try:
            resp = self.process_data(str_byte)
        except My_Error as err:
            self.transport.write("error\n{}\n\n".format(err).encode())
            return

        self.transport.write(resp.encode())


def run_server(host, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(ClientServerProtocol, host, port)
    server = loop.run_until_complete(coro)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


run_server('127.0.0.1', 8888)
