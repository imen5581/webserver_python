import asyncio

class ClientError(Exception):
    pass

class ClientServerProtocol(asyncio.Protocol):

    local_data = {}

    def connection_made(self, transport):
        self.transport = transport
        #print(self.transport.get_extra_info(name='peername'))

    def get(self, data):
        try:
            answer_to_service = 'ok\n'
            answer_key = data[0]
            if answer_key == '*':
                for key, values in self.local_data.items():
                    for value in values:
                        answer_to_service += " ".join([key, str(value[1]), str(value[0]) + '\n'])
            elif answer_key != '*':
                if answer_key in self.local_data.keys():
                    for value in self.local_data[answer_key]:
                        answer_to_service += ' '.join([answer_key, str(value[1]), str(value[0]) + '\n'])
            return f'{answer_to_service}\n'
        except Exception as err:
            raise ClientError(err)

    def put(self, data):

        def isfloat(value):
            try:
                float(value)
                return True
            except ValueError:
                return False

        def isint(value):
            try:
                int(value)
                return True
            except ValueError:
                return False

        if isfloat(data[1]) and isint(data[2]):
            data_key = data[0]
            if data_key not in self.local_data:
                self.local_data[data_key] = []
                #palm.cpu aaa 1150864247\n
            tuplvalues = (int(data[2]), float(data[1]))
            if tuplvalues not in self.local_data[data_key]:
                self.local_data[data_key].append(tuplvalues)
                self.local_data[data_key] = list(dict(self.local_data[data_key]).items())
                self.local_data[data_key].sort(key=lambda x: x[0])



            return "ok\n\n"
        else:
            return 'error\nwrong command\n\n'

    def process_data(self, data):
        list_data = data.strip('\r\n').split()
        if len(list_data) > 1 and len(list_data) <= 4:
            request = list_data[0]
            if request == 'get' and len(list_data) == 2:
                    response = self.get(list_data[1:])
            elif request == 'put' and len(list_data) >= 3:
                response = self.put(list_data[1:])
            else:
                response = 'error\nwrong command\n\n'
            return response
        else:
            response = 'error\nwrong command\n\n'
            return response

    def data_received(self, data):
        resp = self.process_data(data.decode())
        self.transport.write(resp.encode())


def run_server(host='127.0.0.1', port=8888):

    loop = asyncio.get_event_loop()
    coro = loop.create_server(
        ClientServerProtocol,
        host, port
    )

    server = loop.run_until_complete(coro)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


if __name__ == '__main__':
    run_server("127.0.0.1", 8888)