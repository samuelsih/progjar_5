import socket
import logging
import sys
from concurrent.futures import ThreadPoolExecutor


class BackendList:
    def __init__(self):
        self.servers = []
        self.servers.append(('127.0.0.1', 9002))
        self.servers.append(('127.0.0.1', 9003))
        self.servers.append(('127.0.0.1', 9004))
        self.servers.append(('127.0.0.1', 9005))
        self.current = 0

    def getserver(self) -> tuple:
        s = self.servers[self.current]
        self.current = self.current + 1
        if (self.current >= len(self.servers)):
            self.current = 0
        return s


def SendToServer(from_connection: socket.socket, server_connection: socket.socket):
    try:
        while True:
            data = from_connection.recv(32)
            if data:
                server_connection.send(data)
            else:
                server_connection.close()
                break
    except OSError:
        pass

    finally:
        from_connection.close()


def GetResultFromServer(from_connection: socket.socket, server_connection: socket.socket):
    try:
        while True:
            data = server_connection.recv(32)
            if data:
                from_connection.send(data)
            else:
                from_connection.close()
                break
    except OSError:
        pass

    finally:
        from_connection.close()


class Server:
    def __init__(self, portnumber: int):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('', portnumber))
        self.socket.listen(5)
        self.bservers = BackendList()
        # logging.warning(
        #     "load balancer running on port {}" . format(portnumber))

    def run(self):
        with ThreadPoolExecutor(30) as executor:
            while True:
                client_conn, client_addr = self.socket.accept()
                server_addr = self.bservers.getserver()
                server_socket = socket.socket(
                    socket.AF_INET, socket.SOCK_STREAM)
                server_socket.settimeout(1)

                # logging.warning(f"{client_conn} connected to {server_addr}")

                try:
                    server_socket.connect(server_addr)
                    # logging.warning("connection from {}".format(client_addr))

                    executor.submit(SendToServer, client_conn, server_socket)
                    executor.submit(GetResultFromServer,
                                    client_conn, server_socket)

                except Exception as e:
                    print(e)
                    break


def main():
    portnumber = 44444
    try:
        portnumber = int(sys.argv[1])
    except:
        pass
    svr = Server(portnumber)
    svr.run()


if __name__ == "__main__":
    main()
