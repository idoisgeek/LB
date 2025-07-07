import socket
import threading
import time

SERV_HOST = '10.0.0.1'
PORT_80 = 80
lock = threading.Lock() # To calculate finish time safely - each time only one req
thread_semaphore = threading.Semaphore(50) # Limit to 50 concurrent threads



#servers - keep ip, sock pd, finish time for servers management and types can handle
servers = {
    '1': {'addr': '192.168.0.101', 'sock': None, 'finish_time': 0, 'can_handle': ['V', 'P']},
    '2': {'addr': '192.168.0.102', 'sock': None, 'finish_time': 0, 'can_handle': ['V', 'P']},
    '3': {'addr': '192.168.0.103', 'sock': None, 'finish_time': 0, 'can_handle': ['M']}
}


def TimePrint(string):
    print('%s: %s----' % (time.strftime('%H:%M:%S', time.localtime(time.time())), string))


def createSocket(addr, port):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((addr, port))
        return sock
    except Exception as e:
        TimePrint("Could not connect to %s:%s: %s" % (addr, port, e))
        return None


def parse(req):
    if len(req) < 2:
        return None, None

    req_type = req[0]
    req_time = ord(req[1]) #convert to int in order to sum finish time

    return req_type, req_time


def getOptimalServer(req_type, req_time):
    #select optimal server, by type first and finish time second

    current_time = time.time()
    best_server = None
    earliest_finish = float('inf')

    with lock:
        for server_id, server_info in servers.items():
            # Check if server can handle this request type
            if req_type not in server_info['can_handle']:
                continue

            # Check if server socket is available
            if server_info['sock'] is None:
                continue

            # Calculate when this server will be free
            server_finish_time = max(current_time, server_info['finish_time'])

            if server_finish_time < earliest_finish:
                earliest_finish = server_finish_time
                best_server = server_id

        # Update the selected server's finish time
        if best_server:
            servers[best_server]['finish_time'] = max(current_time, servers[best_server]['finish_time']) + req_time

    return best_server


def handle_client(client_sock, client_addr):
    try:
        req = client_sock.recv(2)
        if len(req) < 2:
            TimePrint("Invalid request from %s" % (client_addr,))
            client_sock.close()
            return

        req_type, req_time = parse(req)
        if req_type is None or req_type not in ['V', 'M', 'P']:
            TimePrint("Invalid request type '%s' from %s" % (req_type, client_addr))
            client_sock.close()
            return

        servID = getOptimalServer(req_type, req_time)

        if servID is None:
            TimePrint("No available server for request type '%s' from %s" % (req_type, client_addr))
            client_sock.close()
            return

        server_sock = servers[servID]['sock']
        server_addr = servers[servID]['addr']

        TimePrint("Received %s request (time=%s) from %s, routing to server %s (%s)" %
                (req_type, req_time, client_addr[0], servID, server_addr))

        server_sock.sendall(req)
        data = server_sock.recv(2)
        client_sock.sendall(data)

    except Exception as e:
        TimePrint("Error handling client %s: %s" % (client_addr, e))
    finally:
        client_sock.close()
        thread_semaphore.release()


def main():
    global servers

    TimePrint("Smart Load Balancer Started")
    TimePrint("Server capabilities:")
    TimePrint("  Server 1 and 2: Video and Pictures")
    TimePrint("  Server 3: Music")
    TimePrint("Connecting to servers...")

    # Connect to all backend servers
    for server_id, server_info in servers.items():
        addr = server_info['addr']
        sock = createSocket(addr, PORT_80)
        servers[server_id]['sock'] = sock
        if sock:
            TimePrint("Connected to server %s at %s" % (server_id, addr))
        else:
            TimePrint("Failed to connect to server %s at %s" % (server_id, addr))

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    #listening on '10.0.0.1' port 80 for clients; new request handled using new thread
    try:
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((SERV_HOST, 80))
        server_sock.listen(10) #queue size 10
        TimePrint("Listening on %s:80" % SERV_HOST)

        while True:
            client_sock, client_addr = server_sock.accept()
            thread_semaphore.acquire()
            thread = threading.Thread(target=handle_client, args=(client_sock, client_addr))
            thread.daemon = True
            thread.start()
    finally:
        server_sock.close()


if __name__ == "__main__":
    main()