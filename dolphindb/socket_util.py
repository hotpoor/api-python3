def sendall(socket, msg, objs=b""):
    totalsent = 0
    MSGLEN = len(msg)
    while totalsent < MSGLEN:
        sent = socket.send(msg[totalsent:].encode('utf-8'))
        if sent == 0:
            raise RuntimeError("socket connection broken")
        totalsent = totalsent + sent

    if objs != b"":
        for obj in objs:
            # print(obj)
            sent = socket.send(obj)
            totalsent = totalsent + sent
    return totalsent


_rcv_buffer_size = 32768


def recvall(socket, n, bufferList):
    socket.setblocking(1)
    buffer = bufferList[0]
    if len(buffer) >= n:
        ret = buffer[:n]
        bufferList[0] = buffer[n:]
        return ret
    while len(buffer) < n:
        packet = socket.recv(_rcv_buffer_size)
        if not packet:
            break
        buffer += packet

    if len(buffer) >= n:
        ret = buffer[:n]
        bufferList[0] = buffer[n:]
        return ret
    else:
        ret = buffer
        bufferList[0] = b''
        return ret


def recvallhex(socket, n, bufferList):
    socket.setblocking(1)
    buffer = bufferList[0]
    if len(buffer) >= n:
        ret = buffer[:n].hex()
        bufferList[0] = buffer[n:]
        return ret

    while len(buffer) < n:
        packet = socket.recv(_rcv_buffer_size)
        if not packet:
            break
        buffer += packet

    if len(buffer) >= n:
        ret = buffer[:n]
        bufferList[0] = buffer[n:]
        return ret
    else:
        ret = buffer
        bufferList[0] = b''
        return ret


def readline(socket, bufferList):
    start = 0
    buffer = bufferList[0]
    while True:
        pos = buffer.find(b'\n', start)
        if pos != -1:
            ret = buffer[:pos]
            bufferList[0] = buffer[pos+1:]
            return ret.decode('utf-8')
        start = len(buffer)
        packet = socket.recv(_rcv_buffer_size)
        if packet == b'':
            raise IOError("read empty byte")
        buffer += packet


def read_string(socket, bufferList):
    start = 0
    buffer = bufferList[0]
    while True:
        pos = buffer.find(b'\x00', start)
        if pos != -1:
            ret = buffer[:pos]
            bufferList[0] = buffer[pos+1:]
            return ret.decode('utf-8')
        start = len(buffer)
        packet = socket.recv(_rcv_buffer_size)
        if packet == b'':
            raise IOError("read empty byte")
        buffer += packet


# stable version of socket_util.py
#
# def recvall(socket, n, bufferList):
#     data = b''
#     socket.setblocking(1)
#     while len((data)) < n:
#         packet = socket.recv(n - len((data)))
#         if not packet:
#             break
#         data += packet
#     return data
#
# def recvallhex(socket, n, bufferList):
#     data = ''
#     socket.setblocking(1)
#     while len(bytes.fromhex(data)) < n:
#         packet = socket.recv(n - len(bytes.fromhex(data)))
#         if not packet:
#             break
#         data += packet.hex()
#     return data
#
#
# def readline(socket, bufferList):
#     data = b''
#     while True:
#         packet = socket.recv(1)
#         if packet == b"":
#             raise IOError("read empty byte")
#         if b'\n' == packet:
#             return data.decode('utf-8')
#         data += packet
#
#
# def read_string(socket, bufferList):
#     data = b''
#     while True:
#         packet = socket.recv(1)
#         if b'\x00' == packet:
#             return data.decode('utf-8')
#         data += packet
