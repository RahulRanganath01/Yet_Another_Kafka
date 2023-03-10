import socket
import sys
ClientMultiSocket = socket.socket()
ClientMultiSocket1 = socket.socket()
ClientMultiSocket2 = socket.socket()
host = '127.0.0.1'

#host = '10.20.203.176'
port = 6006

host1 = '127.0.0.1'
port1 = 6007
host2 = '127.0.0.1'
port2 = 6008
print('Waiting for connection response')
try:
    ClientMultiSocket.connect((host, port))
except socket.error as e:
    print(str(e))
ClientMultiSocket.send(str.encode(sys.argv[1]))

try:
    ClientMultiSocket1.connect((host1, port1))
except socket.error as e:
    print(str(e))
ClientMultiSocket1.send(str.encode(sys.argv[1]))

try:
    ClientMultiSocket2.connect((host2, port2))
except socket.error as e:
    print(str(e))
ClientMultiSocket2.send(str.encode(sys.argv[1]))

if sys.argv[1] == "p":
    while True:
        Input = input('Enter the topic and contents of the topic with a comma separated: ')

        ClientMultiSocket.send(str.encode(Input))
        res = ClientMultiSocket.recv(1024)

        ClientMultiSocket1.send(str.encode(Input))
        ClientMultiSocket2.send(str.encode(Input))
        
        print(res.decode('utf-8'))
    ClientMultiSocket.close()

else:
    Input = input('Enter the topic name for which you need the contents for: ')
    ClientMultiSocket.send(str.encode(Input))

    ClientMultiSocket1.send(str.encode(Input))
    ClientMultiSocket2.send(str.encode(Input))
    res = ""
    while True:
        if (ClientMultiSocket.recv) != res:
            res = ClientMultiSocket.recv(1024)
            print(res.decode('utf-8'))
            
    ClientMultiSocket.close()
    ClientMultiSocket1.close()
    ClientMultiSocket2.close()
