import socket
import os
from _thread import *
import sys

ProducerSideSocket = socket.socket()
#host = '127.0.0.1'
#port = 6008

host = sys. argv[1]
port = int(sys.argv[2])

ThreadCount_p = 0
ThreadCount_c = 0
something = {}

try:
    ProducerSideSocket.bind((host, port))
except socket.error as e:
    print(str(e))


print('Socket is listening..')
ProducerSideSocket.listen(5)


def multi_threaded_producer(connection):
    while True:
        data_d = connection.recv(2048)
        data = data_d.decode('utf-8')
        topic= data.split(",",1)[0]
        con = data.split(",",1)[1]
        print('Topic: ' + topic + ' has contents:' + con)
    connection.close()


while True:
    Client_p, address_p = ProducerSideSocket.accept()
    print('Connected to: ' + address_p[0] + ':' + str(address_p[1]))
    d = Client_p.recv(2048)
    d_d = d.decode('utf-8')
    if d_d == "p":
        start_new_thread(multi_threaded_producer, (Client_p, ))
        ThreadCount_p += 1
        print('Total Number of Producers: ' + str(ThreadCount_p))
    else:
        ThreadCount_c += 1
        print('Total Number of Consumers: ' + str(ThreadCount_c))
ProducerSideSocket.close()

