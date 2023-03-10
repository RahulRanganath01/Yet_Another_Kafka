import socket
import os
from _thread import *
import sys
from socket import gethostbyname
import os.path
import logging

ProducerSideSocket = socket.socket()
#host = gethostbyname('0.0.0.0') 
#host = '127.0.0.1'
#port = 6006

host = sys. argv[1]
port = int(sys.argv[2])

ThreadCount_p = 0
ThreadCount_c = 0
path1 = 'F:/Broker_Files'
path2 = 'F:/Broker_Files_Replica'
something = {}


if os.path.isdir(path1):
    os.makedirs(path1, exist_ok = True)
else:
    os.mkdir(path1)
if os.path.isdir(path2):
    os.makedirs(path2, exist_ok = True)
else:
    os.mkdir(path2)
log1 = path1 + '/' + 'Logfile1' + '.log'

logging.basicConfig(filename=log1, encoding='utf-8', level=logging.DEBUG)

try:
    ProducerSideSocket.bind((host, port))
except socket.error as e:
    print(str(e))


message = 'Socket is listening...'
print(message)
logging.info("Message from Server: {}".format(message))



ProducerSideSocket.listen(5)





def multi_threaded_producer(connection):
    connection.send(str.encode('Server is working:'))
    while True:
        data_d = connection.recv(2048)
        data = data_d.decode('utf-8')
        topic= data.split(",",1)[0]
        con = data.split(",",1)[1]
        if topic not in something.keys():
            something[topic] = [con]
        else:
            something[topic].append(con)
        response = 'Broker message: ' + 'Message Recieved by the broker. Thank you Producer'
        if not data:
            break
        connection.sendall(str.encode(response))
        print('Topic: ' + topic + ' has contents:' + con)
        logging.info("Topic: {}".format(topic))
        logging.info(" Contents: {}".format(con))


        path_f = path1 + '/' + topic
        path_ff = path2 + '/' + topic
        topic_name = path_f + '/' + topic + ".txt"
        topic_name1 = path_ff + '/' + topic + ".txt"
        #Original
        f1 = path_f + '/' + topic + '1' + ".txt"
        f2 = path_f + '/' + topic + '2' + ".txt"
        f3 = path_f + '/' + topic + '3' + ".txt"
        f4 = path_f + '/' + topic + '4' + ".txt"
        count_path = path_f + '/' + "count.txt"
        #Replica
        f11 = path_ff + '/' + topic + '1' + ".txt"
        f22 = path_ff + '/' + topic + '2' + ".txt"
        f33 = path_ff + '/' + topic + '3' + ".txt"
        f44 = path_ff + '/' + topic + '4' + ".txt"
        count_path1 = path_ff + '/' + "count.txt"

        #original
        if os.path.isdir(path_f):
            partition_1 = open(f1, "a")
            partition_2 = open(f2, "a")
            partition_3 = open(f3, "a")
            partition_4 = open(f4, "a")
            path_f_r = open(count_path, "r")

            c = path_f_r.readline()
            c = int(c)
            path_f_r.close()
            path_f_w = open(count_path, "w")
            path_f_w.write(str(c + 1))
            path_f_w.close()
            n = c % 4
            if n == 0:
                partition_1.write(con + '\n')
            elif n == 1:
                partition_2.write(con + '\n')
            elif n == 2:
                partition_3.write(con + '\n')
            else:
                partition_4.write(con + '\n')

            partition_1.close()
            partition_2.close()
            partition_3.close()
            partition_4.close()
        else:
            os.mkdir(path_f)
            partition_1 = open(f1, "w")
            partition_2 = open(f2, "w")
            partition_3 = open(f3, "w")
            partition_4 = open(f4, "w")
            count_file = open(count_path, "w")
            count_file.write(str(1))
            count_file.close()
            partition_1.write(con + '\n')
            partition_1.close()
            partition_2.close()
            partition_3.close()
            partition_4.close()

        #replica
        if os.path.isdir(path_ff):
            partition_11 = open(f11, "a")
            partition_22 = open(f22, "a")
            partition_33 = open(f33, "a")
            partition_44 = open(f44, "a")
            path_f_r1 = open(count_path1, "r")

            d = path_f_r1.readline()
            d = int(d)
            path_f_r1.close()
            path_f_w1 = open(count_path1, "w")
            path_f_w1.write(str(d + 1))
            path_f_w1.close()
            m = d % 4
            if m == 0:
                partition_11.write(con + '\n')
            elif m == 1:
                partition_22.write(con + '\n')
            elif m == 2:
                partition_33.write(con + '\n')
            else:
                partition_44.write(con + '\n')

            partition_11.close()
            partition_22.close()
            partition_33.close()
            partition_44.close()
        else:
            os.mkdir(path_ff)
            partition_11 = open(f11, "w")
            partition_22 = open(f22, "w")
            partition_33 = open(f33, "w")
            partition_44 = open(f44, "w")
            count_file1 = open(count_path1, "w")
            count_file1.write(str(1))
            count_file1.close()
            partition_11.write(con + '\n')
            partition_11.close()
            partition_22.close()
            partition_33.close()
            partition_44.close()
    connection.close()

def multi_threaded_consumer(connection):
    connection.send(str.encode('Broker is working:'))
    top_c = connection.recv(2048)
    top_c_e = top_c.decode('utf-8')
    c_d = 0
    if top_c_e in something.keys():
        c_d_n = len(something[top_c_e])
    while True:
        if top_c_e not in something.keys():
            continue
        c_d_n = len(something[top_c_e])
        if top_c_e in something.keys() and c_d != c_d_n:
            connection.sendall(str.encode(str(something[top_c_e][c_d : c_d_n]).lstrip("[").rstrip("]")))
            c_d  = c_d_n
        else:
            pass
    connection.close()


while True:
    Client_p, address_p = ProducerSideSocket.accept()
    print('Connected to: ' + address_p[0] + ':' + str(address_p[1]))
    logging.info('Connected to: {}'.format(address_p[0]))
    logging.info(' : {}'.format(str(address_p[1])))
    d = Client_p.recv(2048)
    d_d = d.decode('utf-8')
    if d_d == "p":
        start_new_thread(multi_threaded_producer, (Client_p, ))
        ThreadCount_p += 1
        print('Total Number of Producers: ' + str(ThreadCount_p))
        logging.info('Total Number of Producers: {} '.format(str(ThreadCount_p)))
    else:
        start_new_thread(multi_threaded_consumer, (Client_p, ))
        ThreadCount_c += 1
        print('Total Number of Consumers: ' + str(ThreadCount_c))
        logging.info('Total Number of Consumers: {} '.format(str(ThreadCount_p)))
ProducerSideSocket.close()
