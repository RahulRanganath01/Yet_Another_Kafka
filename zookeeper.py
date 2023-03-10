import socket
from threading import start_new_thread
from time import sleep
import pickle

port_of_pros = []  # port number of each process
n = len(port_of_pros)
myID = 0
unsuccessful = 'false'  # a unsuccessful flag which denotes if a process is failed or not

socket_1 = socket.socket()
socket_2 = socket.socket()

socket_2.bind(('0.0.0.0', port_of_pros[myID]))
socket_2.listen(2)

while True:
    try:
        # trying to connect to the first process.
        socket_1.connect(('localhost', port_of_pros[(myID+1) % n]))
    except:
        # if it fails we are waiting for the connection to the process.
        print("Waiting for connection...")
        sleep(1)  # the socket will be sleeping until it happens.
        continue

    break
socket_1.send('handshake')
print(socket_1)

print('hello')

# recv function is used to recieve data from both TCP and UDP sockets of size 1024 bytes
failData = socket_1.recv(1024)

# a flag to tell if the current node is the one conducting the election
Initiator_Flag = False


def client_thread(c):
    global socket_1, failData, Initiator_Flag
    try:
        while True:
            recieved_Data = c.recv(1024)
            if recieved_Data == 'handshake':
                print('handshake received')
                c.send(unsuccessful)

            if recieved_Data == 'false' or failData == 'false':
                print('unsuccessful status of process') + \
                    str((myID+1) % n) + ' : ' + str(failData)
                failData = ''
            elif recieved_Data == 'true' or failData == 'true':
                failData = ''
                socket_1.close()
                socket_1 = socket.socket()
                print('Initiating Election')
                socket_1.connect(('localhost', port_of_pros[(myID+2) % n]))
                lst = [myID]
                lst = pickle.dumps(lst)
                socket_1.send('election')
                sleep(2)
                socket_1.send(lst)
                print("sent")
            elif recieved_Data == 'election':
                if not Initiator_Flag:
                    print("hello")
                    recieved_Data = c.recv(1024)
                    print('election list received')
                    data2 = pickle.loads(recieved_Data)
                    print(data2)
                    if myID in data2:
                        Initiator_Flag = True

                    if not Initiator_Flag:
                        data2.append(myID)
                        recieved_Data = pickle.dumps(data2)
                        socket_1.send('election')
                        sleep(1)
                        socket_1.send(recieved_Data)
                    elif Initiator_Flag:
                        print("election recieved_Data received")
                        print(data2)
                        coordinator = max(data2)
                        print('Process ' + str(coordinator) +
                              ' is the coordinator')
                        socket_1.send('leader')
                        sleep(1)
                        socket_1.send(str(coordinator))
            elif recieved_Data == 'leader' and not Initiator_Flag:
                recieved_Data = c.recv(1024)
                print('Process ' + recieved_Data + ' is the coordinator')
                socket_1.send('leader')
                sleep(1)
                socket_1.send(str(recieved_Data))
            elif recieved_Data == 'leader' and Initiator_Flag:
                Initiator_Flag = False
    except KeyboardInterrupt:
        # close the sockets in case of a keyboard interrupt
        c.close()
        socket_1.close()
        socket_2.close()
        print("sockets are closed ...")


while True:
    c, addr = socket_2.accept()
    start_new_thread(client_thread, (c,))
