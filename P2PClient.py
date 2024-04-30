
import socket
import threading
import time
import logging
import argparse
import os
import struct

#logging
logging.basicConfig(filename="logs.log", format="%(message)s", filemode="a")
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

#Global Variable
tracker_ip = "localhost"
tracker_port = 5100
folder_path = ''
transfer_port = 0
entity_name = ''
missing_chunks = []
total_chunk = 0
local_chunks = {}

def parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('-folder', required=True)
    parser.add_argument('-transfer_port', type=int, required=True)
    parser.add_argument('-name', required=True)
    
    args = parser.parse_args()
    return args.folder, args.transfer_port, args.name

#read local chunks file only
def read_file_by_lines():
    with open(os.path.join(folder_path, 'local_chunks.txt'), 'r') as file:
        lines = file.readlines()
    file.close()
    return lines

#Sending LOCAL_CHUNKS request to tracker
def send_chunks_to_tracker(tracker_socket):
    lines = read_file_by_lines()
    numChunks = None
    for line in lines:
        line = line.strip()
        if line.endswith("LASTCHUNK"):
            numChunks, lastChunk = line.split(',')
            numChunks = int(numChunks)
        else:
            chunk_index, filename = line.split(',')
            local_chunks[int(chunk_index)] = filename
            request = "LOCAL_CHUNKS," + str(chunk_index) + "," + "localhost" + "," + str(transfer_port)
            tracker_socket.sendall(request.encode())
            time.sleep(1)
            logger.info(entity_name + "," + request)

    return numChunks
	
def update_tracker(chunk_index, filename, tracker_socket):
    local_chunks[int(chunk_index)] = filename
    request = "LOCAL_CHUNKS," + str(chunk_index) + "," + "localhost" + "," + str(transfer_port)
    tracker_socket.sendall(request.encode())
    time.sleep(1)
    logger.info(entity_name + "," + request)
    
    
def request_info_from_tracker(chunk_index, tracker_socket):
    request = "WHERE_CHUNK," + str(chunk_index)
    tracker_socket.sendall(request.encode())
    logger.info(entity_name + "," + request)
    time.sleep(1)
    response = tracker_socket.recv(2048).decode()
    time.sleep(1)
    return response

def request_chunks_from_peer(peer_ip, peer_port, chunk_index, filename):
    peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    peer_socket.connect((peer_ip, int(peer_port)))
    request = "REQUEST_CHUNK," + str(chunk_index)
    peer_socket.sendall(request.encode())
    time.sleep(1)

    logger.info(entity_name + "," + request + "," + "localhost" + "," + str(peer_port))
    datasize = struct.unpack('>I', peer_socket.recv(4))[0]
    
    data = b''  # 수신된 데이터를 저장할 변수 초기화
    payloadsize = 0  # 현재까지 수신된 데이터의 크기

    # 전체 페이로드 수신
    while payloadsize <= datasize:
        response = peer_socket.recv(datasize - payloadsize)
        if not response:
            break  # 연결이 닫히거나 에러 발생
        data += response
        payloadsize += len(response)
    
    with open(os.path.join(folder_path, filename), 'wb') as file:
        file.write(data)
        file.flush()
        file.close()
    peer_socket.close() 
   
    
def process_peer(peer_socket):
    while True:
        request = peer_socket.recv(2048).decode()   
        if not request:
            break
        if request.startswith("REQUEST_CHUNK"):
            typeReq, chunk_index = request.split(',')
            filename = None
            if int(chunk_index) in local_chunks.keys():
                filename = local_chunks[int(chunk_index)]
            if filename:
                with open(os.path.join(folder_path, filename), 'rb') as file:
                    while True:
                        chunk = file.read(2048)
                        if not chunk:
                            file.close()
                            break
                        peer_socket.sendall(chunk)
                        time.sleep(1)
            break
    peer_socket.close()
        

def accepting_peers():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    client_socket.bind(("localhost",int(transfer_port))) 
    client_socket.listen(5)

    while True:
        peer_socket, addr = client_socket.accept()
        
        request = peer_socket.recv(2048).decode()   
        if not request:
            break
        if request.startswith("REQUEST_CHUNK"):
            typeReq, chunk_index = request.split(',')
            filename = None
            if int(chunk_index) in local_chunks.keys():
                filename = local_chunks[int(chunk_index)]
                
                filepath = os.path.join(folder_path, filename)

                filesize = os.stat(filepath).st_size
                peer_socket.sendall(struct.pack('>I', filesize))
                
                with open(filepath, 'rb') as file:
                    chunk = file.read(2048)
                    while chunk:
                        peer_socket.sendall(chunk)
                        chunk = file.read(2048)
                        
    peer_socket.close()
    time.sleep(1)
    
def find_missing_chunks():
    tracker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tracker_socket.connect((tracker_ip, tracker_port)) ### 변경
    total_chunk = send_chunks_to_tracker(tracker_socket)
    for i in range(total_chunk):
        if (i + 1) not in local_chunks.keys():
            missing_chunks.append((i+1))

    while len(missing_chunks) > 0:
        ind = missing_chunks.pop(0)
        response = request_info_from_tracker(ind, tracker_socket).split(',')
       
        if response[0] == 'GET_CHUNK_FROM' and len(response) > 3:
            # 첫 번째 피어의 IP 주소와 포트 번호 선택
            peer_ip = response[2]  # 첫 번째 피어의 IP 주소
            try:
                peer_port = int(response[3])  # 첫 번째 피어의 포트 번호
            except ValueError:
                print("Error: Invalid port number.")
                continue  # 유효하지 않은 포트 번호인 경우, 다음 반복으로 넘어갑니다.

            new_file_name = 'chunk_' + str(ind)
            request_chunks_from_peer(peer_ip, peer_port, ind, new_file_name)
            update_tracker(ind, new_file_name, tracker_socket)
        else:
            missing_chunks.append(ind)
    tracker_socket.close()
    


if __name__ == "__main__":
    tempF, tempTP, tempN = parse()
    folder_path = tempF
    transfer_port = tempTP
    entity_name = tempN
    t = threading.Thread(target=accepting_peers, daemon=True)
    t.start()
    find_missing_chunks()
   