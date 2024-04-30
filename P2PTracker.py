import socket
import argparse
import threading
import sys
import hashlib
import time
import logging

connections = {}
check_list = []
chunk_list = []

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler('logs.log')
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


#data tuple list entry: ({chunk_index},{ip_addr},{transfer_port})
def manage_Lists():
    while True:
        temp_check_list = list(check_list)  # 작업 중인 리스트를 변경하지 않기 위해 복사본 생성
        for item in temp_check_list:
            # chunk_list에 같은 chunk_index를 가진 항목이 있는지 확인
            duplicate = [chunkItem for chunkItem in chunk_list if chunkItem[0] == item[0]]
            if not duplicate:
                # chunk_list에 항목 추가
                chunk_list.append(item)
                check_list.remove(item)  # 처리된 항목은 check_list에서 제거
            else:
                # 같은 chunk_index를 가진 항목이 이미 chunk_list에 있으면,
                # 해당 항목을 check_list에서 제거 (file_hash 고려 X)
                check_list.remove(item)
        time.sleep(1)  # 무한 루프를 방지하기 위해 잠시 대기

def findChunk(conn, chunk_idx, chunk_list):
    string = "GET_CHUNK_FROM,{}".format(chunk_idx)
    found = False
    for chunk in chunk_list:
        if chunk[0] == chunk_idx:
            found = True
            ip_address, transfer_port = chunk[1], chunk[2]
            string += ",{},{}".format(ip_address, transfer_port)
            logger.info(string)
            
            conn.send(string.encode())
            time.sleep(1)

    if not found:
        conn.send("CHUNK_LOCATION_UNKNOWN,{}".format(chunk_idx).encode())
        logger.info("P2PTracker,CHUNK_LOCATION_UNKNOWN,{}".format(chunk_idx)) ## just string


def runTracker(conn):
    with conn:
        while True:
            data = conn.recv(2048)
            if not data:  # 연결이 끊어진 경우 루프 탈출
                break
            data = data.decode()
            message_list = data.split()
            for message in message_list:
                split_data = message.split(",")
                if split_data[0] == "LOCAL_CHUNKS":
                    # LOCAL_CHUNKS 명령을 받았을 때, file_hash를 제외하고 처리
                    dataTuple = (int(split_data[1]), split_data[2], int(split_data[3]))  # {chunk_index}, {ip_addr}, {transfer_port}
                    chunk_list.append(dataTuple)
                    # 로깅
                    logger.info(message)
                elif split_data[0] == "WHERE_CHUNK":
                    # WHERE_CHUNK 명령을 받았을 때, 해당 청크 인덱스에 대한 정보 찾기
                    findChunk(conn, int(split_data[1]), chunk_list)
                    # 로깅
                    logger.info(message)
            time.sleep(1)
            
def main():
    ##Referenced by MegathreadPA2 TA POST
    s_sock = socket.socket()
    s_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s_sock.bind(('localhost', 5100))

    s_sock.listen()

    while True:
        connection, address = s_sock.accept()
        t = threading.Thread(target=runTracker, args=(connection,))
        t.start()


if __name__ == "__main__":
	main()