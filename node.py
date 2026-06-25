import socket
import time
import sys
from datetime import datetime
from protocol import *

SERVER_HOST  = "127.0.0.1"
SERVER_PORT  = 6000
RESULT_FILE  = "resultado.txt"


def run_node(pid: int, repetitions: int, hold_time: float) -> None:
    """
    Executa o loop de requisição de exclusão mútua para o nodo pid.
    """
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.connect((SERVER_HOST, SERVER_PORT))

    conn.sendall(encode_msg(TYPE_HELLO, pid))

    for seq in range(1, repetitions + 1):

        conn.sendall(encode_msg(TYPE_REQUEST, pid))

        while True:
            raw = conn.recv(MSG_SIZE)
            if not raw:
                raise ConnectionError(f"Nodo {pid}: servidor encerrou a conexão inesperadamente")
            msg_type, target = decode_msg(raw)
            # GRANT recebido, pode entrar na RC
            if msg_type == TYPE_GRANT and target == pid:
                break              

        timestamp = datetime.now().strftime("%H:%M:%S.%f")
        with open(RESULT_FILE, "a") as fp:
            fp.write(f"nodo={pid} ts={timestamp}\n")

        time.sleep(hold_time)

        conn.sendall(encode_msg(TYPE_RELEASE, pid))

    conn.close()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(f"Uso: python {sys.argv[0]} <pid> <repetitions> <hold_time>")
        sys.exit(1)
    run_node(
        pid         = int(sys.argv[1]),
        repetitions = int(sys.argv[2]),
        hold_time   = float(sys.argv[3]),
    )
