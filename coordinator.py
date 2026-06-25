import os
import socket
import threading
import queue
import logging
import itertools
from collections import deque
from protocol import *

HOST = "127.0.0.1"
PORT = 6000

# PID -> socket do cliente conectado
node_sockets: dict[int, socket.socket] = {}

# contador de quantas vezes cada PID entrou na RC
grant_count: dict[int, int] = {}

# lock para proteger acesso a variáveis compartilhadas
state_lock = threading.Lock()

# flag  para encerrar threads
shutdown_flag = threading.Event()

# fila de mensagens recebidas dos clientes 
incoming: queue.Queue = queue.Queue()

# snapshot usado para interface de status
algo_snapshot = {"cs_holder": None, "wait_queue": []}

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/server.log",
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def log_event(msg: str):
    """Registra evento no arquivo de log"""
    logging.info(msg)

# gera IDs sequenciais
request_counter = itertools.count()   # gera IDs sequenciais
# PID -> request_id
request_map: dict[int, int] = {}      

def snapshot(cs_holder, wait_queue):
    """Cria string com estado atual do algoritmo"""
    return f"CS={cs_holder} | Q={list(wait_queue)}"

def send_grant(pid: int):
    """Envia permissão de entrada na região crítica"""
    with state_lock:
        # pega socket do PID
        sock = node_sockets.get(pid)  

    if not sock:
        log_event(f"ERROR | GRANT falhou pid={pid}")
        return

    try:
        sock.sendall(encode_msg(TYPE_GRANT, pid))
        log_event(f"GRANT_SENT | pid={pid}")
    except Exception as e:
        log_event(f"ERROR | GRANT pid={pid}: {e}")

def client_reader(pid: int, sock: socket.socket):
    """Recebe mensagens do cliente e coloca na fila global"""
    while not shutdown_flag.is_set():
        try:
            # recebe mensagem
            raw = sock.recv(MSG_SIZE)  
            if not raw:
                break

            msg_type, sender = decode_msg(raw)

            log_event(f"RECV | {msg_type} | pid={sender}")

            # coloca mensagem na fila do algoritmo
            incoming.put((msg_type, sender))

        except Exception:
            break

    # cliente desconectou
    incoming.put(("DISCONNECT", pid))

    with state_lock:
        node_sockets.pop(pid, None)

    log_event(f"DISCONNECT | pid={pid}")

# THREAD: ACEITA CONEXÕES
def connection_listener(server_sock):
    """Aceita conexões e registra novos processos"""
    while not shutdown_flag.is_set():
        try:
            client_sock, addr = server_sock.accept()
        except OSError:
            break

        try:
            raw = client_sock.recv(MSG_SIZE)
            msg_type, pid = decode_msg(raw)

            # só aceita HELLO inicial
            if msg_type != TYPE_HELLO:
                client_sock.close()
                continue

        except Exception:
            client_sock.close()
            continue

        with state_lock:
            # evita PID duplicado
            if pid in node_sockets:
                log_event(f"ERROR | PID duplicado {pid}")
                client_sock.close()
                continue

            node_sockets[pid] = client_sock
            grant_count.setdefault(pid, 0)

        log_event(f"CONNECT | pid={pid} addr={addr}")

        # cria thread para ler mensagens desse cliente
        threading.Thread(
            target=client_reader,
            args=(pid, client_sock),
            daemon=True
        ).start()


def algorithm_thread():
    """Executa algoritmo centralizado de exclusão mútua"""

    cs_holder = None         
    wait_queue = deque()      

    while not shutdown_flag.is_set():
        try:
            msg_type, pid = incoming.get(timeout=0.5)
        except queue.Empty:
            continue

        # pedido de entrada
        if msg_type == TYPE_REQUEST:
            req_id = next(request_counter)
            request_map[pid] = req_id

            log_event(f"REQUEST | pid={pid} req={req_id} | {snapshot(cs_holder, wait_queue)}")

            # se RC livre, entra direto
            if cs_holder is None:
                cs_holder = pid
                send_grant(pid)
                log_event(f"GRANT | pid={pid} req={req_id} | {snapshot(cs_holder, wait_queue)}")

            # senão,entra na fila
            else:
                if pid not in wait_queue:
                    wait_queue.append(pid)

        # liberação da RC
        elif msg_type == TYPE_RELEASE:
            req_id = request_map.get(pid, -1)

            log_event(f"RELEASE | pid={pid} req={req_id} | {snapshot(cs_holder, wait_queue)}")

            # libera RC se era o dono
            if cs_holder == pid:
                with state_lock:
                    grant_count[pid] += 1
                cs_holder = None

            # se tem fila entao passa para próximo
            if wait_queue:
                next_pid = wait_queue.popleft()
                cs_holder = next_pid

                next_req = request_map.get(next_pid, -1)
                send_grant(next_pid)

                log_event(f"GRANT | pid={next_pid} req={next_req} | {snapshot(cs_holder, wait_queue)}")

        elif msg_type == "DISCONNECT":
            # remove da fila
            wait_queue = deque([p for p in wait_queue if p != pid])

            # se estava na RC
            if cs_holder == pid:
                cs_holder = None

                if wait_queue:
                    next_pid = wait_queue.popleft()
                    cs_holder = next_pid
                    send_grant(next_pid)

        # atualiza snapshot global (para interface)
        with state_lock:
            algo_snapshot["cs_holder"] = cs_holder
            algo_snapshot["wait_queue"] = list(wait_queue)

def interface_thread():
    """Interface do terminal"""
    print("Servidor ativo: status | contagem | sair")

    while True:
        cmd = input(">>> ").strip().lower()

        if cmd == "status":
            with state_lock:
                print("CS:", algo_snapshot["cs_holder"])
                print("Fila:", algo_snapshot["wait_queue"])

        elif cmd == "contagem":
            with state_lock:
                for k in sorted(grant_count):
                    print(f"PID {k}: {grant_count[k]}")

        elif cmd == "sair":
            shutdown_flag.set()

            with state_lock:
                for s in node_sockets.values():
                    try:
                        s.close()
                    except:
                        pass
            break

        else:
            print("Comandos: status | contagem | sair")

def main():
    """Inicializa servidor e threads"""

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((HOST, PORT))
    server_sock.listen(50)

    print(f"Servidor rodando em {HOST}:{PORT}")

    # thread que aceita conexões
    threading.Thread(target=connection_listener, args=(server_sock,), daemon=True).start()

    # thread que executa algoritmo de exclusão mútua
    threading.Thread(target=algorithm_thread, daemon=True).start()

    # interface roda na thread principal
    interface_thread()

    server_sock.close()

if __name__ == "__main__":
    main()
