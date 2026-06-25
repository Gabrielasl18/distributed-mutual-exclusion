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

node_sockets: dict[int, socket.socket] = {}
grant_count: dict[int, int] = {}

state_lock = threading.Lock()
shutdown_flag = threading.Event()

incoming: queue.Queue = queue.Queue()

algo_snapshot = {"cs_holder": None, "wait_queue": []}

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/server.log",
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def log_event(msg: str):
    """Registra eventos no log do coordenador."""
    logging.info(msg)

request_counter = itertools.count()
request_map: dict[int, int] = {}

def snapshot(cs_holder, wait_queue):
    """Cria representação textual do estado do algoritmo."""
    return f"CS={cs_holder} | Q={list(wait_queue)}"

def send_grant(pid: int):
    """Envia permissão de entrada na região crítica ao processo."""
    with state_lock:
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
    """Thread que recebe mensagens de um cliente e coloca na fila."""
    while not shutdown_flag.is_set():
        try:
            raw = sock.recv(MSG_SIZE)
            if not raw:
                break

            msg_type, sender = decode_msg(raw)
            log_event(f"RECV | {msg_type} | pid={sender}")
            incoming.put((msg_type, sender))

        except Exception:
            break

    incoming.put(("DISCONNECT", pid))

    with state_lock:
        node_sockets.pop(pid, None)

    log_event(f"DISCONNECT | pid={pid}")

def connection_listener(server_sock):
    """Aceita novas conexões e registra os processos."""
    while not shutdown_flag.is_set():
        try:
            client_sock, addr = server_sock.accept()
        except OSError:
            break

        try:
            raw = client_sock.recv(MSG_SIZE)
            msg_type, pid = decode_msg(raw)

            if msg_type != TYPE_HELLO:
                client_sock.close()
                continue

        except Exception:
            client_sock.close()
            continue

        with state_lock:
            if pid in node_sockets:
                log_event(f"ERROR | PID duplicado {pid}")
                client_sock.close()
                continue

            node_sockets[pid] = client_sock
            grant_count.setdefault(pid, 0)

        log_event(f"CONNECT | pid={pid} addr={addr}")

        threading.Thread(
            target=client_reader,
            args=(pid, client_sock),
            daemon=True
        ).start()

def algorithm_thread():
    """Implementa o algoritmo centralizado de exclusão mútua."""
    cs_holder = None
    wait_queue = deque()

    while not shutdown_flag.is_set():
        try:
            msg_type, pid = incoming.get(timeout=0.5)
        except queue.Empty:
            continue

        if msg_type == TYPE_REQUEST:
            req_id = next(request_counter)
            request_map[pid] = req_id

            log_event(f"REQUEST | pid={pid} req={req_id} | {snapshot(cs_holder, wait_queue)}")

            if cs_holder is None:
                cs_holder = pid
                send_grant(pid)
                log_event(f"GRANT | pid={pid} req={req_id} | {snapshot(cs_holder, wait_queue)}")
            else:
                if pid not in wait_queue:
                    wait_queue.append(pid)

        elif msg_type == TYPE_RELEASE:
            req_id = request_map.get(pid, -1)

            log_event(f"RELEASE | pid={pid} req={req_id} | {snapshot(cs_holder, wait_queue)}")

            if cs_holder == pid:
                with state_lock:
                    grant_count[pid] += 1
                cs_holder = None

            if wait_queue:
                next_pid = wait_queue.popleft()
                cs_holder = next_pid

                next_req = request_map.get(next_pid, -1)
                send_grant(next_pid)

                log_event(f"GRANT | pid={next_pid} req={next_req} | {snapshot(cs_holder, wait_queue)}")

        elif msg_type == "DISCONNECT":
            wait_queue = deque([p for p in wait_queue if p != pid])

            if cs_holder == pid:
                cs_holder = None
                if wait_queue:
                    next_pid = wait_queue.popleft()
                    cs_holder = next_pid
                    send_grant(next_pid)

        with state_lock:
            algo_snapshot["cs_holder"] = cs_holder
            algo_snapshot["wait_queue"] = list(wait_queue)

def interface_thread():
    """Interface de terminal para monitorar o coordenador."""
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
    """Inicializa servidor e threads do sistema."""
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((HOST, PORT))
    server_sock.listen(50)

    print(f"Servidor rodando em {HOST}:{PORT}")

    threading.Thread(target=connection_listener, args=(server_sock,), daemon=True).start()
    threading.Thread(target=algorithm_thread, daemon=True).start()

    interface_thread()

    server_sock.close()

if __name__ == "__main__":
    main()