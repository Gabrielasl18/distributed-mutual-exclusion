import time
from multiprocessing import Process
from datetime import datetime
from node import run_node, RESULT_FILE


def launch_experiment(n: int, r: int, k: float) -> None:
    """
    Inicia n nodos em paralelo, cada um acessando a RC r vezes
    com tempo de permanência k segundos.
    """
    open(RESULT_FILE, "w").close()

    print(f"\n{'='*50}")
    print(f"  Experimento iniciado")
    print(f"  Nodos (n)          : {n}")
    print(f"  Repetições (r)     : {r}")
    print(f"  Tempo na RC (k)    : {k}s")
    print(f"  Linhas esperadas   : {n * r}")
    print(f"{'='*50}")

    workers: list[Process] = []
    for pid in range(1, n + 1):
        p = Process(target=run_node, args=(pid, r, k), name=f"nodo-{pid}")
        p.start()
        workers.append(p)

    for p in workers:
        p.join()

    print("\nTodos os nodos finalizaram.\n")
    validate_result(n, r)


def validate_result(n: int, r: int) -> None:
    """
    Verifica a corretude de resultado.txt:
      ✓ Número total de linhas == n * r
      ✓ Timestamps em ordem não-decrescente (relógio do sistema)
      ✓ Cada nodo escreveu exatamente r vezes
    """
    print(f"{'─'*50}")
    print("  VALIDAÇÃO DO RESULTADO")
    print(f"{'─'*50}")

    try:
        with open(RESULT_FILE, "r") as f:
            lines = [ln.strip() for ln in f if ln.strip()]
    except FileNotFoundError:
        print(f"  [ERRO] Arquivo '{RESULT_FILE}' não encontrado.")
        return

    expected = n * r
    total_ok = len(lines) == expected
    status   = "✓" if total_ok else "✗"
    print(f"  {status} Linhas: {len(lines)} encontradas / {expected} esperadas")

    access_counts: dict[str, int] = {}
    prev_ts  = datetime.min
    order_ok = True

    for line in lines:
        try:
            fields   = dict(item.split("=") for item in line.split())
            nodo_key = fields["nodo"]
            ts       = datetime.strptime(fields["ts"], "%H:%M:%S.%f")
        except Exception:
            print(f"  [AVISO] Linha malformada ignorada: {line!r}")
            continue

        access_counts[nodo_key] = access_counts.get(nodo_key, 0) + 1

        if ts < prev_ts:
            print(f"  ✗ Violação de ordem cronológica: {line}")
            order_ok = False
        prev_ts = ts

    chr_status = "✓" if order_ok else "✗"
    print(f"  {chr_status} Ordem cronológica dos timestamps")

    print(f"\n  Acessos registrados por nodo (esperado: {r} cada):")
    all_counts_ok = True
    for nodo_key in sorted(access_counts, key=lambda x: int(x)):
        count  = access_counts[nodo_key]
        ok     = count == r
        sym    = "✓" if ok else "✗"
        print(f"    {sym} Nodo {nodo_key}: {count}/{r}")
        if not ok:
            all_counts_ok = False

    for pid in range(1, n + 1):
        if str(pid) not in access_counts:
            print(f"    ✗ Nodo {pid}: 0/{r}  ← nenhuma entrada!")
            all_counts_ok = False

    print(f"\n{'─'*50}")
    result = "APROVADO" if (total_ok and order_ok and all_counts_ok) else "REPROVADO"
    print(f"  Resultado final: {result}")
    print(f"{'─'*50}\n")


if __name__ == "__main__":
    time.sleep(1)
    launch_experiment(n=5, r=3, k=1)
