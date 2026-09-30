#!/usr/bin/env python3
"""Verifica se cada cliente le a propria ultima escrita (garantia read-your-writes).

Cada cliente (thread, com X-Client-ID proprio) escreve um valor novo numa das suas chaves e,
logo em seguida, le a mesma chave pelo coordenador. Uma leitura com valor diferente do que o
proprio cliente acabou de escrever e contada como "leitura antiga".

Uso: python teste_leitura_propria.py <prefixo> [segundos] [clientes]
"""

import json
import sys
import threading
import time
from collections import Counter
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

URL = "http://127.0.0.1:5000"
PREFIX = sys.argv[1]
SECONDS = float(sys.argv[2]) if len(sys.argv) > 2 else 30
CLIENTS = int(sys.argv[3]) if len(sys.argv) > 3 else 8
KEYS_PER_CLIENT = 10


def call(method, path, client_id, body=None):
    data = json.dumps(body).encode() if body is not None else None
    request = Request(URL + path, data=data, method=method,
                      headers={"Content-Type": "application/json", "X-Client-ID": client_id})
    try:
        with urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read())
    except HTTPError as exc:
        return exc.code, {}
    except (URLError, OSError):
        return 0, {}


stats = Counter()
stale_by_replica = Counter()
lock = threading.Lock()
stop = threading.Event()


def client(number):
    client_id = f"{PREFIX}-cliente-{number}"
    counter = 0
    while not stop.is_set():
        key = f"{PREFIX}:c{number}:k{counter % KEYS_PER_CLIENT}"
        value = f"{client_id}-{counter}"
        counter += 1
        status, _ = call("POST", "/write", client_id, {"chave": key, "valor": value})
        with lock:
            stats[f"escrita_{status}"] += 1
        if status != 200:
            continue
        status, body = call("GET", "/read/" + quote(key, safe=""), client_id)
        with lock:
            stats[f"leitura_{status}"] += 1
            if status == 200 and body.get("valor") != value:
                stats["leitura_antiga"] += 1
                stale_by_replica[body.get("replica_consultada")] += 1
            if body.get("failover"):
                stats["leitura_com_failover"] += 1


threads = [threading.Thread(target=client, args=(n,)) for n in range(CLIENTS)]
started = time.monotonic()
for thread in threads:
    thread.start()
time.sleep(SECONDS)
stop.set()
for thread in threads:
    thread.join()

reads_ok = stats["leitura_200"]
print(f"prefixo={PREFIX} duracao={time.monotonic() - started:.1f}s clientes={CLIENTS}")
print("contagens:", dict(sorted(stats.items())))
if reads_ok:
    print(f"leituras antigas: {stats['leitura_antiga']} de {reads_ok} leituras OK "
          f"({100 * stats['leitura_antiga'] / reads_ok:.1f}%)")
print("leituras antigas por replica:", dict(stale_by_replica))
