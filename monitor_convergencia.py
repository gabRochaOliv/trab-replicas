#!/usr/bin/env python3
"""Acompanha a convergencia das replicas (so le os JSONs e o /health; nao escreve nada).

Uso: python monitor_convergencia.py [intervalo_em_segundos]
Para sozinho quando as 3 replicas ficam identicas e a fila do coordenador esvazia.
"""

import json
import sys
import time
from datetime import datetime
from urllib.request import urlopen

INTERVAL = float(sys.argv[1]) if len(sys.argv) > 1 else 30.0
FILES = [f"data/replica{i}.json" for i in (1, 2, 3)]


def load(path):
    # O JSON e trocado de forma atomica pela replica; se estiver bloqueado, tenta de novo.
    for _ in range(10):
        try:
            with open(path, encoding="utf-8") as file:
                return json.load(file)
        except (OSError, json.JSONDecodeError):
            time.sleep(0.1)
    raise RuntimeError(f"nao foi possivel ler {path}")


def health():
    try:
        with urlopen("http://127.0.0.1:5000/health", timeout=2) as response:
            return json.loads(response.read())
    except OSError:
        return None


start = time.monotonic()
print("hora     | decorrido | divergentes | R1xR2 | R3 atrasada vs R1 | fila | pendentes R1/R2/R3 | estados")
while True:
    r1, r2, r3 = (load(path) for path in FILES)
    keys = set(r1) | set(r2) | set(r3)
    divergent = sum(1 for k in keys if not (r1.get(k) == r2.get(k) == r3.get(k)))
    r1_r2 = sum(1 for k in keys if r1.get(k) != r2.get(k))
    r3_r1 = sum(1 for k in keys if r3.get(k) != r1.get(k))
    h = health()
    if h and "replicas" in h:
        replicas = list(h["replicas"].values())
        fila = h["fila_replicacao"]
        pend = "/".join(str(r["pendentes"]) for r in replicas)
        estados = "/".join(r["estado"] for r in replicas)
    else:
        fila, pend, estados = "?", "?", "coordenador sem resposta"
    elapsed = int(time.monotonic() - start)
    print(f"{datetime.now():%H:%M:%S} | {elapsed // 60:>3}m{elapsed % 60:02d}s  | {divergent:>11} | "
          f"{r1_r2:>5} | {r3_r1:>17} | {fila:>4} | {pend:>18} | {estados}", flush=True)
    if divergent == 0 and fila == 0:
        print(f"CONVERGIU: {len(keys)} chaves, 3 replicas identicas.")
        break
    time.sleep(INTERVAL)
