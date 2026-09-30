#!/usr/bin/env python3
"""Gateway HTTP com consistencia strong, eventual ou read-your-writes (RYW)."""

from __future__ import annotations

import argparse
import hashlib
import json
import queue
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, unquote, urlparse
from urllib.request import Request, urlopen


DEFAULT_REPLICAS = (
    "http://127.0.0.1:5001", "http://127.0.0.1:5002", "http://127.0.0.1:5003"
)


class ReplicaUnavailable(Exception):
    pass


@dataclass(frozen=True)
class ReplicaResponse:
    status: int
    body: dict


class ReplicaClient:
    def __init__(self, urls: tuple[str, ...], timeout: float):
        self.urls = tuple(url.rstrip("/") for url in urls)
        self.timeout = timeout

    def _request(self, index: int, path: str, payload: dict | None = None) -> ReplicaResponse:
        data, headers = None, {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = Request(self.urls[index] + path, data=data, headers=headers)
        try:
            with urlopen(request, timeout=self.timeout) as response:
                return ReplicaResponse(response.status, json.loads(response.read()))
        except HTTPError as exc:
            try:
                body = json.loads(exc.read())
            except (json.JSONDecodeError, UnicodeDecodeError):
                body = {"erro": "resposta invalida da replica"}
            return ReplicaResponse(exc.code, body)
        except (URLError, TimeoutError, OSError) as exc:
            raise ReplicaUnavailable(
                f"replica {self.urls[index]} indisponivel: {exc}"
            ) from exc

    def read(self, index: int, key: str) -> ReplicaResponse:
        return self._request(index, "/read/" + quote(key, safe=""))

    def write(self, index: int, key: str, value) -> ReplicaResponse:
        return self._request(index, "/write", {"chave": key, "valor": value})

    def is_healthy(self, index: int) -> bool:
        try:
            return self._request(index, "/health").status == 200
        except ReplicaUnavailable:
            return False


class Coordinator:
    def __init__(self, mode: str, replicas: tuple[str, ...], timeout: float, delay: float,
                 retry_interval: float):
        self.mode = mode
        self.client = ReplicaClient(replicas, timeout)
        self.delay = delay
        self.retry_interval = retry_interval
        self._round_robin = 0
        self._state_lock = threading.Lock()
        # Mantem a ordem das escritas durante a propagacao assincrona.
        self._write_lock = threading.Lock()
        self._versions: dict[str, int] = {}
        self._replication_queue: queue.Queue[tuple[str, object, int, int] | None] = queue.Queue()
        # Estado de failover, retry e reconciliacao (usado pelos modos eventual e ryw).
        replica_count = len(self.client.urls)
        # Replicas marcadas como fora: nao recebem operacoes ate serem reconciliadas.
        self._down: set[int] = set()
        # Atualizacoes que cada replica perdeu: chave -> (valor, versao). So a ultima importa.
        self._pending: list[dict[str, tuple[object, int]]] = [{} for _ in range(replica_count)]
        # Ultima versao de cada chave que cada replica confirmou.
        self._applied: list[dict[str, int]] = [{} for _ in range(replica_count)]
        # RYW: ultima versao que cada cliente escreveu em cada chave.
        self._client_versions: dict[str, dict[str, int]] = {}
        self.counters = {
            "replicas_marcadas_fora": 0, "failovers_escrita": 0, "failovers_leitura": 0,
            "propagacoes_adiadas": 0, "atualizacoes_reconciliadas": 0, "reconciliacoes": 0,
        }
        self._stop = threading.Event()
        self._worker = threading.Thread(target=self._replicate_worker, daemon=True)
        self._worker.start()
        self._recovery = threading.Thread(target=self._recovery_worker, daemon=True)
        self._recovery.start()

    def close(self) -> None:
        self._stop.set()
        self._replication_queue.put(None)

    def _count(self, name: str, amount: int = 1) -> None:
        with self._state_lock:
            self.counters[name] += amount

    def _is_down(self, index: int) -> bool:
        with self._state_lock:
            return index in self._down

    def _mark_down(self, index: int, reason: str) -> None:
        with self._state_lock:
            if index in self._down:
                return
            self._down.add(index)
            self.counters["replicas_marcadas_fora"] += 1
        print(f"[falha] replica {self.client.urls[index]} marcada como fora ({reason}); "
              "operacoes vao para as outras replicas")

    def _mark_applied(self, index: int, key: str, version: int) -> None:
        # Chamado com _write_lock: a replica confirmou esta versao da chave.
        self._applied[index][key] = version
        pending = self._pending[index].get(key)
        if pending is not None and pending[1] <= version:
            del self._pending[index][key]

    def _candidates(self, first: int) -> list[int]:
        # Replica preferida primeiro; depois as outras, sempre na mesma ordem.
        count = len(self.client.urls)
        return [(first + offset) % count for offset in range(count)]

    def status(self) -> dict:
        with self._state_lock:
            down = set(self._down)
            counters = dict(self.counters)
        replicas = {
            url: {"estado": "fora" if index in down else "ok",
                  "pendentes": len(self._pending[index])}
            for index, url in enumerate(self.client.urls)
        }
        return {"replicas": replicas, "fila_replicacao": self._replication_queue.qsize(),
                "contadores": counters}

    def _replicate_worker(self) -> None:
        while True:
            task = self._replication_queue.get()
            if task is None:
                self._replication_queue.task_done()
                return
            key, value, source, version = task
            if self.delay:
                time.sleep(self.delay)
            with self._write_lock:
                # Uma propagacao atrasada nunca pode sobrescrever uma escrita mais nova.
                if self._versions.get(key) != version:
                    self._replication_queue.task_done()
                    continue
                for index in range(len(self.client.urls)):
                    if index == source:
                        continue
                    self._propagate(index, key, value, version)
            self._replication_queue.task_done()

    def _propagate(self, index: int, key: str, value, version: int) -> None:
        # Chamado com _write_lock. Se a replica estiver fora, a atualizacao fica
        # pendente para ser reenviada quando ela voltar (em vez de ser descartada).
        if not self._is_down(index):
            try:
                response = self.client.write(index, key, value)
                if response.status == 200:
                    self._mark_applied(index, key, version)
                    return
                reason = f"status {response.status}"
            except ReplicaUnavailable as exc:
                reason = str(exc)
            self._mark_down(index, f"replicacao assincrona: {reason}")
        self._pending[index][key] = (value, version)
        self._count("propagacoes_adiadas")

    def _recovery_worker(self) -> None:
        # Retry: a cada intervalo, testa as replicas fora; se responderem, reconcilia.
        while not self._stop.wait(self.retry_interval):
            with self._state_lock:
                down = sorted(self._down)
            for index in down:
                if self.client.is_healthy(index):
                    self._reconcile(index)

    def _reconcile(self, index: int) -> None:
        # Reenvia as atualizacoes pendentes, uma por vez. Escritas novas continuam
        # entrando como pendentes, porque a replica so e liberada quando nada faltar.
        url = self.client.urls[index]
        print(f"[reconciliacao] replica {url} respondeu /health; "
              f"reenviando {len(self._pending[index])} atualizacoes pendentes")
        started, sent = time.monotonic(), 0
        while True:
            with self._write_lock:
                pending = self._pending[index]
                if not pending:
                    with self._state_lock:
                        self._down.discard(index)
                    break
                key, (value, version) = next(iter(pending.items()))
                try:
                    response = self.client.write(index, key, value)
                    reason = f"status {response.status}"
                except ReplicaUnavailable as exc:
                    response, reason = None, str(exc)
                if response is None or response.status != 200:
                    print(f"[reconciliacao] replica {url} falhou de novo ({reason}); "
                          f"{len(pending)} pendentes, nova tentativa em {self.retry_interval}s")
                    self._count("atualizacoes_reconciliadas", sent)
                    return
                del pending[key]
                self._applied[index][key] = version
                sent += 1
        self._count("atualizacoes_reconciliadas", sent)
        self._count("reconciliacoes")
        print(f"[reconciliacao] replica {url} sincronizada: {sent} atualizacoes em "
              f"{time.monotonic() - started:.1f}s; voltou a receber operacoes")

    def _next_replica(self) -> int:
        with self._state_lock:
            index = self._round_robin % len(self.client.urls)
            self._round_robin += 1
            return index

    def _client_replica(self, client_id: str) -> int:
        digest = hashlib.sha256(client_id.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big") % len(self.client.urls)

    def write(self, key: str, value, client_id: str) -> tuple[int, dict]:
        if self.mode == "strong":
            succeeded, errors = [], []
            with self._write_lock:
                for index, url in enumerate(self.client.urls):
                    try:
                        response = self.client.write(index, key, value)
                        if response.status == 200:
                            succeeded.append(url)
                        else:
                            errors.append({"replica": url, "status": response.status})
                    except ReplicaUnavailable as exc:
                        errors.append({"replica": url, "erro": str(exc)})
            if errors:
                return 503, {
                    "erro": "nao foi possivel confirmar a escrita em todas as replicas",
                    "replicas_atualizadas": succeeded, "falhas": errors, "modo": self.mode,
                }
            return 200, {
                "mensagem": "valor gravado", "chave": key, "valor": value,
                "replicas_confirmadas": len(succeeded), "modo": self.mode,
            }

        first = (self._client_replica(client_id) if self.mode == "ryw"
                 else self._next_replica())
        unavailable = []
        with self._write_lock:
            # Failover: se a replica escolhida estiver fora, tenta a proxima.
            for source in self._candidates(first):
                url = self.client.urls[source]
                if self._is_down(source):
                    unavailable.append(url)
                    continue
                try:
                    response = self.client.write(source, key, value)
                except ReplicaUnavailable as exc:
                    self._mark_down(source, str(exc))
                    unavailable.append(url)
                    continue
                if response.status >= 500:
                    self._mark_down(source, f"status {response.status}")
                    unavailable.append(url)
                    continue
                if response.status != 200:
                    return 502, {"erro": "a replica recusou a escrita", "detalhes": response.body}
                break
            else:
                return 503, {"erro": "nenhuma replica disponivel para a escrita",
                             "replicas_indisponiveis": unavailable, "modo": self.mode}
            version = self._versions.get(key, 0) + 1
            self._versions[key] = version
            self._mark_applied(source, key, version)
            if self.mode == "ryw":
                self._client_versions.setdefault(client_id, {})[key] = version
            self._replication_queue.put((key, value, source, version))
        body = {
            "mensagem": "valor gravado", "chave": key, "valor": value,
            "replica_confirmada": self.client.urls[source],
            "propagacao": "assincrona", "modo": self.mode,
        }
        if unavailable:
            self._count("failovers_escrita")
            body.update(failover=True, replicas_indisponiveis=unavailable)
        return 200, body

    def read(self, key: str, client_id: str) -> tuple[int, dict]:
        if self.mode == "strong":
            responses = []
            for index, url in enumerate(self.client.urls):
                try:
                    responses.append((url, self.client.read(index, key)))
                except ReplicaUnavailable as exc:
                    return 503, {"erro": str(exc), "modo": self.mode}
            if all(response.status == 404 for _, response in responses):
                return 404, {"erro": "chave nao encontrada", "chave": key, "modo": self.mode}
            if any(response.status != 200 for _, response in responses):
                return 409, {"erro": "replicas divergentes", "chave": key, "modo": self.mode}
            values = [response.body.get("valor") for _, response in responses]
            if any(value != values[0] for value in values[1:]):
                return 409, {
                    "erro": "replicas divergentes", "chave": key,
                    "valores": values, "modo": self.mode,
                }
            return 200, {
                "chave": key, "valor": values[0], "modo": self.mode,
                "replicas_consultadas": len(responses),
            }

        first = (self._client_replica(client_id) if self.mode == "ryw"
                 else self._next_replica())
        # RYW: a replica lida precisa ter pelo menos a ultima escrita deste cliente.
        min_version = (self._client_versions.get(client_id, {}).get(key, 0)
                       if self.mode == "ryw" else 0)
        unavailable, lagging = [], []
        # Failover: se a replica escolhida estiver fora, tenta a proxima.
        for index in self._candidates(first):
            url = self.client.urls[index]
            if self._is_down(index):
                unavailable.append(url)
                continue
            if self._applied[index].get(key, 0) < min_version:
                lagging.append(url)
                continue
            try:
                response = self.client.read(index, key)
            except ReplicaUnavailable as exc:
                self._mark_down(index, str(exc))
                unavailable.append(url)
                continue
            if response.status >= 500:
                self._mark_down(index, f"status {response.status}")
                unavailable.append(url)
                continue
            break
        else:
            body = {"erro": "nenhuma replica disponivel para a leitura",
                    "replicas_indisponiveis": unavailable, "modo": self.mode}
            if lagging:
                body["replicas_sem_a_escrita_do_cliente"] = lagging
            return 503, body
        body = dict(response.body)
        body["modo"] = self.mode
        body["replica_consultada"] = self.client.urls[index]
        if unavailable or lagging:
            self._count("failovers_leitura")
            body["failover"] = True
            body["replicas_indisponiveis"] = unavailable
            if lagging:
                body["replicas_sem_a_escrita_do_cliente"] = lagging
        return response.status, body


def make_handler(coordinator: Coordinator):
    class CoordinatorHandler(BaseHTTPRequestHandler):
        server_version = "CoordenadorHTTP/1.0"

        def _json(self, status: int, body: dict) -> None:
            encoded = json.dumps(body, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def _client_id(self) -> str:
            return self.headers.get("X-Client-ID", self.client_address[0]).strip()

        def _read_key(self) -> str | None:
            parsed = urlparse(self.path)
            if parsed.path.startswith("/read/"):
                return unquote(parsed.path[len("/read/"):]).strip()
            if parsed.path == "/read":
                return parse_qs(parsed.query).get("chave", [""])[0].strip()
            return None

        def do_GET(self) -> None:  # noqa: N802
            if urlparse(self.path).path == "/health":
                body = {"status": "ok", "servico": "coordenador", "modo": coordinator.mode}
                if coordinator.mode != "strong":
                    body.update(coordinator.status())
                self._json(200, body)
                return
            key = self._read_key()
            if key is None:
                self._json(404, {"erro": "rota nao encontrada"})
            elif not key:
                self._json(400, {"erro": "informe a chave"})
            else:
                status, body = coordinator.read(key, self._client_id())
                self._json(status, body)

        def do_POST(self) -> None:  # noqa: N802
            if urlparse(self.path).path != "/write":
                self._json(404, {"erro": "rota nao encontrada"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                payload = json.loads(self.rfile.read(length))
            except (ValueError, json.JSONDecodeError):
                self._json(400, {"erro": "corpo JSON invalido"})
                return
            if not isinstance(payload, dict):
                self._json(400, {"erro": "o corpo deve ser um objeto JSON"})
                return
            key = payload.get("chave")
            if not isinstance(key, str) or not key.strip():
                self._json(400, {"erro": "'chave' deve ser uma string nao vazia"})
                return
            if "valor" not in payload:
                self._json(400, {"erro": "campo 'valor' obrigatorio"})
                return
            status, body = coordinator.write(key.strip(), payload["valor"], self._client_id())
            self._json(status, body)

        def log_message(self, format: str, *args) -> None:
            print(f"[coordenador/{coordinator.mode}] {self.address_string()} - {format % args}")

    return CoordinatorHandler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inicia o coordenador chave/valor")
    parser.add_argument("--mode", required=True, choices=("strong", "eventual", "ryw"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--replicas", nargs="+", default=list(DEFAULT_REPLICAS))
    parser.add_argument("--timeout", type=float, default=2.0)
    parser.add_argument("--replication-delay", type=float, default=1.0,
                        help="atraso da propagacao assincrona, em segundos")
    parser.add_argument("--retry-interval", type=float, default=1.0,
                        help="intervalo para testar replicas fora e reconcilia-las, em segundos")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    coordinator = Coordinator(args.mode, tuple(args.replicas), args.timeout,
                              max(0, args.replication_delay), max(0.1, args.retry_interval))
    server = ThreadingHTTPServer((args.host, args.port), make_handler(coordinator))
    print(f"Coordenador em http://{args.host}:{args.port} (modo: {args.mode})")
    print("Replicas: " + ", ".join(args.replicas))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nEncerrando coordenador...")
    finally:
        server.server_close()
        coordinator.close()


if __name__ == "__main__":
    main()
