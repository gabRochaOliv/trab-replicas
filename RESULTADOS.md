# CAP em um armazenamento replicado: original × melhorado

Trabalho de Sistemas Distribuídos (UENP, Ciência da Computação, 2026)
**Autores:** Gabriel Rocha, Ruan Tirabassi e João Pedro Lima
**Sistema base:** [rgcoelho01/replicas](https://github.com/rgcoelho01/replicas), do Prof. Ricardo G. Coelho

Testamos os três modos de consistência do sistema (`strong`, `eventual` e `ryw`) em três cenários: normal, com uma réplica fora e depois que ela volta. A partir das falhas que encontramos, implementamos **failover**, **retry da replicação** e **reconciliação automática** (no estilo *hinted handoff*). A versão melhorada está na branch `versao-melhorada`.

## Resumo

| Métrica (réplica 3 fora) | Original | Melhorado |
|---|---:|---:|
| Disponibilidade `eventual` | 66,7% | **100%** |
| Disponibilidade `ryw` | 49,1% | **100%** |
| Throughput `eventual` | 21,05 ops/s | **99,32 ops/s** |
| Chaves desatualizadas na R3 depois que ela volta | 285 (permanente) | **0 em ~10 s** |
| Leituras antigas no `ryw` (2.729 leituras) | n/a | **0** |
| Custo sem falha (`eventual`, mesma máquina) | 97,12 ops/s | 95,86 ops/s |

## Arquitetura

```
        YCSB / clientes
              │  HTTP /write, /read
              ▼
      Coordenador :5000
      ┌───────┼───────┐
      ▼       ▼       ▼
   R1 :5001 R2 :5002 R3 :5003
   (JSON)   (JSON)   (JSON)
```

| Modo | Escrita | Leitura |
|---|---|---|
| `strong` | Grava nas 3; só confirma se todas responderem | Lê as 3; responde 409 se divergirem |
| `eventual` | Grava em 1 (round-robin) e propaga em segundo plano | Lê de 1 (round-robin) |
| `ryw` | Grava na réplica fixa do cliente (hash do `X-Client-ID`) e propaga | Lê sempre da réplica fixa |

## Problemas do original → solução

| Problema | Efeito medido | Solução |
|---|---|---|
| Sem failover: cada operação vai para uma única réplica e responde 503 se ela falhar | 333 erros (`eventual`) e 509 erros (`ryw`) em 1.000 ops | **Failover:** tenta a próxima réplica, em ordem fixa |
| Propagação que falha é só impressa e descartada | R3 ficou 285 chaves atrasada | **Retry:** a atualização vira pendência da réplica |
| Réplica que volta serve os dados antigos | 285 → 285 chaves; leitura antiga com HTTP 200 | **Reconciliação:** a réplica recebe as pendências antes de voltar a atender |
| Failover simples quebraria o RYW | n/a | Só lê de uma réplica que já tenha a versão escrita pelo cliente; se nenhuma tiver, responde 503 em vez de um valor antigo |
| `PermissionError` no `os.replace` (Windows) derrubava a conexão | Falhas no `load` (inserts 231 e 857) | Até 5 tentativas (50–200 ms); se persistir, HTTP 500 com a mensagem |
| Nenhuma visibilidade do estado | n/a | `/health` mostra réplicas fora, pendências, fila e contadores |

Ciclo de vida de uma réplica na versão melhorada:

```
ok ──(requisição falha)──► fora ──(/health responde)──► reconciliando ──(pendências = 0)──► ok
```

A API não mudou: as rotas e os códigos HTTP são os mesmos, só foram acrescentados campos às respostas. O modo `strong` não foi alterado.

## Metodologia

| Parâmetro | Valor |
|---|---|
| Ferramenta | YCSB 0.18.0-SNAPSHOT, binding `coordenador` |
| Registros / operações | 1.000 / 1.000 por run |
| Threads | 8 |
| Workload | 50% READ / 50% UPDATE, `uniform`, 1 campo de 100 bytes |
| Coordenador | `--timeout 0.2`, `--replication-delay 1.0`, `--retry-interval 1.0` (melhorado) |
| Falha | Réplica 3 encerrada antes do run |
| Estado inicial | Réplicas idênticas (1.001 chaves), restauradas do mesmo snapshot |

Máquinas: **M1** (Windows 11, Python 3.7.8, Java 21) rodou o `eventual` original e todos os modos da versão melhorada. **M2** (Windows, Python 3.14) rodou o `strong` e o `ryw` originais. Cada cenário foi executado uma vez.

Ferramentas extras: `monitor_convergencia.py` compara os 3 JSONs até convergirem, e `teste_leitura_propria.py` mede se cada cliente lê o que acabou de escrever.

## Resultados YCSB

| Modo | Cenário | Original OK / erros | Original ops/s | Melhorado OK / erros | Melhorado ops/s |
|---|---|---:|---:|---:|---:|
| `strong` | Normal | 1.000 / 0 | 23,72 (M2) | 1.000 / 0 | 26,31 |
| `strong` | R3 fora | 0 / 1.000 | 6,90 (M2) | 0 / 1.000 | 8,16 |
| `eventual` | Normal | 1.000 / 0 | 97,12 | 1.000 / 0 | 95,86 |
| `eventual` | R3 fora | 667 / 333 | 21,05 | **1.000 / 0** | **99,32** |
| `ryw` | Normal | 999 / 1 | 66,79 (M2) | 1.000 / 0 | 90,83 |
| `ryw` | R3 fora | 491 / 509 | 15,15 (M2) | **1.000 / 0** | **92,77** |

> Os números do `strong` e do `ryw` originais vêm da M2. Como o `strong` não mudou, a diferença de ~11% no cenário normal dá uma ideia do efeito do hardware. Para esses modos, compare a **taxa de sucesso**, não o throughput.

### Latência de UPDATE (ms), réplica 3 fora

| Modo | Original (média / p95 / p99) | Melhorado (média / p95 / p99) |
|---|---|---|
| `eventual` | 588,7 / 955,4 / 1.119,2 (ops OK) | **134,7 / 214,4 / 292,6** |
| `ryw` | 805,7 / 1.213,4 / 1.232,9 (ops com falha) | **146,1 / 223,2 / 333,8** |

O UPDATE do `eventual` ficou **4,4× mais rápido**. No original, cada operação que tocava a R3 segurava o lock até o timeout de 0,2 s; na versão melhorada, a réplica marcada como fora deixa de ser tentada.

## Durante a falha (`/health` do melhorado)

| Campo | `eventual` | `ryw` |
|---|---:|---:|
| Failovers de escrita | 166 | 188 |
| Failovers de leitura | 167 | 187 |
| **Total de failovers** | **333** | **375** |
| Pendências da R3 | 379 | 394 |
| Chaves atrasadas na R3 (comparando os JSONs) | 379 | 394 |
| Atualizações reconciliadas | 379 | 394 |

- No `eventual`, os **333 failovers são exatamente os 333 erros do original**: a fração do round-robin que ia para a R3 passou a ser atendida por outra réplica.
- O número de pendências bateu **exatamente** com a divergência medida de forma independente nos JSONs.

## Recuperação da réplica 3

| | Original | Melhorado |
|---|---|---|
| `eventual`: chaves atrasadas ao religar | 285 | 379 |
| `eventual`: depois | **285** (sem mudança em 3 min) | **0 em ~10 s** |
| `ryw`: chaves atrasadas ao religar | réplica volta desatualizada | 394 |
| `ryw`: depois | sem correção | **0 em 9,5 s** |
| Leitura pela R3 | valor antigo com HTTP 200 | valor novo |

Sem falha, a convergência do `eventual` levou praticamente o mesmo tempo nas duas versões (~8 min, por causa do `--replication-delay` de 1 s por item da fila).

## Leitura da própria escrita (versão melhorada)

8 clientes escrevem e leem as próprias chaves durante 30 s. No cenário com falha, a R3 cai aos 10 s e volta aos 20 s.

| Modo | Cenário | Leituras | Antigas + 404 | % |
|---|---|---:|---:|---:|
| `ryw` | Normal | 1.374 | 0 | **0,0** |
| `ryw` | R3 cai e volta | 1.355 | 0 | **0,0** |
| `eventual` | Normal | 1.238 | 826 | 66,7 |
| `eventual` | R3 cai e volta | 1.316 | 865 | 65,7 |

O `ryw` manteve a garantia mesmo durante o failover. No `eventual`, ~2/3 das leituras caem em outra réplica antes da propagação, como esperado.

## `strong`: escrita parcial

O `strong` não foi alterado, e os testes mostraram um problema dele: com a R3 fora, as 481 escritas retornaram 503, mas **389 chaves foram gravadas em R1 e R2** antes da falha (não há rollback). Depois de religar a R3, a leitura das 1.001 chaves deu **612 × 200** e **389 × 409** (réplicas divergentes), e essa divergência não diminui.

## Limitações conhecidas

- **Rajadas de 503:** sob carga, timeouts sucessivos (sem réplica parada) marcaram as 3 réplicas como fora ao mesmo tempo (267 escritas com 503 no teste de leitura própria do `eventual`). Não aconteceu em nenhum run do YCSB. Correção sugerida: tentar as réplicas mesmo quando todas estiverem marcadas como fora.
- **Timeouts ambíguos:** com timeout de 0,2 s, a réplica às vezes grava depois que o coordenador desistiu. A versão melhorada reenvia e reconcilia; o original simplesmente descartava.
- As pendências ficam na memória do coordenador, que continua sendo ponto único de falha.
- Os testes param réplicas; não simulam uma partição de rede propriamente dita.

## Conclusão

O `strong` preservou a consistência na leitura, mas ficou 100% indisponível com uma réplica fora e é ~3,6× mais lento que o `eventual`. No original, os modos assíncronos não cumpriam o que prometiam: não eram totalmente disponíveis e o `eventual` não convergia depois de uma falha. Com failover, retry e reconciliação, o `eventual` e o `ryw` ficaram **100% disponíveis** com uma réplica fora, **convergiram em ~10 s** após a recuperação e o `ryw` teve **0 leituras antigas**, sem custo mensurável no cenário normal.
