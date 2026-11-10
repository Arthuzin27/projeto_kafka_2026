import json
import os
import sys
from collections import defaultdict, deque
from datetime import datetime

from confluent_kafka import Consumer, Producer

BROKER = "localhost:9092"
TOPICO_ENTRADA = "transacoes"
TOPICO_ALERTAS = "alertas"
GRUPO = "antifraude"
LIMITE_VALOR = 5_000.00
LIMITE_TRANSACOES = 3
JANELA_SEGUNDOS = 10

os.system("")
sys.stdout.reconfigure(encoding="utf-8")
VERDE, VERMELHO, AMARELO, NEGRITO, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[1m", "\033[0m"

consumer = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": GRUPO,
    "auto.offset.reset": "earliest",
    "session.timeout.ms": 10_000,
})
producer = Producer({"bootstrap.servers": BROKER})

historico = defaultdict(deque)


def ao_receber_particoes(consumer, particoes):
    numeros = sorted(p.partition for p in particoes)
    print(f"\n{VERDE}{NEGRITO}{'=' * 60}")
    print(f"🔄 REBALANCE! Esta instância agora lê as partições: {numeros}")
    print(f"{'=' * 60}{RESET}\n")


def ao_perder_particoes(consumer, particoes):
    numeros = sorted(p.partition for p in particoes)
    print(f"\n{AMARELO}⏸️  Devolvendo partições {numeros} para o grupo...{RESET}")


def verificar_fraude(t):
    if t["valor"] > LIMITE_VALOR:
        return f"valor alto: R$ {t['valor']:,.2f} (limite R$ {LIMITE_VALOR:,.2f})"

    agora = datetime.fromisoformat(t["timestamp"]).timestamp()
    horarios = historico[t["conta_origem"]]
    horarios.append(agora)
    while horarios and agora - horarios[0] > JANELA_SEGUNDOS:
        horarios.popleft()

    if len(horarios) > LIMITE_TRANSACOES:
        return f"rajada: {len(horarios)} transações em {JANELA_SEGUNDOS}s"
    return None


consumer.subscribe([TOPICO_ENTRADA],
                   on_assign=ao_receber_particoes,
                   on_revoke=ao_perder_particoes)
print(f"🛡️  Antifraude (grupo '{GRUPO}') aguardando partições...  (Ctrl+C para parar)")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Erro: {msg.error()}")
            continue

        t = json.loads(msg.value())
        motivo = verificar_fraude(t)
        if motivo:
            alerta = {"transacao": t, "motivo": motivo,
                      "detectado_em": datetime.now().isoformat(timespec="seconds")}
            producer.produce(TOPICO_ALERTAS, key=t["conta_origem"],
                             value=json.dumps(alerta, ensure_ascii=False))
            producer.poll(0)
            print(f"{VERMELHO}🚨 ALERTA [p{msg.partition()}] {t['conta_origem']}: {motivo}{RESET}")
        else:
            print(f"✅ ok      [p{msg.partition()}] {t['conta_origem']:>8} R$ {t['valor']:>10,.2f}")
except KeyboardInterrupt:
    print("\n🛑 Encerrando antifraude...")
finally:
    consumer.close()
    producer.flush(5)
    print("Antifraude encerrado.")
