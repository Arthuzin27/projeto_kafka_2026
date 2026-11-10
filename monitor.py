import json
import os
import sys
from datetime import datetime

from confluent_kafka import OFFSET_BEGINNING, Consumer

BROKER = "localhost:9092"
TOPICO = "transacoes"
GRUPO = "monitor"
ATRASO_DESTAQUE = 3

os.system("")
sys.stdout.reconfigure(encoding="utf-8")
CINZA, AZUL, NEGRITO, RESET = "\033[90m", "\033[94m", "\033[1m", "\033[0m"

do_inicio = "--do-inicio" in sys.argv

consumer = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": GRUPO,
    "auto.offset.reset": "earliest",
    "session.timeout.ms": 10_000,
})


def ao_receber_particoes(consumer, particoes):
    if do_inicio:
        for p in particoes:
            p.offset = OFFSET_BEGINNING
        consumer.assign(particoes)
        print(f"{AZUL}⏪ Voltando ao início das partições {[p.partition for p in particoes]}{RESET}")
    else:
        print(f"{AZUL}▶️  Continuando do último offset salvo nas partições "
              f"{[p.partition for p in particoes]}{RESET}")


consumer.subscribe([TOPICO], on_assign=ao_receber_particoes)
print(f"📊 Monitor (grupo '{GRUPO}') conectado em {BROKER}  (Ctrl+C para parar)\n")

total_msgs = 0
total_valor = 0.0
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Erro: {msg.error()}")
            continue

        t = json.loads(msg.value())
        total_msgs += 1
        total_valor += t["valor"]

        atraso = (datetime.now() - datetime.fromisoformat(t["timestamp"])).total_seconds()
        marca = f"{CINZA}⏮ recuperada ({atraso:.0f}s atrás){RESET}" if atraso > ATRASO_DESTAQUE else ""

        print(f"[partição {msg.partition()} | offset {msg.offset():>4}] "
              f"{t['conta_origem']:>8} → {t['conta_destino']:<8} R$ {t['valor']:>10,.2f}  "
              f"{NEGRITO}#{total_msgs}  total R$ {total_valor:,.2f}{RESET} {marca}")
except KeyboardInterrupt:
    print("\n🛑 Encerrando monitor...")
finally:
    consumer.close()
    print(f"Monitor encerrado. Leu {total_msgs} transações nesta execução.")
