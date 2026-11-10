import json
import os
import random
import sys
import time
import uuid
from datetime import datetime

from confluent_kafka import Producer

BROKER = "localhost:9092"
TOPICO = "transacoes"
INTERVALO_MIN = 0.5
INTERVALO_MAX = 1.0
CHANCE_VALOR_ALTO = 0.07
CHANCE_RAJADA = 0.05
TAMANHO_RAJADA = 5

CONTAS = ["ana", "bruno", "carla", "diego", "elisa",
          "fabio", "gabi", "hugo", "isabela", "joao"]
CIDADES = ["São Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba",
           "Salvador", "Recife", "Porto Alegre", "Brasília"]

os.system("")
sys.stdout.reconfigure(encoding="utf-8")
CORES_PARTICAO = ["\033[96m", "\033[95m", "\033[93m"]
RESET = "\033[0m"

producer = Producer({"bootstrap.servers": BROKER})


def nova_transacao(conta_origem, valor):
    destino = random.choice([c for c in CONTAS if c != conta_origem])
    return {
        "id": str(uuid.uuid4())[:8],
        "conta_origem": conta_origem,
        "conta_destino": destino,
        "valor": round(valor, 2),
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "cidade": random.choice(CIDADES),
    }


def confirmacao(erro, msg):
    if erro:
        print(f"❌ Falha ao enviar: {erro}")
        return
    t = json.loads(msg.value())
    cor = CORES_PARTICAO[msg.partition() % 3]
    print(f"📤 {t['conta_origem']:>8} → {t['conta_destino']:<8} "
          f"R$ {t['valor']:>10,.2f}  "
          f"{cor}[partição {msg.partition()} | offset {msg.offset()}]{RESET}")


def enviar(transacao):
    producer.produce(
        TOPICO,
        key=transacao["conta_origem"],
        value=json.dumps(transacao, ensure_ascii=False),
        callback=confirmacao,
    )
    producer.poll(0)


print(f"🚀 Gerador de Pix enviando para o tópico '{TOPICO}' em {BROKER}  (Ctrl+C para parar)\n")
try:
    while True:
        conta = random.choice(CONTAS)
        sorteio = random.random()

        if sorteio < CHANCE_RAJADA:
            print(f"⚡ Injetando RAJADA de {TAMANHO_RAJADA} Pix da conta '{conta}'")
            for _ in range(TAMANHO_RAJADA):
                enviar(nova_transacao(conta, random.uniform(50, 900)))
                time.sleep(0.2)
        elif sorteio < CHANCE_RAJADA + CHANCE_VALOR_ALTO:
            print(f"💰 Injetando Pix de VALOR ALTO da conta '{conta}'")
            enviar(nova_transacao(conta, random.uniform(6_000, 25_000)))
        else:
            enviar(nova_transacao(conta, random.uniform(5, 1_500)))

        time.sleep(random.uniform(INTERVALO_MIN, INTERVALO_MAX))
except KeyboardInterrupt:
    print("\n🛑 Parando o gerador...")
finally:
    producer.flush(5)
    print("Gerador encerrado.")
