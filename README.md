[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/BK9AX0KL)

# Exercício 11 — RabbitMQ / AMQP (castromir)

Exemplo **mais complexo** que o [RabbitMQ-Example](../RabbitMQ-Example) da aula, aplicando o **mesmo domínio** dos exercícios 9–10 (monitoramento de temperatura com leituras, agregação em janela móvel, persistência e alertas), porém com **RabbitMQ** em vez de Kafka.

Baseado no padrão **produtor–consumidor** com `rabbitpy` (exchange topic, filas duráveis, `ack`).

## Arquitetura

```
sensor_producer_indoor.py  ──rk sensor.indoor──►  sensor.readings.indoor
                                                        │
sensor_producer_outdoor.py ──rk sensor.outdoor─►  sensor.readings.outdoor
                                                        │
                        aggregator_indoor.py / aggregator_outdoor.py
                                                        │
                        rk aggregate.report (exchange temperature.topic)
                                    ├──────────────────┴──────────────────┐
                                    ▼                                     ▼
                    temperature.aggregates.persist          temperature.aggregates.alerts-check
                                    │                                     │
                         persist_consumer.py                    alert_detector.py
                                    │                                     │
                                    ▼                                     ▼
                              SQLite                         temperature.alerts
                                                                         │
                                                                alert_notifier.py
```

### Filas (5 distintas)

| Fila | Produtor(es) | Consumidor(es) | Finalidade |
|------|--------------|----------------|------------|
| `sensor.readings.indoor` | `sensor_producer_indoor.py` | `aggregator_indoor.py` | Leituras brutas zona interna |
| `sensor.readings.outdoor` | `sensor_producer_outdoor.py` | `aggregator_outdoor.py` | Leituras brutas zona externa |
| `temperature.aggregates.persist` | agregadores | `persist_consumer.py` | Gravar agregados em SQLite |
| `temperature.aggregates.alerts-check` | agregadores | `alert_detector.py` | Avaliar limiares e gerar alertas |
| `temperature.alerts` | `alert_detector.py` | `alert_notifier.py` | Simular envio de notificação |

Cada agregado publicado com routing key `aggregate.report` é **entregue em cópia** às duas filas de agregados (padrão pub/sub via bindings AMQP).

Mensagens JSON com campos de negócio (`sensor_id`, `celsius`, `observed_at`, `avg_celsius`, `zone`, etc.) — não são strings de demonstração.

## Pré-requisitos

- Python 3.10+
- Docker **ou** broker RabbitMQ acessível (como no exemplo da aula)

## Broker local (Docker)

```bash
docker compose up -d
```

- AMQP: `localhost:5672`
- Management UI: http://localhost:15672 (usuário `myuser` / senha `abc123`)

Credenciais padrão em `python/const.py` (sobrescrevíveis por variáveis de ambiente).

## Execução

```bash
cd python
pip install -r ../requirements.txt
```

Inicie **sete processos** (terminais separados, todos em `python/`):

| # | Comando | Papel |
|---|---------|--------|
| 1 | `python sensor_producer_indoor.py` | Produtor leituras indoor |
| 2 | `python sensor_producer_outdoor.py` | Produtor leituras outdoor |
| 3 | `python aggregator_indoor.py` | Agregação janela móvel indoor |
| 4 | `python aggregator_outdoor.py` | Agregação janela móvel outdoor |
| 5 | `python persist_consumer.py` | Persistência SQLite |
| 6 | `python alert_detector.py` | Detecção de limiar |
| 7 | `python alert_notifier.py` | Notificação simulada |

Consulta local ao banco (após alguns agregados):

```bash
python query_client.py
```

Variáveis úteis: `RABBITMQ_HOST`, `AGG_WINDOW_HOURS`, `SENSOR_DELTA_C`, `ALERT_THRESHOLD_INDOOR_C`, `ALERT_THRESHOLD_OUTDOOR_C`.

### Broker remoto (aula)

Se usar servidor como na aula, ajuste `RABBITMQ_HOST` e crie usuário/vhost conforme [RabbitMQ-Example](../RabbitMQ-Example/README.md). O script `install_rabbitmq.sh` permanece disponível na raiz para instalação em Debian/Ubuntu.

---

## Comparação: RabbitMQ/AMQP vs Kafka (nesta aplicação)

Os exercícios **9–10** implementam o mesmo fluxo lógico com **Apache Kafka** (tópicos `sensor-readings` e `temperature-aggregates`). Aqui o barramento é **RabbitMQ** com exchange **topic** e filas dedicadas.

### Modelo de mensagens

| Aspecto | Kafka (Ex. 9–10) | RabbitMQ (Ex. 11) |
|---------|------------------|-------------------|
| Unidade | **Tópico** (log append-only, particionado) | **Fila** ligada a **exchange** (mensagem removida após `ack`) |
| Roteamento | Partição + chave opcional; consumer group | **Bindings** e **routing keys** (`sensor.indoor`, `aggregate.report`) |
| Múltiplos assinantes do mesmo evento | Vários **consumer groups** leem o mesmo tópico de forma independente | Várias **filas** com o mesmo binding recebem **cópia** de cada mensagem |
| Histórico | Retenção configurável; replay por offset | Foco em entrega pontual; sem replay nativo após consumo e `ack` |

### Papel de cada componente neste lab

| Função | Kafka | RabbitMQ |
|--------|-------|----------|
| Leituras de sensor | 1 tópico `sensor-readings` | 2 filas (indoor / outdoor) + 2 produtores |
| Agregação | 1 consumidor `aggregator_consumer.py` | 2 agregadores (um por fila de leitura) |
| Persistência + alertas | API consome tópico de agregados; alertas não modelados | 2 filas paralelas a partir do mesmo `aggregate.report` |
| Notificação | — | Fila `temperature.alerts` + worker com latência simulada |

### Desenvolvimento

- **Kafka**: contrato por tópico JSON; sem schema registry neste lab; agregador mantém estado em memória e republica no segundo tópico.
- **RabbitMQ**: topologia explícita (exchange, filas, keys) declarada em `mq_common.py`; encaixa bem **filas por tipo de tarefa** (persistir vs. checar alerta vs. notificar).

### Operação

| Critério | Kafka | RabbitMQ |
|----------|-------|----------|
| Throughput / escala de stream | Forte para alto volume e analytics | Adequado a filas de trabalho e integração entre serviços |
| Garantia de processamento | Offset commit; reprocessamento por grupo | `ack` / `reject(requeue=True)` por mensagem |
| Backpressure | Lag do consumer group | `prefetch` e tamanho da fila no broker |
| Observabilidade | Lag, partições, retenção | Management UI (porta 15672), profundidade de fila |
| Complexidade neste projeto | Menos filas, 2 tópicos | Mais filas, roteamento fino, workers especializados |

### Síntese para este domínio

- **Kafka** é natural quando o fluxo é um **stream contínuo** de leituras e agregados, com possível **replay**, vários consumidores independentes via groups e retenção para análise.
- **RabbitMQ** é natural quando cada mensagem dispara uma **tarefa** com roteamento claro (persistir, avaliar regra, notificar), filas por responsabilidade e sem necessidade de reler o log inteiro.

Nesta aplicação de temperatura, Kafka unifica o canal de leituras; RabbitMQ **separa zonas e pipelines de trabalho** (persistência vs. alerta vs. notificação) com cópias AMQP — padrão típico de **enterprise messaging** e filas de tarefas.

---

## Estrutura

```
python/
  const.py
  mq_common.py
  db_store.py
  sensor_producer_indoor.py
  sensor_producer_outdoor.py
  aggregator_worker.py
  aggregator_indoor.py
  aggregator_outdoor.py
  persist_consumer.py
  alert_detector.py
  alert_notifier.py
  query_client.py
docker-compose.yml
requirements.txt
install_rabbitmq.sh
```
