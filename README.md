# EcoGrid-Energy-core

Event-driven microservices demo for energy trading using Flask + RabbitMQ + Docker Compose.

## Overview

This project simulates an energy marketplace with 3 services:

- `meter-service`: receives meter readings and publishes energy events.
- `marketplace-service`: creates and tracks trades from meter events.
- `wallet-service`: processes payments for matched trades.

Messaging is handled through RabbitMQ using topic exchanges and durable queues.

## Architecture

High-level flow:

1. `meter-service` receives a reading via HTTP.
2. It publishes `MeterDataIngested` to `energy_exchange`.
3. `marketplace-service` consumes that event, creates a pending trade, and publishes `TradeMatched`.
4. `wallet-service` consumes `TradeMatched` and either:
	 - publishes `PaymentProcessed` if payment succeeds, or
	 - publishes `PaymentFailed` if wallet does not exist or has insufficient funds.
5. `marketplace-service` consumes wallet events and updates trade status:
	 - `completed` on `PaymentProcessed`
	 - `cancelled` on `PaymentFailed`

## Tech Stack

- Python 3
- Flask
- pika (RabbitMQ client)
- RabbitMQ 3.12 (management image)
- Docker Compose

## Repository Structure

```text
.
|- docker-compose.yml
|- meter-service/
|  |- meter_service.py
|  |- requirements.txt
|  |- Dockerfile
|- marketplace-service/
|  |- marketplace_service.py
|  |- requirements.txt
|  |- Dockerfile
|- wallet-service/
|  |- wallet_service.py
|  |- requirements.txt
|  |- Dockerfile
|- shared/
|  |- event_bus.py
```

## Ports

- meter-service: `http://localhost:5001`
- marketplace-service: `http://localhost:5002`
- wallet-service: `http://localhost:5003`
- RabbitMQ AMQP: `localhost:5672`
- RabbitMQ management UI: `http://localhost:15672` (credentials configured via environment variables)

## Security Note

- This repository is intended for local development/testing.
- Do not use default credentials in shared or production environments.
- Move broker credentials to non-committed environment files (for example, `.env`) when publishing.

## Run with Docker Compose

```bash
docker compose up --build -d
```

Check status:

```bash
docker compose ps
```

Stop everything:

```bash
docker compose down
```

## API Endpoints

### meter-service (`:5001`)

- `GET /health`
- `POST /meter/reading`
- `GET /meter/<meter_id>`
- `GET /meter`

Example:

```bash
curl -X POST http://localhost:5001/meter/reading \
	-H "Content-Type: application/json" \
	-d '{
		"meter_id": "m1",
		"consumption": 12.5,
		"location": "Block A"
	}'
```

### marketplace-service (`:5002`)

- `GET /health`
- `POST /trades`
- `GET /trades`
- `GET /trades/<trade_id>`
- `GET /market/prices`

### wallet-service (`:5003`)

- `GET /health`
- `POST /wallet/create`
- `GET /wallet/<user_id>`
- `POST /wallet/<user_id>/transaction`

Create a wallet for the generated buyer (`user_<meter_id>` pattern):

```bash
curl -X POST http://localhost:5003/wallet/create \
	-H "Content-Type: application/json" \
	-d '{
		"user_id": "user_m1",
		"initial_balance": 1000
	}'
```

## Quick End-to-End Demo

1. Start stack.
2. Create wallet for `user_m1`.
3. Submit meter reading for `meter_id: m1`.
4. Check trades:

```bash
curl http://localhost:5002/trades
```

Expected status transitions:

- `pending` after trade creation/match.
- `completed` after wallet publishes `PaymentProcessed`.
- `cancelled` if wallet publishes `PaymentFailed`.

## Event Exchanges and Event Types

### `energy_exchange`

- `meter.reading.updated`
- `MeterDataIngested`

### `marketplace_exchange`

- `trade.created`
- `TradeMatched`
- `trade.completed`
- `trade.cancelled`

### `wallet_exchange`

- `wallet.created`
- `transaction.recorded`
- `PaymentProcessed`
- `PaymentFailed`

## Chaos Scenario (no extra tools)

Simple resilience test by crashing marketplace process:

```bash
docker compose up -d
docker update --restart always marketplace-service
docker exec marketplace-service sh -c "kill -9 1"
docker inspect -f "status={{.State.Status}} restartCount={{.RestartCount}}" marketplace-service
```

Expected:

- container restarts automatically (`restartCount` increases),
- services continue once dependencies are available.

RabbitMQ outage simulation:

```bash
docker stop rabbitmq
docker compose logs --tail 80 marketplace-service wallet-service
docker start rabbitmq
```

Expected:

- temporary messaging failures while broker is down,
- services recover after broker returns.

## Known Limitations

- In-memory storage (data is lost on restart).
- No authentication/authorization on service APIs.
- Limited retry/backoff and no persistent outbox.
- No automated test suite yet.

## Useful Commands

View recent logs:

```bash
docker compose logs --tail 100 meter-service marketplace-service wallet-service rabbitmq
```

Rebuild and restart:

```bash
docker compose up --build -d
```

Remove stack and networks:

```bash
docker compose down
```
