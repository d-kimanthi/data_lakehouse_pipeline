SHELL := /bin/bash

# Load env if present
ifneq (,$(wildcard ./.env))
  include .env
  export
endif

TOPIC?=orders

up:
	docker compose up -d

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=200

create-topic:
	docker compose exec kafka kafka-topics.sh --create --topic $(TOPIC) --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1 || true

list-topics:
	docker compose exec kafka kafka-topics.sh --list --bootstrap-server kafka:9092

produce-sample:
	bash scripts/produce_sample.sh $(TOPIC)

submit-aws:
	bash spark/submit_aws.sh

submit-minio:
	bash spark/submit_minio.sh

sql-aws:
	bash spark/sql_aws.sh

sql-minio:
	bash spark/sql_minio.sh
setup-catalog:
	bash catalog/setup_catalog.sh

.PHONY: up down logs create-topic list-topics produce-sample submit-aws submit-minio sql-aws sql-minio setup-catalog