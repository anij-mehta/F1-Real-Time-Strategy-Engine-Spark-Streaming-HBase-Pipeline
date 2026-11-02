COMPOSE := docker compose
.PHONY: export-data reset up down init stream train-model submit verify-hbase evaluate dashboard logs

reset:
	$(COMPOSE) --profile tools down -v
up:
	$(COMPOSE) up -d --build
down:
	$(COMPOSE) down
init:
	$(COMPOSE) --profile tools run --rm init
stream:
	$(COMPOSE) --profile tools up --build streamer
export-data:
	$(COMPOSE) --profile tools run --rm export-training-data
train-model: init export-data
	$(COMPOSE) --profile tools run --rm --entrypoint /opt/spark/bin/spark-submit spark-submit /opt/spark/work-dir/train_pit_model.py --report-path /opt/f1/docs/model_report.md
submit: init
	$(COMPOSE) --profile tools run --rm spark-submit
verify-hbase:
	$(COMPOSE) --profile tools run --rm --entrypoint python3 spark-submit /opt/spark/work-dir/verify_hbase.py
EVAL_SPEED ?= 1
evaluate: init
	python evaluate.py --speed $(EVAL_SPEED)
dashboard:
	$(COMPOSE) up -d --build dashboard
logs:
	$(COMPOSE) logs --tail=100 -f namenode datanode hbase spark-master spark-worker
