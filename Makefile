PYTHON ?= python3
APP_HOST ?= 127.0.0.1
APP_PORT ?= 8765
SMOKE_OUTPUT_DIR ?= /tmp/ady_resolver_demo_smoke

.PHONY: compile js-check unit test smoke app

compile:
	$(PYTHON) -m py_compile src/*.py

js-check:
	node --check src/static/app.js

unit:
	$(PYTHON) -m unittest discover -s tests -v

test: compile js-check unit

smoke:
	rm -rf "$(SMOKE_OUTPUT_DIR)"
	$(PYTHON) src/address_resolver.py \
		--mode predict \
		--eval-dataset-dir examples/demo_reference \
		--model-path models/stage2_model.json \
		--output-dir "$(SMOKE_OUTPUT_DIR)" \
		--compare-variants \
		--jobs 1

app:
	$(PYTHON) src/resolver_app.py --host "$(APP_HOST)" --port "$(APP_PORT)"
