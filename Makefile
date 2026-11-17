.PHONY: install test lint synth notebooks mapreduce

install:
	pip install -r requirements.txt

lint:
	ruff check src tests tools

test:
	python -m pytest

synth:
	python tools/make_synthetic.py --out data/sample --users 3000

notebooks:
	python tools/build_notebooks.py

# Smoke-run every notebook on the synthetic sample (BDM_FRACTION=1 because the sample is tiny)
smoke: synth
	BDM_DATA_DIR=data/sample BDM_WORK_DIR=work BDM_FRACTION=1.0 BDM_SPARK_MEM=3g BDM_SHUFFLE_PARTITIONS=8 \
	BDM_CUTOFF=2019-11-10 BDM_BENCH_FRACTIONS=0.5,1.0 BDM_BENCH_CORES=1,2 BDM_BENCH_CORE_FRACTION=0.5 \
	python tools/build_notebooks.py --execute

mapreduce: synth
	hadoop/run_local_mapreduce.sh data/sample/2019-Oct.csv work/mr
