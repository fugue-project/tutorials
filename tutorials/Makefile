.PHONY: help clean dev docs package test

help:
	@echo "The following make targets are available:"
	@echo "	 rundev		docker build and start the jupyter notebook mounting the current directory for development"

rundev:
	docker build -t fuguetutorial:latest .
	docker run -p 8888:8888 -v $(PWD):/home/vscode/work fuguetutorial:latest jupyter notebook --port=8888 --ip=0.0.0.0 --no-browser --allow-root 

dev:
	pip3 install -r requirements.txt

olddocs:
	rm -rf docs/tutorials
	rm -rf docs/images
	rm -rf docs/build
	rm docs/README.ipynb
	cp README.ipynb docs/
	cp -r tutorials/ docs/tutorials
	cp -r images/ docs/images
	rm -rf docs/tutorials/.ipynb_checkpoints
	rm -rf docs/tutorials/dask-worker-space
	rm -rf docs/tutorials/spark-warehouse
	python -m sphinx docs/ docs/build

docs:
	rm -rf docs/build
	rm -rf tutorials/.ipynb_checkpoints
	rm -rf tutorials/dask-worker-space
	rm -rf tutorials/spark-warehouse
	python -m sphinx ./ docs/build

jdocs:
	jupyter-book build .
