.PHONY: help clean dev docs package test

help:
	@echo "The following make targets are available:"
	@echo "	 rundev		docker build and start the jupyter notebook mounting the current directory for development"

rundev:
	docker build -t fuguetutorial:latest .
	docker run -p 8888:8888 -v $(PWD):/home/jovyan/work fuguetutorial:latest jupyter notebook