
init:
	uv sync

test:
	python -m unittest discover -s tests -v