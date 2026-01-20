init:
	uv sync --all-extras --no-install-project

test:
	python -m unittest discover -s tests -v
