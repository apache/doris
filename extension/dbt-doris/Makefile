checkfiles = dbt/adapters test/
py_warn = PYTHONDEVMODE=1

up:
	@poetry update

deps: 
	@poetry install

style: deps
	@isort -src $(checkfiles)
	@black $(checkfiles)

check: deps
	@black --check $(checkfiles) || (echo "Please run 'make style' to auto-fix style issues" && false)
	@pflake8 $(checkfiles)
	@bandit -x tests -r $(checkfiles)
	@mypy $(checkfiles)

test: deps
	$(py_warn) pytest

build:
	@poetry build

ci: check test
