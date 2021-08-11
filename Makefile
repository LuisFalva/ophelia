.PHONY: help clean clean-build clean-pyc clean-test
.DEFAULT_GOAL := help


# AutoEnv
#ifeq (${BUILD_NUMBER},) # This ensures the CI skips dotenv
#	ENV ?= .env
#	ENV_GEN := $(shell ./.env.gen ${ENV} .env.required)
#	include ${ENV}
#	export $(shell sed 's/=.*//' ${ENV})
#endif


# AutoDoc
define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -rf build dist .eggs .cache
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -rf .tox .coverage htmlcov coverage-reports pylint.txt .pytest_cache

.PHONY: test
test: install-requirements_dev ## run tests quickly with the default Python
	py.test tests

test-all: install-requirements_test artifact ## run tests on every Python version with tox
	tox --installpkg `find dist -name "ophelia*whl"`

install-%:
	pip install -r $*.txt

.PHONY: lint
lint: ## check style with flake8
	pip install flake8
	flake8 --output-file=pylint.txt ophelia

.PHONY: test-coverage
test-coverage: install-requirements_test ## check code coverage
	coverage run --source=ophelia -m pytest tests
	coverage report -m --fail-under 80
	coverage html -d coverage-reports

.PHONY: version
version:
	@echo 0.1.dev0 #version

.PHONY: artifact
artifact:
ifneq (${VERSION},)
	pip install bumpversion
	bumpversion --new-version ${VERSION} --no-commit --allow-dirty --no-tag part
endif
	python setup.py bdist_wheel

.PHONY: install
install:
	pip install .

.PHONY: docs
docs: install-requirements_dev ## generate and shows documentation
	@make -C docs spelling html
	# Replace files with .md extension with .html extension
	@find ./docs/_build/ -name '*.html' -exec sed -i 's/\(\w*\)\.md\(W*\)/\1.html\2/g' {} \;
	@python -m webbrowser -t docs/_build/html/index.html
