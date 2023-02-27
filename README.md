# README

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# Getting started

This repo is meant to represent a "canonical" Python project, where some of the infrastructure
details associated with styling, linting, testing, and test coverage are
handled so you don't have to think about them. We've taken an opinionated (and incomplete!) view of what
"canonical" means - comments and pull requests welcome.

This repo is meant to be a *starting point* for your own repo and is
meant to be forked to save setup time. If you want to use this repo
from BitBucket, click the "Fork" button from the top of the repo and then
modify your forked version.

## Package management

We use [Pipenv](https://packaging.python.org/en/latest/tutorials/managing-dependencies/), currently the recommended
package management solution by the Python packaging authority. See the docs
for details on installing / removing packages.

To install runtime dependencies run `pipenv install`.

To install dev dependencies run `pipen install --dev`.

Note that we do NOT use a `pyproject.toml` file because we have not assumed that
the current library will be packaged (and distributed on, say, PyPi). If this
canonical repo is used as a package, a `pyproject.toml` file (and associated packaging / build machinery) could be
added.

## Pre-commit

We use the [pre-commit](https://pre-commit.com/) package, with code style
and code linting hooks. Once `pipenv` is set up (see above), install
pre-commit hooks by running: `pipenv run pre-commit install`, which
should set up style and linting rules.

### Style

We use [black](https://github.com/psf/black) on pre-commit. "Any color you like."

### Linting

We use [Ruff](https://github.com/charliermarsh/ruff) on pre-commit, the Rust-based linter that's much
faster than `pylint` or `flake8` with comparable coverage.

### Static analysis

At the moment, not using anything but looking into Microsoft's [Pyright](https://github.com/microsoft/pyright)
and [MyPy](https://mypy-lang.org/).

## Tests

### Unit tests

We use `pytest` and note that test running has not been included in the pre-commit hooks.
Instead, tests (and test coverage) are expected to be reported as part of a continuous
integration (CI) suite, either as part of a build/deploy pipeline or via actions
in a version control provider (Bitbucket, GitHub).

To run the tests on the command line: `pipenv run pytest`

### Test coverage

Some options here for integrating with CI but coverage is reported
locally at the moment through [coverage](https://coverage.readthedocs.io/en/7.2.1/)
by first running:
`pipenv run coverage run -m pytest` to generate the coverage data.

Then run `pipenv run coverage report`. There are HTML versions
if needed but these should not be committed to the repo.
