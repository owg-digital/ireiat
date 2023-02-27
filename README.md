# README

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


## Package management

We use [Pipenv](https://packaging.python.org/en/latest/tutorials/managing-dependencies/), currently the recommended
package management solution by the Python packaging authority. See the docs
for details on installing / removing packages.

Note that we do NOT use a `pyproject.toml` file because we have not assumed that
the current library will be packaged (and distributed on, say, PyPi). If this
canonical repo is used as a package, a `pyproject.toml` file could be added.

## Pre-commit

We use the [pre-commit](https://pre-commit.com/) package, with code style
and code linting hooks.

### Style

We use [black](https://github.com/psf/black). "Any color you like."

### Linting

We use [Ruff](https://github.com/charliermarsh/ruff), the Rust-based linter that's much
faster than `pylint` or `flake8` with comparable coverage.
