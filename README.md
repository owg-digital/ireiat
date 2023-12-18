# README

![Python](https://img.shields.io/badge/python-3.10-blue.svg)
[![License](https://img.shields.io/badge/License-BSD_3--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# Purpose

WIP

# Quick start

You should be able to install the package with `pip install ireiat`.

After which, you can just run `ireiat` from a command-line!

Run `ireiat --help` for CLI options.

# For developers

## Install dependencies

We use `pipenv` for package management and have done development on Windows exclusively.

- Install `pipenv`
- Install runtime libs: `pipenv install`
- Install dev libs: `pipenv install --dev`
- Install pre-commit hooks: `pipenv run pre-commit install`
- Build / commit your `src` and `tests`

Note that `pre-commit` pins versions of these tools with a hard-coded value, but
the versions used can be automatically updated by running:
`pipenv run pre-commit autoupdate`.

## Tests

### Unit tests

We use `pytest` and note that test running has not been included in the pre-commit hooks.
Instead, tests (and test coverage) are expected to be reported as part of a continuous
integration (CI) suite, either as part of a build/deploy pipeline or via actions
in a version control provider.

To run the tests on the command line: `pipenv run pytest`

## Documentation

WIP
