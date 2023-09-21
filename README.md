# README

![Python](https://img.shields.io/badge/python-3.10-blue.svg)
[![License](https://img.shields.io/badge/License-BSD_3--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# Purpose

Oliver Wyman proposes developing a model and executable software application, the
Intermodal Routing and Environmental Impact Analysis Tool (IREIAT), that will allow
infrastructure planners to understand expected flows on the US intermodal freight
network given demand, expected intermodal costs, and network constraints.
While some infrastructure planners may have in-house, proprietary tools for
planning, pricing, and network analysis, there is no end-to-end software package
that generates optimized shipping options across the entire intermodal network.
Understanding flows is particularly important when seeking to reduce freight transportation
greenhouse gas (GHG) emissions and congestion, either by increasing the market share of intermodal or deploying new technologies.

The goal of this project will be to provide the target user group (infrastructure planners)
with a tool to understand how cost, transit time, disruptions/resilience, and
GHG emissions affect freight flows across the current and future intermodal network.

At the end of the project, a command-line interface (CLI) tool will be delivered, which
will allow users to solve freight flow problems on the North American intermodal
network (leveraging highway, rail, and waterway modes) given specified origin-destination
demand and flow constraints. Users will be able to examine solutions through text file
outputs summarizing flow paths, utilization metrics, and overall output metrics
($/LCOTKM, $/tCO2e, etc.).

# Quick start

WIP

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
