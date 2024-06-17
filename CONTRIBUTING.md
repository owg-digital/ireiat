# Contributing

We are not actively developing the repo GitHub issues can be raised
if you have questions or follow-ups.

If you'd like to do incremental development, please fork the repo and create
a pull request from your feature branch into this repo.

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

### Code coverage

To run code coverage tests, run `pipenv run coverage run -m pytest`.

To get a code coverage report, run `pipenv run coverage report` (after running the above
coverage command)

## Documentation

Our documentation is published automatically via a GitHub
hook into [ReadTheDocs](https://ireiat.readthedocs.io/en/latest/).
