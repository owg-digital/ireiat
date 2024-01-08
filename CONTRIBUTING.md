# CONTRIBUTING

We are not actively developing the repo but at least one Oliver Wyman team
member can be reached at nicholas.padon@oliverwyman.
com if you have questions
or follow-ups.

If you'd like to do incremental development, please fork the repo.

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

We envision generating the documentation with Sphinx
and publishing online. Have not done that yet...
