# Pset Final

[![Build Status](https://travis-ci.com/csci-e-29/2020fa-pset-final-ckevinhill.svg?branch=master)](https://travis-ci.com/github/Pset Final/2020fa-pset-final-ckevinhill)

[![Maintainability](https://api.codeclimate.com/v1/badges/xxx/maintainability)](https://codeclimate.com/repos/xxx/maintainability)

[![Test Coverage](https://api.codeclimate.com/v1/badges/xxx/test_coverage)](https://codeclimate.com/repos/xxx/test_coverage)

## Objectives


<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*


<!-- END doctoc generated TOC please keep comment here to allow auto update -->

#### Installation

If you are using the Docker environment, you should be good to go.  Mac/windows
users should [install
pipenv](https://pipenv.readthedocs.io/en/latest/#install-pipenv-today) into
their main python environment as instructed.  If you need a new python 3.7
environment, you can use a base
[conda](https://docs.conda.io/en/latest/miniconda.html) installation.

```bash
# Optionally create a new base python 3.7
conda create -n py37 python=3.7
conda activate py37
pip install pipenv
pipenv install ...
```

```bash
pipenv install --dev
pipenv run python some_python_file
```

If you get a TypeError, see [this
issue](https://github.com/pypa/pipenv/issues/3363)

#### Usage

Rather than `python some_file.py`, you should run `pipenv run python
some_file.py` or `pipenv shell` etc

***Never*** pip install something!  Instead you should `pipenv install pandas`
or `pipenv install --dev pytest`.  Use `--dev` if your app only needs the
dependency for development, not to actually do it's job.

Be sure to commit any changes to your [Pipfile](./Pipfile) and
[Pipfile.lock](./Pipfile.lock)!

##### Pipenv inside docker

Because of the way docker freezes the operating system, installing a new package
within docker is a two-step process:

```bash
docker-compose build

# Now i want a new thing
./drun_app pipenv install pandas # Updates pipfile, but does not rebuild image
# running ./drun_app python -c "import pandas" will fail!

# Rebuild
docker-compose build
./drun_app python -c "import pandas" # Now this works
```

#### Set the Travis environment variables

This is Advanced Python for Data Science, so of course we also want to automate
our tests and pset using CI/CD.  Unfortunately, we can't upload our .env or run
`aws configure` and interactively enter the credentials for the Travis builds,
so we have to configure Travis to use the access credentials without
compromising the credentials in our git repo.

We've provided a working `.travis.yml` configuration that only requires the AWS
credentials when running on the master branch, but you will still need to the
final step of adding the variables for your specific pset repository.

To add environment variables to your Travis environment, you can use of the
following options:

* Navigating to the settings, eg https://travis-ci.com/csci-e-29/YOUR_PSET_REPO/settings
* The [Travis CLI](https://github.com/travis-ci/travis.rb)
* encrypting into the `.travis.yml` as instructed [here](https://docs.travis-ci.com/user/environment-variables/#defining-encrypted-variables-in-travisyml).

Preferably, you should only make your 'prod' credentials available on your
master branch: [Travis
Settings](https://docs.travis-ci.com/user/environment-variables/#defining-variables-in-repository-settings)

You can chose the method you think is most appropriate.  Our only requirement is
that ***THE KEYS SHOULD NOT BE COMMITTED TO YOUR REPO IN PLAIN TEXT ANYWHERE***.

For more information, check out the [Travis Documentation on Environment
Variables](https://docs.travis-ci.com/user/environment-variables/)

__*IMPORTANT*__: If you find yourself getting stuck or running into issues,
please post on Piazza and ask for help.  We've provided most of the instructions
necessary for this step and do not want you spinning your wheels too long just
trying to download the data.

### __Main__

