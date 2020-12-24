# ignite-python-client
Apache Ignite thin (binary protocol) client, written in Python 3.

## Prerequisites

- Python 3.4 or above (3.6, 3.7 and 3.8 are tested),
- Access to Apache Ignite node, local or remote. The current thin client
  version was tested on Apache Ignite 2.7.0 (binary client protocol 1.2.0).

## Installation

#### *for end user*
If you only want to use the `pyignite` module in your project, do:
```
$ pip install pyignite
```

#### *for developer*
If you want to run tests, examples or build documentation, clone
the whole repository:
```
$ git clone git@github.com:apache/ignite-python-thin-client.git
$ pip install -e .
```

This will install the repository version of `pyignite` into your environment
in so-called “develop” or “editable” mode. You may read more about
[editable installs](https://pip.pypa.io/en/stable/reference/pip_install/#editable-installs)
in the `pip` manual.

Then run through the contents of `requirements` folder to install
the additional requirements into your working Python environment using
```
$ pip install -r requirements/<your task>.txt
```

You may also want to consult the `setuptools` manual about using `setup.py`.

## Documentation
[The package documentation](https://apache-ignite-binary-protocol-client.readthedocs.io)
is available at *RTD* for your convenience.

If you want to build the documentation from source, do the developer
installation as described above, then run the following commands:
```
$ cd ignite/modules/platforms/python
$ pip install -r requirements/docs.txt
$ cd docs
$ make html
```

Then open `ignite/modules/platforms/python/docs/generated/html/index.html`
in your browser.

## Examples
Some examples of using pyignite are provided in
`ignite/modules/platforms/python/examples` folder. They are extensively
commented in the
“[Examples of usage](https://apache-ignite-binary-protocol-client.readthedocs.io/en/latest/examples.html)”
section of the documentation.

This code implies that it is run in the environment with `pyignite` package
installed, and Apache Ignite node is running on localhost:10800.

## Testing
*NB!* All tests require Apache Ignite node running on localhost:10800. For the convenience, `docker-compose.yml` is present.
So installing `docker` and `docker-compose` is recommended. Also, it is recommended installing `pyignite` in development
mode. You can do that using following command:
```
$ pip install -e .
```
### Run without ssl
```
$ docker-compose down && docker-compose up -d ignite
$ pytest
```
### Run with examples
```
$ docker-compose down && docker-compose up -d ignite
$ pytest --examples
```
### Run with ssl and not encrypted key
```
$ docker-compose down && docker-compose up -d ignite
$ pytest --use-ssl=True --ssl-certfile=./tests/config/ssl/client_full.pem
```
### Run with ssl and password-protected key
```
$ docker-compose down && docker-compose up -d ignite
$ pytest --use-ssl=True --ssl-certfile=./tests/config/ssl/client_with_pass_full.pem --ssl-keyfile-password=654321
```

If you need to change the connection parameters, see the documentation on
[testing](https://apache-ignite-binary-protocol-client.readthedocs.io/en/latest/readme.html#testing).
