# Spark Notebook (Launcher)

[![Build Status](https://travis-ci.org/mas-dse/spark-notebook.svg?branch=master)](https://travis-ci.org/mas-dse/spark-notebook)

Spark Notebook (Launcher) is a web-based interface to install and start
Apache Spark and Jupyter Notebook on Amazon Web Services' Elastic Map Reduce (EMR).

Requires Python 2.7 or 3.6.


## Installation

* Install required packages.

```
pip install -r requirements.txt
```


## Usage

If the required packages are installed in a virtualenv, activate the virtualenv first.

```
source <env_name>/bin/activate
```
1. Run `./run.py`.
2. A browser window will automatically open the URL: `http://localhost:5000`.

Please refer to [docs](docs) for more details.


## Environment

1. Apache Spark 2.1.1 will be installed with [Elastic Map Reduce](https://aws.amazon.com/emr/) 5.6.0.
2. Continuum Anaconda Python 4.4.0 will be installed with Python 2 and Python 3 iPython Kernels.


## Important Notes

Please keep the credentials file (by default, `credentials.yaml`) in a secure
place. Especially, do NOT accidently upload it to any public GitHub repositories.
