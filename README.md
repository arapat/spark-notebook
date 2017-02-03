# Spark Notebook (Launcher)

Spark Notebook (Launcher) is a web-based interface to install and start
Apache Spark and Jupyter Notebook on Amazon Web Services.

Requires Python 3.4 or newer.


## Installation

It is a good idea to create a virtual environment for `spark-notebook`, especially
if the default `python` on your system is an older version. But it is not a
requirement. [This article](https://hackercodex.com/guide/python-development-environment-on-mac-osx/)
is a good reference if you are new to Python development environment.

* Make sure you have python 3.4 or newer version installed. If not, install it first.

* Create a virtual environment which uses `python3`.

```
virtualenv -p python3 <env_name>
```
* Activate the virtualenv created in step 2.

```
source <env_name>/bin/activate
```
* Install required packages.

```
pip install -r requirements.txt
```


## Usage

If the required packages are installed in a virtualenv, activate the virtualenv first.

```
source <env_name>/bin/activate
```
1. Run `python run.py`.
2. A browser window will automatically open the URL: `http://localhost:5000`.

Please refer to [docs](docs) for more details.

## Environment

1. Apache Spark will be installed using [Flintrock](https://github.com/nchammas/flintrock).
2. `PYSPARK_PYTHON` is set to `python2.7` on the cluster.
3. `numpy` and `matplotlib` are installed by default on the cluster.

## Important Notes

Please keep the credentials file (by default, `credentials.yaml`) in a secure
place. Especially, do NOT accidently upload it to any public GitHub repositories.
