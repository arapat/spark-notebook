# Spark Notebook (Launcher)

Spark Notebook (Launcher) is a web-based interface to install and start
Apache Spark and Jupyter Notebook on Amazon Web Services.

Requires Python 3.4 or newer.


## Installation

It is a good idea to create a virtual environment for `spark-notebook`, especially
if the default `python` on your system is an older version. But it is not a
requirement. [This article](https://hackercodex.com/guide/python-development-environment-on-mac-osx/)
is a good reference if you are new to Python development environment.

(1) Make sure you have python 3.4 or newer version installed. If not, install it first.

(2) Create a virtual environment which uses `python3`.
```
virtualenv -p python3 <env_name>
```
(3) Activate the virtualenv created in step 2.
```
source <env_name>/bin/activate
```
(4) Install required packages.
```
pip install -r requirements.txt
```


## Usage

If the required packages are installed in a virtualenv, activate the virtualenv first.
```
source <env_name>/bin/activate
```
1. Run `python spark-notebook.py`.
2. A browser window will automatically open the URL `http://localhost:5000`.


## Environment

1. Apache Spark will be installed using [Flintrock](https://github.com/nchammas/flintrock).
2. `PYSPARK_PYTHON` is set to `python2.7` on the cluster.
3. `numpy` and `matplotlib` are installed by default on the cluster.


## Create SparkContext `sc` in Jupyter Notebook

Here is a sample code to create a SparkContext variable in a Jupyter Notebook.
```python
from pyspark import SparkConf
from pyspark import SparkContext

# `master_url` is equal to the Spark master URL, and is created by spark-notebook
conf = SparkConf().setMaster(master_url)
sc = SparkContext(conf=conf)
```


## Accessing S3

A helper variable `s3helper` is initialized when Jupyter Notebook is launched.
[remote/examples/FilesIO.ipynb](https://github.com/arapat/spark-notebook/blob/master/remote/examples/FilesIO.ipynb)
contains examples of using `s3helper`. Alternatively, you can try to call `s3helper.help()`.

### Set up your S3 credentials

In the cluster details page, you can set up your S3 credentials on the cluster by clicking the credential's name under the "Set up S3" section.


## Important Notes

Please keep the credentials file (by default, `credentials.yaml`) in a secure
place. Especially, do NOT accidently upload it to any public GitHub repositories.
