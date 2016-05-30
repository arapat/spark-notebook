# Spark Notebook (Launcher)

Spark Notebook (Launcher) is a web-based interface to install and start
Apache Spark and Jupyter Notebook on Amazon Web Services.

Requires Python 2.7 and Apache Spark.


## Installation

1. Get Apache Spark from the project website.
2. Set the value of the `$SPARK_PATH` variable to the path of
the Spark root directory.
3. Install required packages: `pip install -r requirements.txt`.


## Usage

1. Run `python spark-notebook.py`.
2. A browser window will automatically open the URL `http://localhost:5000`.


## Details

1. Apache Spark will be installed using the
[spark-ec2](https://github.com/amplab/spark-ec2) script. The Spark launch script
may be replaced by [Flintrock](https://github.com/nchammas/flintrock) later when
`Flintrock` makes 1.0 release.
2. `PYSPARK_PYTHON` is set to `python2.7` on the cluster.
3. `numpy` and `matplotlib` are installed by default on the cluster.


## Accessing S3

A helper variable `s3helper` is initialized when Jupyter Notebook is launched.
[remote/examples/FilesIO.ipynb](https://github.com/arapat/spark-notebook/blob/master/remote/examples/FilesIO.ipynb)
contains examples of using `s3helper`.


## Important Notes

Please keep the credentials file (by default, `credentials.yaml`) in a secure
place. Especially, do NOT accidently upload it to any public GitHub repositories.
