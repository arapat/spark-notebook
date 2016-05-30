# Spark Notebook (Launcher)

The Spark Notebook (Launcher) is a web-based interface to launch a cluster on
Amazon Web Services, then install and start Apache Spark and Jupyter notebook
on it.

Requires Python 2.7 and Apache Spark.


## Installation

1. Get Apache Spark from the project website.
2. Set the value of the `$SPARK_PATH` variable to the path of
the Spark root directory.
3. Install required packages: `pip install -r requirements.txt`.


## Usage

1. Run `python spark-notebook.py`.
2. A browser window will automatically open the URL `http://localhost:5000`.


## Important Note

Please keep the credentials file (by default, `credentials.yaml`) in a secure
place. Especially, do NOT accidently upload it to any public GitHub repositories.
