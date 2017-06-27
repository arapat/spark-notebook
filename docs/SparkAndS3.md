## Create SparkContext `sc` in Jupyter Notebook

Here is a sample code to create a SparkContext variable in a Jupyter Notebook.

```
from pyspark import SparkContext
sc = SparkContext()
```

## Accessing S3

A helper variable `s3helper` is initialized when Jupyter Notebook is launched.
[provision/workspace/FilesIO.ipynb](../provision/workspace/FilesIO.ipynb)
contains examples of using `s3helper`. Alternatively, you can try to call `s3helper.help()`.
