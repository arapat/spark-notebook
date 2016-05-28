# coding: utf-8
from boto.s3.connection import S3Connection
import os


def _download_s3(files, tgt, aws_access_key, aws_secret_access_key):
    os.system("/root/ephemeral-hdfs/bin/hdfs dfs -mkdir -p " + tgt)
    os.system(("/root/ephemeral-hdfs/bin/hdfs dfs "
               "-Dfs.s3n.awsAccessKeyId=%s -Dfs.s3n.awsSecretAccessKey=%s "
               "-cp %s %s")
              % (aws_access_key, aws_secret_access_key, files, tgt))


class S3Helper:
    """A helper function to access S3 files"""
    def __init__(self):
        self.aws_access_key = None
        self.aws_secret_access_key = None
        self.conn = None
        self.bucket_name = None
        self.bucket = None

    def set_credential(self, aws_access_key, aws_secret_access_key):
        """Set AWS credential.

            Args:
                aws_access_key, aws_secret_access_key
            Returns:
                None
        """
        self.aws_access_key = aws_access_key
        self.aws_secret_access_key = aws_secret_access_key
        _conf = sc._jsc.hadoopConfiguration()
        _conf.set("fs.s3n.awsAccessKeyId", aws_access_key)
        _conf.set("fs.s3n.awsSecretAccessKey", aws_secret_access_key)

    def open_bucket(self, bucket_name):
        """Open a S3 bucket.

            Args:
                bucket_name
            Returns:
                None
        """
        if bucket_name.startswith('s3n://') or bucket_name.startswith('s3://'):
            raise ValueError('bucket_name must NOT contain any prefix '
                             '(e.g. s3:// or s3n://)')
        if not self.aws_access_key:
            raise ValueError('AWS credential is not set.')

        while bucket_name[-1] == '/':
            bucket_name = bucket_name[:-1]
        self.bucket_name = bucket_name
        self.conn = S3Connection(self.aws_access_key,
                                 self.aws_secret_access_key)
        self.bucket = self.conn.get_bucket(self.bucket_name)

    def ls(self, path=''):
        """List all files in `path`.

            Args:
                path
            Returns:
                an array of files in `path`
        """
        if not self.bucket:
            raise Exception('no bucket is opened.')

        path = path.strip()
        if len(path) and path[0] == '/':
            path = path[1:]
        files = self.bucket.list(prefix=path)

        if path == '':
            k = 1
        else:
            k = len(path.split('/')) + 1
        return sorted(list(set(
            ['/'.join(t.key.split('/')[:k]) for t in files])))

    def get_path(self, path=''):
        """Get paths of all files in `path` with s3 prefix,
           which can be passed to Spark.

            Args:
                path
            Returns:
                an array of file paths with s3 prefix
        """
        if not self.bucket:
            raise Exception('no bucket is opened.')

        path = path.strip()
        if len(path) and path[0] == '/':
            path = path[1:]
        files = self.bucket.list(prefix=path)
        prefix = "s3n://%s/" % self.bucket_name
        return [prefix + t.key for t in files]

    def load_path(self, path, tgt):
        """Load all files in `path` to the directory `tgt` in HDFS.

            Args:
                path, tgt
            Returns:
                an array of file paths in HDFS
        """
        if not self.bucket:
            raise Exception('no bucket is opened.')

        path, tgt = path.strip(), tgt.strip()
        if len(path) and path[0] == '/':
            path = path[1:]
        if tgt == '' or (tgt[0] != '/' and not tgt.startswith('hdfs://')):
            tgt = '/' + tgt
        if tgt[-1] != '/':
            tgt = tgt + '/'
        files = self.bucket.list(prefix=path)
        prefix = "s3n://%s/" % self.bucket_name
        _download_s3(' '.join([prefix + t.key for t in files]),
                     tgt, self.aws_access_key, self.aws_secret_access_key)
        return [tgt + t.key.rsplit('/', 1)[1] for t in files]

    def put_file(self, filename, tgt):
        """Save a local file `filename` to the directory `tgt` on S3.

            Args:
                filename, tgt
            Returns:
                None
        """
        if not self.bucket:
            raise Exception('no bucket is opened.')

        tgt = tgt.strip()
        if len(tgt) and tgt[0] == '/':
            tgt = tgt[1:]
        if not tgt:
            tgt = filename.rsplit('/', 1)[-1]
        k = self.bucket.new_key(tgt)
        k.set_contents_from_filename(filename)

    def get_file(self, key_name):
        """Download the remote file `key_name` on S3 to local.

            Args:
                key_name
            Returns:
                None
        """
        if not self.bucket:
            raise Exception('no bucket is opened.')

        key = key_name.strip()
        if key[0] == '/':
            key = key[1:]
        k = self.bucket.get_key(key)
        if not k:
            raise Exception("File %s doesn't exist." % key_name)
        k.get_contents_to_filename(key.rsplit('/', 1)[-1])

s3helper = S3Helper()
