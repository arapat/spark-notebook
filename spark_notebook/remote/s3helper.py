# coding: utf-8
from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection
from os.path import expanduser
import os
import subprocess


def _run_command(command, detail=False):
    proc = subprocess.Popen(command.split(),
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    if detail:
        while True:
            line = proc.stdout.readline()
            if not line:
                break
            print(line)
    return proc.communicate()


def _list_hdfs(path):
    command = expanduser("~/hadoop/bin/hdfs dfs -ls ") + path
    out, _err = _run_command(command)
    if out:
        print(out)


def _s3_to_hdfs(files, tgt, aws_access_key, aws_secret_access_key):
    _out, err1 = _run_command(
        expanduser("~/hadoop/bin/hdfs dfs -mkdir -p ") + tgt)
    if err1:
        print(err1)
        return
    _out, err2 = _run_command((
        expanduser("~/hadoop/bin/hdfs dfs ") +
        "-Dfs.s3n.awsAccessKeyId=%s -Dfs.s3n.awsSecretAccessKey=%s "
        "-cp %s %s")
        % (aws_access_key, aws_secret_access_key, files, tgt))
    if err2:
        print(err2)


def _hdfs_to_s3(files, tgt, aws_access_key, aws_secret_access_key):
    _out, err = _run_command((
        expanduser("~/hadoop/bin/hadoop distcp ") +
        "-Dfs.s3n.awsAccessKeyId=%s -Dfs.s3n.awsSecretAccessKey=%s "
        "%s %s")
        % (aws_access_key, aws_secret_access_key, files, tgt), True)
    if err:
        print(err)


def _local_to_hdfs(src, tgt):
    if tgt[0] != '/':
        tgt = '/' + tgt
    _out, err1 = _run_command(
        expanduser("~/hadoop/bin/hdfs dfs -mkdir -p ") + tgt)
    if err1:
        print(err1)
        return
    _out, err2 = _run_command(
        expanduser("~/hadoop/bin/hdfs dfs -cp %s %s")
        % ("file://" + os.path.join(src, '*'), tgt))
    if err2:
        print(err2)


class S3Helper:
    """A helper function to access S3 files"""
    def __init__(self):
        self.aws_access_key = None
        self.aws_secret_access_key = None
        self.conn = None
        self.bucket_name = None
        self.bucket = None

    def help(self):
        print('''
        s3helper is a helper object to move files and directory between
        local filesystem, AWS S3 and local HDFS.

        Usage:

        1. Set your aws credentials:
            s3helper.set_credential(<aws_access_key>, <aws_secret_access_key>)
        Spark-notebook may already set up the credentials for you. It can be
        checked by
            s3helper.print_credential()
        2. Open a S3 bucket under your account
            s3helper.open_bucket(<bucket_name>)
        3. List all files under the opened S3 bucket
            s3helper.ls() or s3helper.ls_s3()
        Or optionally,
            s3helper.ls(<file_path>) or s3helper.ls_s3(<file_path>)
        4. List all files on HDFS
            s3helper.ls_hdfs()
        Or optionally,
            s3helper.ls_hdfs(<file_path>)
        where <file_path> is an absolute path in the opened S3 bucket.

        Now you can access your S3 files.

        1. Transfer files between S3 and HDFS
          a. To download all S3 files under a directory to HDFS, please call
                s3helper.s3_to_hdfs(<s3_directory_path>, <HDFS_directory_path>)
          b. To upload a directory on HDFS to S3, please call
                s3helper.hdfs_to_s3(<HDFS_directory_path>, <s3_directory_path>)

        2. Transfer files between S3 and local filesystem (not HDFS)
          a. To download one single S3 file to local filesystem, please call
                s3helper.s3_to_local(<s3_file_path>, <local_file_path>)
          b. To upload a file on local filesystem to S3, please call
                s3helper.local_to_s3(<local_file_path>, <s3_directory_path>)

        3. Transfer files between local filesystem and HDFS
          a. To upload a directory on local filesystem to HDFS, please call
                s3helper.local_to_hdfs(<local_dir_path>, <HDFS_dir_path>)

        4. Get S3 file paths without data transfer
          a. To get the URLs of S3 files under a directory, please call
                s3helper.get_path(<s3_directory_path>)
          Note this method do nothing on your local HDFS.
        ''')

    def set_credential(self, aws_access_key, aws_secret_access_key):
        """Set AWS credential.

            Args:
                aws_access_key, aws_secret_access_key
            Returns:
                None
        """
        self.aws_access_key = aws_access_key
        self.aws_secret_access_key = aws_secret_access_key

    def set_sparkcontext(self, sc):
        """Setup SparkContext to load files directly from S3.
        """
        if not self.aws_access_key:
            raise ValueError('AWS credential is not set. '
                             'Please use set_credential method first.')
        _conf = sc._jsc.hadoopConfiguration()
        _conf.set("fs.s3n.awsAccessKeyId", self.aws_access_key)
        _conf.set("fs.s3n.awsSecretAccessKey", self.aws_secret_access_key)

    def print_credential(self):
        if not self.aws_access_key:
            raise ValueError('AWS credential is not set. '
                             'Please use set_credential method first.')
        print("AWS Access Key ID: %s\nAWS Secret Access Key: %s"
               % (self.aws_access_key, self.aws_secret_access_key))

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
            raise ValueError('AWS credential is not set.'
                             'Please use set_credential method first.')

        while bucket_name[-1] == '/':
            bucket_name = bucket_name[:-1]
        self.bucket_name = bucket_name
        self.conn = S3Connection(self.aws_access_key,
                                 self.aws_secret_access_key)
        try:
            self.bucket = self.conn.get_bucket(self.bucket_name)
        except S3ResponseError as e:
            print('Open S3 bucket "%s" failed.\n' % bucket_name + str(e))
            print(e.message)

    def ls(self, path=''):
        """same as ls_s3"""
        return self.ls_s3(path)

    def ls_s3(self, path=''):
        """List all files in `path` on S3.

            Args:
                path
            Returns:
                an array of files in `path`
        """
        if not self.bucket:
            raise Exception('No bucket is opened. '
                            'Please use open_bucket method first.')

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

    def ls_hdfs(self, path='/'):
        """List all files in `path` on HDFS."""
        if not path or path[0] != '/':
            path = '/' + path
        return _list_hdfs(path)

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

    def s3_to_hdfs(self, src, tgt):
        """Load all files in `src` to the directory `tgt` in HDFS.

            Args:
                src, tgt
            Returns:
                an array of file paths in HDFS
        """
        if not self.bucket:
            raise Exception('no bucket is opened.')

        src, tgt = src.strip(), tgt.strip()
        if len(src) and src[0] == '/':
            src = src[1:]
        if tgt == '' or (tgt[0] != '/' and not tgt.startswith('hdfs://')):
            tgt = '/' + tgt
        if tgt[-1] != '/':
            tgt = tgt + '/'
        files = self.bucket.list(prefix=src)
        prefix = "s3n://%s/" % self.bucket_name
        _s3_to_hdfs(' '.join([prefix + t.key for t in files]),
                    tgt, self.aws_access_key, self.aws_secret_access_key)
        self.ls_hdfs(tgt)

    def hdfs_to_s3(self, src, tgt):
        """Upload a directory `src` on HDFS to a directory `tgt` on S3.

           Args:
                src, tgt
           Returns:
                file list of the `tgt` directory on S3 after uploading
        """
        if not self.bucket:
            raise Exception('no bucket is opened. '
                            'See help() method for more info')

        src, tgt = src.strip(), tgt.strip()
        if src == '' or (src[0] != '/' and not src.startswith('hdfs://')):
            src = '/' + src
        if src[-1] != '/':
            src = src + '/'
        if len(tgt) and tgt[0] == '/':
            tgt = tgt[1:]
        tgt = "s3n://%s/" % self.bucket_name + tgt
        print("*NOTE*\n"
               "This method will create a MapReudce job to upload the content "
               "in HDFS to S3. The process may take a while.\n\n")
        _hdfs_to_s3(src, tgt, self.aws_access_key, self.aws_secret_access_key)

    def local_to_s3(self, filename, tgt):
        """Save a local file `filename` to the directory `tgt` on S3.

            Args:
                filename, tgt
            Returns:
                None
        """
        if not self.bucket:
            raise Exception('no bucket is opened.')
        if not os.path.exists(filename):
            raise Exception("File does not exist.")
        if os.path.isdir(filename):
            raise Exception(
                "Transfer between S3 and local filesystem "
                "does not support directory.")

        tgt = tgt.strip()
        if len(tgt) and tgt[0] == '/':
            tgt = tgt[1:]
        if not tgt:
            tgt = filename.rsplit('/', 1)[-1]
        k = self.bucket.new_key(tgt)
        k.set_contents_from_filename(filename)

    def s3_to_local(self, src, tgt):
        """Download the remote file `key_name` on S3 to local.

            Args:
                src, tgt
            Returns:
                None
        """
        if not self.bucket:
            raise Exception('no bucket is opened.')

        key = src.strip()
        if key[0] == '/':
            key = key[1:]
        k = self.bucket.get_key(key)
        if not k:
            raise Exception(
                "File " + src + " doesn't exist.\n"
                "Note that the transfer between S3 and local filesystem "
                "do not support directory.")
        k.get_contents_to_filename(tgt)

    def local_to_hdfs(self, src, tgt):
        """Upload local directory to HDFS.

           Args:
               src - path to the local directory,
               tgt - path to the HDFS directory
           Returns:
               None
        """
        if src[0] != '/' or tgt[0] != '/':
            raise Exception("The directory path cannot be an relative path.")
        _local_to_hdfs(src, tgt)


s3helper = S3Helper()
