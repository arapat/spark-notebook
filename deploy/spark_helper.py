'''
SparkHelper is a class to deploy Spark and Jupyter Notebook on AWS.
The idea is to split the deployment logic from the Spark-Notebook UI
(i.e. Flask logic).
Current implementation applies spark-ec2 script for the deployment.
'''

import boto.ec2
import json
import logging
import os
import requests
import shutil
import subprocess
import sys
import webbrowser
import yaml
from os.path import expanduser
from time import sleep
from urllib.request import urlopen
from IPython.lib import passwd

from .thread_wrapper import ThreadWrapper
from .thread_wrapper import ThreadIO

IN_PROCESS = "IN_PROCESS"
FAILED = "FAILED"
SUCCEED = "SUCCEED"

logger = logging.getLogger(__name__)
logger.addHandler(logging.FileHandler('deploy.log'))
logger.setLevel(logging.INFO)


class SparkHelper:
    def __init__(self, config):
        self.config = config

        # helper variables to support running setup in separate threads
        self._thread_setup = None
        self._thread_notebook = None
        self._setup_status = None
        logger.info("SparkHelper initialized.")

        with open("thirdparty/ec2instances.info/memory.json") as f:
            self._ec2_config = json.load(f)

    def _run_command(self, command, shell=False):
        if not shell and type(command) is str:
            command = command.split()
        str_cmd = command
        if type(str_cmd) is list:
            str_cmd = ' '.join(str_cmd)
        logger.info("Running command: " + str_cmd)
        p = subprocess.Popen(command, shell=shell,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        return out, err

    def _send_command(self, command, master_only=False):
        cmd = ("flintrock --config ./config.yaml run-command " + self.name +
               " --ec2-identity-file " + self.KEY_IDENT_FILE)
        if master_only:
            cmd += " --master-only"
        cmd += " '" + command + "'"
        out, err = self._run_command(cmd, True)
        if err:
            logger.error(err.decode())

    def _send_file(self, src, tgt):
        command = (' '.join(self.scp[:-1]) + ' ' +
                   src + ' ' + self.scp[-1] + ':' + tgt)
        self._run_command(command.split())

    def _get_file(self, src, tgt):
        command = (' '.join(self.scp[:-1]) + ' ' +
                   self.scp[-1] + ':' + src + ' ' + tgt)
        self._run_command(command.split())

    def _launch_spark(self):
        def set_password(password, file_path):
            with open(file_path) as f:
                r = f.read()
            r += '\nc.NotebookApp.password = u"%s"' % passwd(password)
            tempfile = file_path + ".temp"
            with open(tempfile, "w") as f:
                f.write(r)
            return tempfile

        logger.info("Starting a new cluster: %s" % self.name)

        # Launch a Spark cluster
        prog_path = (subprocess.Popen("which flintrock", stdout=subprocess.PIPE, shell=True)
                               .communicate()[0].decode().strip())
        logger.info("Flintrock path: " + prog_path)
        env = os.environ.copy()
        argv = (
            "python -u %s --config ./config.yaml launch --assume-yes " % prog_path +
            "--num-slaves " + str(self.workers) + " --ec2-instance-type %s " % self.instance +
            "--ec2-key-name " + self.KEY_PAIR + " --ec2-identity-file " + self.KEY_IDENT_FILE
        )

        if self.spot_price:
            argv += " --ec2-spot-price " + str(self.spot_price)
        argv += " " + self.name

        logger.info("Running command: " + argv)
        proc = subprocess.Popen(argv.split(), env=env,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        while True:
            line = proc.stdout.readline().decode().strip()
            print(line)
            logger.info(line)
            if proc.poll() is not None:
                break
            sleep(0.1)

        # If return code is not 0, launching was failed.
        if proc.poll():
            self._setup_status = FAILED
            error_msg = proc.stderr.read().decode().strip()
            print(error_msg)
            logger.error(error_msg)
            print("Launching Spark failed.", file=sys.stderr)
            logger.error("Launching Spark failed.")
            return

        print("Setting up cluster.")
        logger.info("Setting up cluster.")

        self.init_cluster(self.name)

        logger.info("Setting up IPython Notebook.")

        # Write .bashrc
        print("Writing bash configurations.")
        logger.info("Writing .bashrc.")
        self._send_file("remote/spark-ec2/bashrc", "~/.bashrc")
        logger.info("Writing .bash_profile.")
        self._send_file("remote/spark-ec2/bash_profile", "~/.bash_profile")

        # Set up HDFS
        print("Writing HDFS configurations.")
        logger.info("Writing hadoop/conf/hadoop-env.sh")
        self._send_command(
            ('echo "\nexport HADOOP_CLASSPATH=\$HADOOP_CLASSPATH:'
             '\$HADOOP_HOME/share/hadoop/tools/lib/*\n" '
             '>> ~/hadoop/conf/hadoop-env.sh'), True)
        print("Restart Hadoop.")
        logger.info("Restart Hadoop.")
        # Restart Hadoop because new jars appended to CLASSPATH
        self._send_command("~/hadoop/sbin/stop-dfs.sh", True)
        self._send_command("~/hadoop/sbin/start-dfs.sh", True)

        # Set up Spark
        print("Writing Spark configurations.")
        logger.info("Writing spark/conf/spark-defaults.conf")
        mem_size = int(0.9 * self._ec2_config[self.instance])
        self._send_command(
            ('echo "\nspark.driver.memory\t%dg\n" '
             '>> ~/spark/conf/spark-defaults.conf' % mem_size), True)

        # Set up IPython Notebook
        print("Setting up IPython Notebook.")
        # IPython config
        self._send_command("mkdir -p ~/.ipython/profile_default", True)
        self._send_file("remote/spark-ec2/ipython_config.py",
                        "~/.ipython/profile_default/ipython_config.py")
        # IPython Notebook config
        nb_config = set_password(self.passwd,
                                 "remote/spark-ec2/ipython_notebook_config.py")
        self._send_file(nb_config,
                        "~/.ipython/profile_default/ipython_notebook_config.py")
        os.remove(nb_config)

        # Send Jupyter helper function files for setting up Spark and S3
        # when a new notebook is opened
        print("Uploading helper functions.")
        logger.info("Uploading helper functions.")
        self._send_file("./remote/init_sc.py", "~")
        self._send_file("./remote/init_s3.py", "~")
        self._send_file("./remote/s3helper.py", "~")

        # Install Python packages
        print("Installing python packages... (may take 2-3 minutes)")
        logger.info("Installing python packages.")

        self._send_command(
            "sudo yum install -y python27-numpy python27-matplotlib")
        print("Installed python package: numpy, matplotlib.")
        self._send_command(
            "sudo yum install -y gcc gcc-c++ git", True)
        print("Installed gcc, git.")
        for package in ["jupyter", "boto", "requests"]:
            self._send_command(
                ("sudo pip install --upgrade " + package), True)
            print("Installed python package: " + package + ".")

        self._setup_status = SUCCEED
        print("The cluster is up!")
        logger.info("The cluster is up!")

    # TODO: Add exception handling for notebook launching
    def _launch_notebook(self):
        # Kill launched notebooks
        self._send_command("kill -9 $(pgrep jupyter)", True)
        # Upload sample notebooks
        self._send_command("mkdir -p ~/workspace/examples", True)
        self._send_file('./remote/examples/FilesIO.ipynb',
                        '~/workspace/examples')
        # Restart Spark - in case some python process stucked
        self._send_command("~/spark/sbin/stop-all.sh", True)
        self._send_command("~/spark/sbin/start-all.sh", True)
        # Re-generate Jupyter notebook configs
        self._send_command("rm -rf ~/.jupyter", True)
        # Launch notebook remotely
        command = self.ssh + ["nohup jupyter notebook > /dev/null "
                              "2> /dev/null < /dev/null &"]
        self._run_command(command)
        logger.info("Jupyter notebook launched.")

    def is_valid_ec2_instance(self, instance_name):
        return instance_name in self._ec2_config

    def get_cluster_names(self):
        names = set()
        for instance in self.conn.get_only_instances():
            is_flintrock = False
            for g in instance.groups:
                if g.name == "flintrock":
                    is_flintrock = True
                    break
            if (is_flintrock and 'Name' in instance.tags and
                    instance.tags['Name'].endswith('-master')):
                names.add(instance.tags['Name'][:-7])
        return list(names)

    def init_account(self, account):
        '''
        Initialize SparkHelper for a specific AWS account.
        '''

        cred = self.config.credentials
        self.AWS_ACCESS_KEY_ID, self.AWS_SECRET_ACCESS_KEY = (
            cred["ec2"][account]["aws-access-key-id"],
            cred["ec2"][account]["aws-secret-access-key"]
        )
        self.KEY_PAIR, self.KEY_IDENT_FILE = (
            cred["ec2"][account]["key-name"],
            cred["ec2"][account]["identity-file"]
        )
        self.email_address = cred["ec2"][account]["email-address"]
        self.conn = boto.ec2.connect_to_region(
            self.config['providers']['ec2']['region'],
            aws_access_key_id=self.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY)
        os.environ["AWS_ACCESS_KEY_ID"] = self.AWS_ACCESS_KEY_ID
        os.environ["AWS_SECRET_ACCESS_KEY"] = self.AWS_SECRET_ACCESS_KEY

    def init_cluster(self, name):
        '''
        Initialize SparkHelper for a specific Spark cluster
        launched on a specific AWS account.
        '''

        def get_master_url():
            instances = self.conn.get_only_instances(
                filters={"instance.group-name": "flintrock",
                         "instance-state-name": "running"})
            for instance in instances:
                if 'Name' in instance.tags and instance.tags['Name'] == name + '-master':
                    break
            if not instance:
                logger.error("No master node exists in %s." % name)
                return ''
            dns_name = instance.public_dns_name
            if not dns_name:
                logger.error("The public DNS name of the %s master node"
                             " is invalid.")
            return instance.public_dns_name

        self.name = name
        self.master_url = get_master_url()
        options = ["-i", self.KEY_IDENT_FILE, "-o", "StrictHostKeyChecking=no",
                   "%s@%s" % (self.config['providers']['ec2']['user'], self.master_url)]
        self.ssh = ["ssh"] + options
        self.scp = ["scp", "-r"] + options
        self.check_security_groups()

    def list_files(self, path):
        command = self.ssh + ["ls", "-lrt", path]
        out, err = self._run_command(command)
        return out.decode() + '\n' + err.decode()

    def download(self, remote, local):
        self._send_command("mv ~/workspace/metastore_db ~", True)
        r = self._get_file(remote, local)
        self._send_command("mv ~/metastore_db ~/workspace", True)
        return r

    # TODO: sync files across the cluster
    def upload(self, local, remote):
        if os.path.exists(local):
            return self._send_file(local, remote)
        return "%s doesn't exist." % local

    def check_security_groups(self):
        '''
        Add current IP address to the security group.
        '''
        security_group_name = "flintrock"

        # Open http://httpbin.org/ip to get the public ip address
        ip_address = json.loads(urlopen('http://httpbin.org/ip').read().decode())['origin']

        # Check for the security group and create it if missing
        for sg in self.conn.get_all_security_groups():
            if sg.name == security_group_name:
                break

        # Verify the security group has the current ip address in it
        tcp_rule = False
        for rule in sg.rules:
            if (str(rule.ip_protocol) == "tcp" and
                    str(rule.from_port) == "0" and
                    str(rule.to_port) == "65535" and
                    str(ip_address) + "/32" in str(rule.grants)):
                logger.info(str(ip_address) + " (TCP) is already added to " +
                            security_group_name + " security group")
                tcp_rule = True

        # If the current ip address is missing from
        # the security group then add it
        if not tcp_rule:
            logger.info("Adding " + str(ip_address) + " (TCP) to " +
                        security_group_name + " security group")
            # Allow all TCP
            sg.authorize('tcp', 0, 65535, str(ip_address) + "/32")

    def setup_s3(self, cred):
        tempfile = "./remote/init_s3.py.temp"
        with open(tempfile, "w") as f:
            f.write("s3helper.set_credential('%s', '%s')" %
                    (cred["aws-access-key-id"], cred["aws-secret-access-key"]))
        self._send_file(tempfile, "~/init_s3.py")
        os.remove(tempfile)

    def setup_cluster(self, name, num_of_workers, instance,
                      spot_price, passwd):
        if self._setup_status is not None:
            return self._setup_status
        self.name = name
        self.workers = num_of_workers
        self.passwd = passwd
        self.instance = instance
        self.spot_price = spot_price

        self._io_setup = ThreadIO()
        self._thread_setup = ThreadWrapper(self._launch_spark, self._io_setup)
        self._setup_status = IN_PROCESS
        self._thread_setup.start()

    def get_setup_status(self):
        return self._setup_status

    def get_setup_duration(self):
        return self._thread_setup.duration()

    def get_setup_log(self):
        return self._io_setup.get_messages()

    def reset_spark_setup(self):
        self._setup_status = None

    def check_notebook(self, force=False):
        if self._thread_notebook and self._thread_notebook.is_running():
            return IN_PROCESS
        self._thread_notebook = None
        if not force:
            try:
                requests.get("http://%s:8888" % self.master_url)
                logger.info("Notebook is ready.")
                return
            except:
                pass
        self._thread_notebook = ThreadWrapper(self._launch_notebook)
        self._thread_notebook.start()
        return IN_PROCESS

    def destroy(self):
        env = os.environ.copy()
        argv = "flintrock --config ./config.yaml destroy --assume-yes " + self.name

        proc = subprocess.Popen(argv.split(), env=env,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)
        out, err = proc.communicate()
        if proc.poll():
            logger.error("Destroying the cluster %s failed." % self.name)
            logger.error(err)
        else:
            logger.info("The cluster %s is destroyed." % self.name)
