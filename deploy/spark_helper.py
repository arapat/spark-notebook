'''
SparkHelper is a class to deploy Spark and Jupyter Notebook on AWS.
The idea is to split the deployment logic from the Spark-Notebook UI
(i.e. Flask logic).
Current implementation applies spark-ec2 script for the deployment.
'''

import boto.ec2
import imp
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
from urllib2 import urlopen
from IPython.lib import passwd

from thread_wrapper import ThreadWrapper
from thread_wrapper import ThreadIO

# Path to spark_ec2.py, may be replaced by other libraries later
SPARK_PATH = "./thirdparty/spark"
ec2_user = "root"

logger = logging.getLogger(__name__)
logger.addHandler(logging.FileHandler('deploy.log'))
logger.setLevel(logging.INFO)


def call_spark_ec2(argv):
    spark_ec2 = imp.load_source("spark_ec2",
                                SPARK_PATH + "/spark_ec2.py")
    sys.argv = argv
    try:
        spark_ec2.main()
    except SystemExit as e:
        if e.code:
            return 1
    except Exception as e:
        print e
        return 1
    return 0


class SparkHelper:
    IN_PROCESS = "IN_PROCESS"
    FAILED = "FAILED"
    SUCCEED = "SUCCEED"

    def __init__(self, config):
        global SPARK_PATH
        SPARK_PATH = config.get("spark_path", SPARK_PATH)
        self.config = config

        self._thread_setup = None
        self._thread_notebook = None
        self._setup_status = None
        logger.info("SparkHelper initialized. Spark path: %s." % SPARK_PATH)

        with open("thirdparty/ec2instances.info/memory.json") as f:
            self._ec2_config = json.load(f)

    def _run_command(self, command):
        if type(command) is str:
            command = command.split()
        logger.info("Running command: " + " ".join(command))
        p = subprocess.Popen(command, shell=False,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if p.poll() == 0:
            return None
        return out, err

    def _send_command(self, command):
        logger.info('Sending command:' + ' '.join(command))
        return self._run_command(self.ssh + command)

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
        argv = ["just-a-placeholder",
                "--key-pair=" + self.KEY_PAIR,
                "--identity-file=" + self.KEY_IDENT_FILE,
                "--region=" + self.config['ec2']['region'],
                "--zone=" + self.config['ec2']['zone'],
                "--slaves=%d" % int(self.workers),
                "--instance-type=" + self.instance,
                "--hadoop-major-version=yarn",
                "--use-existing-master"]
        if self.spot_price:
            argv.append("--spot-price=%.2f" % float(self.spot_price))
        argv.append("launch")
        if self.resume:
            argv.append("--resume")
        argv.append(self.name)

        ret_code = call_spark_ec2(argv)

        # If return code is not 0, launching was failed.
        if ret_code:
            self._setup_status = self.FAILED
            print >> sys.stderr, "Launching Spark failed."
            logger.error("Launching Spark failed.")
            return

        print >> sys.stderr, "Setting up cluster."
        logger.info("Setting up cluster.")

        self.init_cluster(self.name)

        logger.info("Setting up IPython Notebook.")
        with open("./remote/spark-ec2/config.yaml", 'r') as stream:
            file_path = yaml.load(stream)

        # Write .bashrc
        print >> sys.stderr, "Writing bash configurations."
        logger.info("Writing .bashrc.")
        self._send_file("remote/spark-ec2/bashrc", "/root/.bashrc")
        logger.info("Writing .bash_profile.")
        self._send_file("remote/spark-ec2/bash_profile", "/root/.bash_profile")

        # Set up Spark
        print >> sys.stderr, "Writing Spark configurations."
        logger.info("Writing spark/conf/spark-defaults.conf")
        mem_size = int(0.9 * self._ec2_config[self.instance])
        self._send_command(
            ["echo", '"\nspark.driver.memory\t%dg\n"' % mem_size,
             ">>", file_path["spark"]["conf-file"]])

        # Set up IPython Notebook
        print >> sys.stderr, "Setting up IPython Notebook."
        # IPython config
        self._send_file("remote/spark-ec2/ipython_config.py",
                        file_path["ipython"]["config-file"])
        # IPython Notebook config
        nb_config = set_password(self.passwd,
                                 "remote/spark-ec2/ipython_notebook_config.py")
        self._send_file(nb_config,
                        file_path["ipython"]["notebook-config-file"])
        os.remove(nb_config)

        # Send Jupyter helper function files for setting up Spark and S3
        # when a new notebook is opened
        print >> sys.stderr, "Uploading helper functions."
        logger.info("Uploading helper functions.")
        self._send_file("./remote/init_sc.py", "~")
        self._send_file("./remote/init_s3.py", "~")
        self._send_file("./remote/s3helper.py", "~")

        # Install all necessary Python packages
        print >> sys.stderr, "Installing python packages."
        logger.info("Installing python packages.")

        # Install python dev
        self._send_command(["yum", "install", "-y", "python27-devel"])
        # Install pip
        self._send_command(["wget", "https://bootstrap.pypa.io/get-pip.py"])
        self._send_command(["python2.7", "get-pip.py"])
        # Install boto
        self._send_command(["pip", "install", "--upgrade", "boto"])
        # Install Jupyter
        self._send_command(["pip", "install", "--upgrade", "numpy"])
        self._send_command(["pip", "install", "--upgrade", "jupyter"])
        # Install matplotlib and networkx
        self._send_command(["yum", "install", "-y", "freetype-devel"])
        self._send_command(["yum", "install", "-y", "libpng-devel"])
        self._send_command(["yum", "install", "-y", "graphviz-devel"])
        self._send_command(["pip", "install", "--upgrade", "matplotlib"])
        self._send_command(["pip", "install", "--upgrade", "networkx"])
        self._send_command(["pip", "install", "--upgrade", "pygraphviz"])
        # Install requests
        self._send_command(["pip", "install", "--upgrade", "requests"])
        # Sync Python2.7 libraries
        self._send_command(["/root/spark-ec2/copy-dir",
                           "/usr/local/lib64/python2.7/site-packages/"])

        self._setup_status = self.SUCCEED
        print >> sys.stderr, "The cluster is up!"
        logger.info("The cluster is up!")

    # TODO: Add exception handling for notebook launching
    def _launch_notebook(self):
        # Kill launched notebooks
        self._send_command(["kill", "-9", "`pidof python2.7`"])
        # Upload sample notebooks
        self._send_command(["mkdir", "-p", "/root/ipython/examples"])
        self._send_file('./remote/examples/FilesIO.ipynb',
                        '/root/ipython/examples')
        # Restart Spark - in case some python process stucked
        self._send_command(["/root/spark/sbin/stop-all.sh"])
        self._send_command(["/root/spark/sbin/start-all.sh"])
        # Re-generate Jupyter notebook configs
        self._send_command(["rm", "-rf", "/root/.jupyter"])
        # Launch notebook remotely
        command = self.ssh + ["nohup jupyter notebook > /dev/null "
                              "2> /dev/null < /dev/null &"]
        self._run_command(command)
        logger.info("Jupyter notebook launched.")

    def is_valid_ec2_instance(self, instance_name):
        return instance_name in self._ec2_config

    def get_cluster_names(self):
        names = []
        for instance in self.conn.get_only_instances():
            if instance.groups and instance.groups[0].name.endswith("-master"):
                names.append(instance.groups[0].name[:-7])
        return names

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
        self.conn = boto.ec2.connect_to_region(
            self.config['ec2']['region'],
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
                filters={"instance.group-name": "%s-master" % name,
                         "instance-state-name": "running"})
            if not instances:
                logger.error("No master node exists in %s." % name)
                return ''
            dns_name = instances[0].public_dns_name
            if not dns_name:
                logger.error("The public DNS name of the %s master node"
                             " is invalid.")
            return instances[0].public_dns_name

        self.name = name
        self.master_url = get_master_url()
        options = ["-i", self.KEY_IDENT_FILE, "-o", "StrictHostKeyChecking=no",
                   "%s@%s" % (ec2_user, self.master_url)]
        self.ssh = ["ssh"] + options
        self.scp = ["scp", "-r"] + options
        self.check_security_groups()

    def list_files(self, path):
        out, err = self._send_command(["ls", "-lrt", path])
        return out + '\n' + err

    def download(self, remote, local):
        self._send_command(["mv", "/root/ipython/metastore_db", "/root"])
        r = self._get_file(remote, local)
        self._send_command(["mv", "/root/metastore_db", "/root/ipython"])
        return r

    def upload(self, local, remote):
        if os.path.exists(local):
            r = self._send_file(local, remote)
            r2 = self._send_command(["/root/spark-ec2/copy-dir", remote])
            if r is None and r2 is not None:
                r = r2[1]
        else:
            r = "%s doesn't exist." % local
        return r

    def check_security_groups(self):
        '''
        Add current IP address to the security group.
        '''
        security_group_name = self.name + "-master"

        # Open http://httpbin.org/ip to get the public ip address
        ip_address = json.load(urlopen('http://httpbin.org/ip'))['origin']

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
                      spot_price, passwd, resume=False):
        if self._setup_status is not None:
            return self._setup_status
        self.name = name
        self.workers = num_of_workers
        self.passwd = passwd
        self.instance = instance
        self.spot_price = spot_price
        self.resume = resume

        self._io_setup = ThreadIO()
        self._thread_setup = ThreadWrapper(self._launch_spark, self._io_setup)
        self._setup_status = self.IN_PROCESS
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
            return self.IN_PROCESS
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
        return self.IN_PROCESS

    def destroy(self):
        argv = ["just-a-placeholder",
                "--region=" + self.config['ec2']['region'],
                "destroy", self.name]
        logger.info("Destroying the cluster: %s." % self.name)
        io = ThreadIO(["y\n"])
        io.enter()
        ret_code = call_spark_ec2(argv)
        io.exit()
        if ret_code:
            logger.error("Destroying the cluster %s failed." % self.name)
            logger.error(io.get_messages())
        else:
            logger.info("The cluster %s is destroyed." % self.name)
