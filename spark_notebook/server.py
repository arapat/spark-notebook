import os
import os.path
import distutils.util

from .cloud.aws import AWS
from .config import Config
from .credentials import Credentials

from flask import Flask
from flask import redirect
from flask import request
from flask import render_template
from flask import url_for
from flask import flash

import botocore.exceptions
from spark_notebook.exceptions import AWSException
from spark_notebook.exceptions import CredentialsException

app = Flask(__name__)
app.secret_key = 'some_secret'
config = Config()
credentials = Credentials(config.config["credentials"]["path"])


@app.route('/', methods=['GET'])
def main():

    if "config_path" in request.args:
        global config
        global credentials
        config = Config(file_path=request.args.get('config_path'))
        credentials = Credentials(config.config["credentials"]["path"])
        flash("Using config file: %s" % config.file_path)

    if os.path.isfile(config.config["credentials"]["path"]):
        return redirect(url_for('accounts'))
    else:
        return redirect(url_for('save_config_location'))


@app.route('/accounts', methods=['GET', 'POST'])
def accounts():
    error = None

    if request.method == 'POST':
        name = None
        email_address = None
        access_key_id = None
        secret_access_key = None
        ssh_key = None
        key_name = None
        identity_file = None

        if "name" in request.form:
            name = request.form["name"].encode('utf8').decode()
        if "email_address" in request.form:
            email_address = request.form["email_address"].encode('utf8').decode()
        if "access_key_id" in request.form:
            access_key_id = request.form["access_key_id"].encode('utf8').decode()
        if "secret_access_key" in request.form:
            secret_access_key = request.form["secret_access_key"].encode('utf8').decode()
        if "ssh_key" in request.form:
            ssh_key = request.form["ssh_key"].encode('utf8').decode()
        if "key_name" in request.form:
            key_name = request.form["key_name"].encode('utf8').decode()
        if "identity_file" in request.form:
            identity_file = request.form["identity_file"].encode('utf8').decode()

        region_name = config.config["emr"]["region"]

        cloud_account = AWS(access_key_id, secret_access_key, region_name)

        try:
            error = cloud_account.test_credentials()
        except AWSException as e:
            error = e.msg

        if error is None and ssh_key == "generate":
            try:
                cloud_account.create_ssh_key(email_address, os.path.dirname(credentials.file_path))
                key_name = cloud_account.key_name
                identity_file = cloud_account.identity_file
            except AWSException as e:
                error = e.msg

        if error is None:
            try:
                cloud_account.test_ssh_key(key_name, identity_file)
            except AWSException as e:
                error = e.msg

        if error is None:
            try:
                credentials.add(name, email_address, access_key_id, secret_access_key, key_name,
                                identity_file)
            except CredentialsException as e:
                error = e.msg

        if error is None:
            flash("Account %s added" % name)

    return render_template('accounts.html',
                           accounts=credentials.credentials,
                           credential_file=config.config["credentials"]["path"],
                           error=error)


@app.route('/config', methods=['GET', 'POST'])
def save_config_location():
    error = None

    if request.method == 'POST':
        path = request.form['path'].encode('utf8').decode()

        global credentials
        credentials = Credentials(path)
        try:
            credentials.save()
        except CredentialsException as e:
            error = e.msg

        # If there were no errors saving the credentials file then update the credentials path in
        # the config file
        if error is None:
            config.config["credentials"]["path"] = path
            config.save()
            flash("Credentials saved to %s" % path)
            return redirect(url_for('accounts'))

    return render_template('config.html',
                           cred_path=config.config["credentials"]["path"],
                           error=error)


@app.route('/g/<account>', methods=['GET', 'POST'])
def cluster_list_create(account):
    error = None
    subnets = None
    cluster_list = None

    cloud_account = AWS(credentials.credentials[account]["access_key_id"],
                        credentials.credentials[account]["secret_access_key"],
                        config.config["emr"]["region"])

    # if request method is post then create the cluster
    if request.method == "POST":
        name = None
        password = None
        worker_count = None
        subnet_id = None
        instance_type = None
        use_spot = None
        spot_price = None
        bootstrap_path = None
        pyspark_python_version = None

        if "name" in request.form:
            if request.form["name"].encode('utf8').decode() != "":
                name = request.form["name"].encode('utf8').decode()
            else:
                name = config.config['emr']['name']
        if "password" in request.form:
            if request.form["password"].encode('utf8').decode() != "":
                password = request.form["password"].encode('utf8').decode()
            else:
                password = config.config['jupyter']['password']
        if "worker_count" in request.form:
            if request.form["worker_count"].encode('utf8').decode() != "":
                worker_count = request.form["worker_count"].encode('utf8').decode()
            else:
                worker_count = int(config.config['emr']['worker-count'])
        if "subnet_id" in request.form:
            if request.form["subnet_id"].encode('utf8').decode() != "":
                subnet_id = request.form["subnet_id"].encode('utf8').decode()
        if "instance_type" in request.form:
            if request.form["instance_type"].encode('utf8').decode() != "":
                instance_type = request.form["instance_type"].encode('utf8').decode()
            else:
                instance_type = config.config['emr']['instance-type']
        if "use_spot" in request.form:
            if request.form["use_spot"].encode('utf8').decode() == "true":
                use_spot = True
            else:
                use_spot = False
        if "spot_price" in request.form:
            if request.form["spot_price"].encode('utf8').decode() != "":
                spot_price = request.form["spot_price"].encode('utf8').decode()
            else:
                spot_price = config.config['emr']['spot-price']
        if "bootstrap_path" in request.form:
            if request.form["bootstrap_path"].encode('utf8').decode() != "":
                bootstrap_path = request.form["bootstrap_path"].encode('utf8').decode()
        if "pyspark_python_version" in request.form:
            if request.form["pyspark_python_version"].encode('utf8').decode() != "":
                pyspark_python_version = request.form["pyspark_python_version"].encode('utf8')\
                    .decode()

        tags = [{"Key": "cluster", "Value": credentials.credentials[account]["email_address"]}]

        try:
            cluster_id = cloud_account.create_cluster(name,
                                                      credentials.credentials[account]["key_name"],
                                                      instance_type, worker_count, subnet_id,
                                                      use_spot, spot_price, bootstrap_path,
                                                      pyspark_python_version, tags,
                                                      password)
            flash("Cluster launched: %s" % name)
            return redirect(url_for('cluster_details', account=account,
                                    cluster_id=cluster_id))
        except AWSException as e:
            error = e.msg

    # Populate the cluster list
    try:
        cluster_list = cloud_account.list_clusters()
    except AWSException as e:
        error = e.msg

    # Populate the subnets dropdownlist
    try:
        subnets = cloud_account.get_subnets()
    except AWSException as e:
        error = e.msg

    data = {
        'account': account,
        'account_name': account,
        'cluster_name': config.config['emr']['name'],
        'worker_count': str(config.config['emr']['worker-count']),
        'spot_price': "%.2f" % config.config['emr']['spot-price'],
        'instance_type': config.config['emr']['instance-type'],
        'password': config.config['jupyter']['password'],
        'subnets': sorted(subnets["Subnets"], key=lambda k: k["AvailabilityZone"]),
    }

    return render_template('emr-list-create.html',
                           cluster_list=cluster_list,
                           data=data,
                           error=error)


@app.route('/g/<account>/<cluster_id>', methods=["GET", "POST"])
def cluster_details(account, cluster_id):
    error = None
    cluster_info = dict()
    bootstrap_actions = None
    state = None
    state_message = None
    password = None
    ssh_key = None
    logs_bucket_name = None

    cloud_account = AWS(credentials.credentials[account]["access_key_id"],
                        credentials.credentials[account]["secret_access_key"],
                        config.config["emr"]["region"])

    try:
        cluster_info = cloud_account.describe_cluster(cluster_id)["Cluster"]
        if "Status" in cluster_info:
            if "State" in cluster_info['Status']:
                state = cluster_info['Status']['State']
            if "StateChangeReason" in cluster_info['Status']:
                if "Message" in cluster_info['Status']['StateChangeReason']:
                    state_message = cluster_info['Status']['StateChangeReason']['Message']
    except AWSException as e:
        error = e.msg

    # Check if the EMR logs bucket exists and is accessible to the user
    try:
        logs_bucket_name = "aws-logs-%s-%s" % (cloud_account.get_account_id(),
                                               cloud_account.region_name)
        cloud_account.head_s3_bucket(logs_bucket_name)
    except AWSException as e:
        error = e.msg

    # Only get the bootstrap information from running or waiting clusters
    if state == "RUNNING" or state == "WAITING":
        try:
            bootstrap_actions = cloud_account.list_bootstrap_actions(cluster_id)["BootstrapActions"]
        except AWSException as e:
            error = e.msg

        if error is None:
            for action in bootstrap_actions:
                if action["Name"] == "jupyter-provision":
                    password = action["Args"][0]

    master_public_dns_name = None

    if "MasterPublicDnsName" in cluster_info:
        master_public_dns_name = cluster_info["MasterPublicDnsName"]

    if "Ec2InstanceAttributes" in cluster_info:
        if "EmrManagedMasterSecurityGroup" in cluster_info["Ec2InstanceAttributes"]:
            master_security_group = cluster_info["Ec2InstanceAttributes"][
                "EmrManagedMasterSecurityGroup"]
            # Check and open SSH port
            if not cloud_account.get_security_group_port_open(master_security_group, 22):
                cloud_account.authorize_security_group_ingress(master_security_group, 22, "SSH")
            # If emr:open-firewall in config.yml is True then open the extra ports in the firewall
            if bool(distutils.util.strtobool(str(config.config['emr']['open-firewall']))):
                # Check and open YARN ResourceManager port
                if not cloud_account.get_security_group_port_open(master_security_group, 8088):
                    cloud_account.authorize_security_group_ingress(master_security_group, 8088,
                                                                   "YARN ResourceManager")
                # Check and open Jupyter Notebook port
                if not cloud_account.get_security_group_port_open(master_security_group, 8888):
                    cloud_account.authorize_security_group_ingress(master_security_group, 8888,
                                                                   "Jupyter Notebook")
                # Check and open Spark HistoryServer port
                if not cloud_account.get_security_group_port_open(master_security_group, 18080):
                    cloud_account.authorize_security_group_ingress(master_security_group, 18080,
                                                                   "Spark HistoryServer")

    if "ssh_key" in credentials.credentials[account]:
        # Check if the file exists
        if os.path.isfile(credentials.credentials[account]["ssh_key"]):
            ssh_key = credentials.credentials[account]["ssh_key"]

    data = {
        'account': account,
        'cluster_name': cluster_info['Name'],
        'cluster_id': cluster_id,
        'master_url': master_public_dns_name,
        'state': state,
        'state_message': state_message,
        'password': password,
        'ssh_key': ssh_key,
        'master_public_dns_name': master_public_dns_name,
        'logs_bucket_name': logs_bucket_name
    }

    return render_template("emr-details.html", data=data, error=error)


@app.route('/destroy/<account>/<cluster_id>', methods=["POST"])
def destroy_cluster(account, cluster_id):
    cloud_account = AWS(credentials.credentials[account]["access_key_id"],
                        credentials.credentials[account]["secret_access_key"],
                        config.config["emr"]["region"])

    try:
        cloud_account.terminate_cluster(cluster_id)

        return redirect(url_for('cluster_list_create', account=account))
    except AWSException as e:
        data = {}
        return render_template("emr-details.html", data=data, error=e.msg)


@app.errorhandler(IOError)
def handle_ioerror(e):
    return str(e)


@app.errorhandler(botocore.exceptions.PartialCredentialsError)
@app.errorhandler(botocore.exceptions.EndpointConnectionError)
def handle_aws_client_connect(e):
    return str(e)
