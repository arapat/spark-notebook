import os
import os.path
import time

from .cloud.aws import AWS
from .config import Config
from .credentials import Credentials
from .spark_helper import SparkHelper
from .spark_helper import IN_PROCESS
from .spark_helper import SUCCEED
from .spark_helper import FAILED

from flask import Flask
from flask import redirect
from flask import request
from flask import render_template
from flask import url_for
from flask import flash

app = Flask(__name__)
app.secret_key = 'some_secret'
config = Config()
credentials = Credentials(config.config["credentials"]["path"])
spark = SparkHelper(config)


def launch_new_cluster():
    if spark.get_setup_status() is not None:
        return ("Error: There is already one cluster in "
                "the launching process.")

    name, password, workers, instance, spot_price = (
        config['launch']['name'], config['launch']['password'],
        config['launch']['num-slaves'], config['providers']['ec2']['instance-type'],
        config['providers']['ec2']['spot-price'])

    # Cluster name
    if request.form["name"]:
        name = request.form["name"]
    if len(name.strip().split()) > 1:
        return ("Error: The cluster name cannot contain whitespaces.")

    # Cluster password
    if request.form["password"]:
        password = request.form["password"]

    # Number of workers
    if request.form["workers"]:
        try:
            workers = int(request.form["workers"])
        except ValueError:
            return "Error: Number of workers must be a numeric value."

    # Instance type
    if request.form["instances"]:
        instance = request.form["instances"]
    if not spark.is_valid_ec2_instance(instance):
        return ('"Error: EC2 Instance type "' + instance + '" ' +
                "is invalid or not supported.")

    # Spot price: spot or on-demand
    if "spot" not in request.form or request.form["spot"] != "yes":
        spot_price = None
    elif request.form["spot-price"]:
        try:
            spot_price = float(request.form["spot-price"])
        except ValueError:
            return "Error: Spot price must be a numeric value."

    spark.setup_cluster(name, num_of_workers=workers, instance=instance,
                        spot_price=spot_price, passwd=password)


@app.route('/', methods=['GET'])
def main():

    if "config_path" in request.args:
        global config
        global credentials
        config = Config(file_path=request.args.get('config_path'))
        credentials = Credentials(config.config["credentials"]["path"])
        flash("Using config file: %s" % config.file_path)

    if os.path.isfile(config.config["credentials"]["path"]):
        return redirect(url_for('select_account'))
    else:
        return redirect(url_for('save_config_location'))


@app.route('/accounts', methods=['GET'])
def select_account():

    return render_template('accounts.html',
                           accounts=credentials.credentials,
                           credential_file=config.config["credentials"]["path"])


@app.route('/accounts', methods=['POST'])
def add_account():
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

    region_name = config.config["providers"]["ec2"]["region"]

    cloud_account = AWS(access_key_id, secret_access_key, region_name)

    error = cloud_account.test_credentials()

    if error is None and ssh_key == "generate":
        error = cloud_account.create_ssh_key(email_address, os.path.dirname(credentials.file_path))
        key_name = cloud_account.key_name
        identity_file = cloud_account.identity_file

    if error is None:
        error = cloud_account.test_ssh_key(key_name, identity_file)

    if error is None:
        error = credentials.add(name, email_address, access_key_id, secret_access_key, key_name,
                                identity_file)

    if error is None:
        flash("Account %s added" % name)

    return render_template('accounts.html',
                           accounts=credentials.credentials,
                           error=error)


@app.route('/config', methods=['GET', 'POST'])
def save_config_location():
    error = None

    if request.method == 'POST':
        path = request.form['path'].encode('utf8').decode()

        global credentials
        credentials = Credentials(path)
        error = credentials.save()

        # If there were no errors saving the credentials file then update the credentials path in
        # the config file
        if error is None:
            config.config["credentials"]["path"] = path
            config.save()
            flash("Credentials saved to %s" % path)
            return redirect(url_for('select_account'))

    return render_template('config.html',
                           cred_path=config.config["credentials"]["path"],
                           error=error)


@app.route('/g/<account>', methods=['GET', 'POST'])
def open_account(account):
    '''Open AWS account info page'''
    spark.init_account(account)

    if request.method == "POST":
        msg = launch_new_cluster()
        # Should return None if nothing is wrong
        if msg:
            return msg
        return redirect(url_for('open_account', account=account))

    data = {
        'account': account,
        'account_name': account,
        'cluster_name': config['launch']['name'],
        'num_of_workers': str(config['launch']['num-slaves']),
        'spot_price': "%.2f" % config['providers']['ec2']['spot-price'],
        'instances_type': config['providers']['ec2']['instance-type'],
        'password': config['launch']['password']
    }
    try:
        data['clusters'] = spark.get_cluster_names()
    except Exception as e:
        return e.message
    data['launching'] = False
    status = spark.get_setup_status()
    if status is not None:
        data['launching'] = True
        data['pname'] = spark.name
        data['timer'] = "%d seconds" % spark.get_setup_duration()
        data['ready'] = (status == SUCCEED)
        data['dead'] = (status == FAILED)
        data['launch-log'] = spark.get_setup_log()
    return render_template('clusters.html', data=data)


@app.route('/g/<account>/<cluster>', methods=["GET", "POST"])
def open_cluster(account, cluster):
    '''Open cluster info page'''
    spark.init_account(account)
    spark.init_cluster(cluster)

    status = spark.check_notebook()
    data = {
        'account': account,
        'credentials': config.credentials.credentials,
        'account_name': account,
        'cluster_name': cluster,
        'master_url': spark.master_url,
        'notebook-ready': status is None,
        'password': config['launch']['password']
    }
    data['aws_access'] = ("ssh -i %s %s@%s" %
                          (spark.KEY_IDENT_FILE, config['providers']['ec2']['user'],
                           spark.master_url))

    if request.method == "POST":
        if request.form['type'] == 's3':
            usage, name = request.form["usage"], request.form["name"]
            spark.setup_s3(data["credentials"][usage][name])
            process_nb = spark.check_notebook(force=True)
            data["notebook-ready"] = False
            data["setup-s3"] = True
        # Invalid parameter
        else:
            pass

    return render_template("cluster-settings.html", data=data)


@app.route('/reset/<account>', methods=['POST'])
def reset_account(account):
    spark.name = ''
    spark.reset_spark_setup()
    return redirect(url_for('open_account', account=account))


@app.route('/destroy/<account>/<cluster>', methods=["POST"])
def destroy_cluster(account, cluster):
    spark.init_account(account)
    spark.init_cluster(cluster)
    spark.destroy()
    return redirect(url_for('open_account', account=account))


@app.route("/addcred/<account>/<cluster>", methods=["POST"])
def add_s3_cred(account, cluster):
    data = {key: str(request.form[key]) for key in config.credentials.s3_keys}
    data['name'] = str(request.form['name'])
    config.credentials.add(data, 's3')
    return redirect(url_for('open_cluster', account=account, cluster=cluster))
