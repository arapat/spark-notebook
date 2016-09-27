import os
import time

from config import Config
from spark_helper import SparkHelper

from flask import Flask
from flask import redirect
from flask import request
from flask import render_template
from flask import url_for

app = Flask(__name__)
config = Config()
spark = SparkHelper(config)


def launch_new_cluster():
    if spark.get_setup_status() is not None:
        return ("Error: There is already one cluster in "
                "the launching process.")

    name, password, workers, instance, spot_price = (
        config['launch']['name'], config['launch']['password'],
        config['launch']['num-slaves'], config['ec2']['instance-type'],
        config['ec2']['spot-price'])

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


@app.route('/', methods=['GET', 'POST'])
def select_aws():
    credentials_status = []
    '''Show all available AWS accounts.'''
    if request.method == "POST":
        # Set a new config file path
        if request.form["type"] == "set-path":
            credentials_status = config.set_credentials_file_path(str(request.form["path"]))
        # Add a new AWS account
        elif request.form["type"] == "add-account":
            data = {key: str(request.form[key])
                    for key in config.credentials.ec2_keys}
            data['name'] = str(request.form['name'])
            data['identity-file'] = os.path.expanduser(data['identity-file'])
            credentials_status = config.credentials.add(data, 'ec2')
        # Invalid parameter
        else:
            pass

    return render_template('accounts.html',
                           clusters=config.credentials.credentials,
                           cred_path=config.credentials.file_path,
                           credentials_status=credentials_status)


@app.route('/account/<account>', methods=['GET', 'POST'])
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
        'spot_price': "%.2f" % config['ec2']['spot-price'],
        'instances_type': config['ec2']['instance-type'],
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
        data['ready'] = (status == spark.SUCCEED)
        data['dead'] = (status == spark.FAILED)
        data['launch-log'] = spark.get_setup_log()
    return render_template('clusters.html', data=data)


@app.route('/reset/<account>', methods=['POST'])
def reset_account(account):
    spark.name = ''
    spark.reset_spark_setup()
    return redirect(url_for('open_account', account=account))


@app.route('/cluster/<account>/<cluster>', methods=["GET", "POST"])
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
        'notebook-ready': status is None
    }
    data['aws_access'] = ("ssh -i %s root@%s" %
                          (spark.KEY_IDENT_FILE, spark.master_url))

    if request.method == "POST":
        action = request.form['type']

        # Upload files to AWS
        if action == 'upload':
            data['upload-log'] = spark.upload(
                os.path.expanduser(request.form['local-path']),
                request.form['remote-path'])
        # Download files from AWS
        elif action == 'download':
            data['download-log'] = spark.download(
                request.form['remote-path'],
                os.path.expanduser(request.form['local-path']))
        # List files in a remote directory, default: /root/ipython
        elif action == 'list':
            path = request.form['list-path'].strip()
            if not path:
                path = '/root/ipython'
            data['files'] = spark.list_files(path)
        # Set a new S3 credentials to the Jupyter Notebook on AWS
        elif action == 's3':
            usage, name = request.form["usage"], request.form["name"]
            spark.setup_s3(data["credentials"][usage][name])
            process_nb = spark.check_notebook(force=True)
            data["notebook-ready"] = False
            data["setup-s3"] = True
        # Invalid parameter
        else:
            pass

    return render_template("cluster-settings.html", data=data)


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
