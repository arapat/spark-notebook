import os
import os.path

from .cloud.aws import AWS
from .config import Config
from .credentials import Credentials

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

        error = cloud_account.test_credentials()

        if error is None and ssh_key == "generate":
            error = cloud_account.create_ssh_key(email_address,
                                                 os.path.dirname(credentials.file_path))
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
                           credential_file=config.config["credentials"]["path"],
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
            return redirect(url_for('accounts'))

    return render_template('config.html',
                           cred_path=config.config["credentials"]["path"],
                           error=error)


@app.route('/g/<account>', methods=['GET', 'POST'])
def cluster_list_create(account):
    cloud_account = AWS(credentials.credentials[account]["access_key_id"],
                        credentials.credentials[account]["secret_access_key"],
                        config.config["emr"]["region"])

    error = cloud_account.list_clusters()

    # if request method is post then create the cluster
    if request.method == "POST":
        name = None
        password = None
        worker_count = None
        instance_type = None
        use_spot = None
        spot_price = None

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

        error = cloud_account.create_cluster(name, credentials.credentials[account]["key_name"],
                                             instance_type, worker_count, use_spot, spot_price,
                                             password)

        if error is None:
            flash("Cluster launched: %s" % name)
            return redirect(url_for('cluster_details', account=account,
                                    cluster_id=cloud_account.cluster_id))

    data = {
        'account': account,
        'account_name': account,
        'cluster_name': config.config['emr']['name'],
        'worker_count': str(config.config['emr']['worker-count']),
        'spot_price': "%.2f" % config.config['emr']['spot-price'],
        'instance_type': config.config['emr']['instance-type'],
        'password': config.config['jupyter']['password'],
    }

    return render_template('emr-list-create.html',
                           cluster_list=cloud_account.cluster_list,
                           data=data,
                           error=error)


@app.route('/g/<account>/<cluster_id>', methods=["GET", "POST"])
def cluster_details(account, cluster_id):
    cloud_account = AWS(credentials.credentials[account]["access_key_id"],
                        credentials.credentials[account]["secret_access_key"],
                        config.config["emr"]["region"])

    error = cloud_account.describe_cluster(cluster_id)

    master_public_dns_name = None

    if "MasterPublicDnsName" in cloud_account.cluster_info["Cluster"]:
        master_public_dns_name = cloud_account.cluster_info["Cluster"]["MasterPublicDnsName"]

    # TODO: Added Juypter notebook password and ssh key path (replace UPDATE)
    # TODO: Print EMR error message when status is TERMINATED_WITH_ERRORS
    data = {
        'account': account,
        'cluster_name': cloud_account.cluster_info['Cluster']['Name'],
        'cluster_id': cluster_id,
        'master_url': master_public_dns_name,
        'status': cloud_account.cluster_info['Cluster']['Status']['State'],
        'password': "UPDATE",
        'aws_access': ("ssh -i %s hadoop@%s" % ("UPDATE", master_public_dns_name))
    }

    return render_template("emr-details.html", data=data, error=error)


@app.route('/destroy/<account>/<cluster_id>', methods=["POST"])
def destroy_cluster(account, cluster_id):
    cloud_account = AWS(credentials.credentials[account]["access_key_id"],
                        credentials.credentials[account]["secret_access_key"],
                        config.config["emr"]["region"])

    error = cloud_account.terminate_cluster(cluster_id)

    if error is None:
        return redirect(url_for('cluster_list_create', account=account))
    else:
        data = {}
        return render_template("emr-details.html", data=data, error=error)
