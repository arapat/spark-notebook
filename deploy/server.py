import time
from operator import itemgetter

import credentials
from config import config
from SparkHelper import SparkHelper

from flask import Flask
from flask import redirect
from flask import request
from flask import render_template
from flask import url_for

app = Flask(__name__)
sh = SparkHelper()

process_nb = None


@app.route('/', methods=['GET', 'POST'])
def select_aws():
    if request.method == "POST":
        data = {key: str(request.form[key]) for key in credentials.ec2_keys}
        data['name'] = str(request.form['name'])
        data['identity-file'] = os.path.expanduser(data['identity-file'])
        credentials.save(data, 'ec2')
    return render_template('accounts.html',
                           clusters=credentials.load())


@app.route('/account/<account>', methods=['GET', 'POST'])
def open_account(account):
    sh.init_account(account)

    data = {
        'account': account,
        'account_name': account,
        'cluster_name': config['launch']['name'],
        'num_of_workers': str(config['launch']['num-slaves']),
        'password': config['launch']['password']
    }

    if request.method == "POST":
        if sh.ready is not None:
            return ("Error: There is already one cluster in "
                    "the launching process.")
        name, password, workers = (config['launch']['name'],
                                   config['launch']['password'],
                                   config['launch']['num-slaves'])
        if request.form["name"]:
            name = request.form["name"]
        if len(name.strip().split()) > 1:
            return ("Error: The cluster name cannot contain whitespaces.")
        if request.form["password"]:
            password = request.form["password"]
        if request.form["workers"]:
            try:
                workers = int(request.form["workers"])
            except ValueError:
                return "Error: Number of workers must be a numeric value."
        sh.launch_spark(name, num_of_workers=workers,
                        passwd=password)
        return redirect(url_for('open_account', account=account))

    data['clusters'] = sh.get_cluster_names()
    data['launching'] = False
    if sh.ready is not None:
        data['launching'] = True
        data['pname'] = sh.name
        data['timer'] = "%d seconds" % (int(time.time() - sh.timer))
        data['ready'] = sh.ready
        data['dead'] = sh.dead
    return render_template('clusters.html', data=data)


@app.route('/reset/<account>', methods=['POST'])
def reset_account(account):
    sh.name = ''
    sh.ready = None
    return redirect(url_for('open_account', account=account))


@app.route('/cluster/<account>/<cluster>', methods=["GET", "POST"])
def open_cluster(account, cluster):
    global process_nb

    sh.init_account(account)
    sh.init_cluster(cluster)

    if process_nb is None:
        process_nb = sh.check_notebook()
    else:
        if not process_nb.is_alive():
            process_nb = None

    data = {
        'account': account,
        'account_name': account,
        'cluster_name': cluster,
        'master_url': sh.master_url,
        'notebook-ready': process_nb is None
    }
    data['aws_access'] = ("ssh -i %s root@%s" %
                          (sh.KEY_IDENT_FILE, sh.master_url))

    if request.method == "POST":
        action = request.form['type']
        if action == 'upload':
            local = os.path.expanduser(request.form['local-path'])
            data['upload-log'] = sh.upload(
                local, request.form['remote-path'])
        elif action == 'download':
            local = os.path.expanduser(request.form['local-path'])
            data['download-log'] = sh.download(
                request.form['remote-path'], local)
            print 'log:', data['download-log']
        else:
            path = request.form['list-path']
            if path.strip() == '':
                path = '/root/ipython'
            data['files'] = sh.list_files(path)

    return render_template("cluster-settings.html", data=data)


@app.route('/destroy/<account>/<cluster>', methods=["POST"])
def destroy_cluster(account, cluster):
    sh.init_account(account)
    sh.init_cluster(cluster)
    sh.destroy()
    return redirect(url_for('open_account', account=account))