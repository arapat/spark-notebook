#!/usr/bin/env python

import unittest
from flask import url_for
from mock import patch
from spark_notebook.server import app
from tests import fake_boto


class SparkNotebookTestCase(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None

        self.test_config_file = "./tests/test_files/test_config.yaml"

    def tearDown(self):
        pass

    @patch('boto3.client', fake_boto.FakeBotoClient)
    @patch.object(fake_boto.FakeBotoClient, 'describe_cluster')
    @patch.object(fake_boto.FakeBotoClient, 'list_bootstrap_actions')
    def test_emr_details(self, mock_bootstrap_actions, mock_describe_cluster):
        with app.test_client() as c:
            c.get('/?config_path=%s' % self.test_config_file)

            #
            # Test a Staring Cluster
            #
            cluster_details = {
                'Cluster': {
                    'Id': 'J-starting-cluster',
                    'Name': 'starting-cluster',
                    'Status': {
                        'State': 'STARTING',
                    },
                    'MasterPublicDnsName': 'starting.cluster',
                }
            }

            mock_describe_cluster.return_value = cluster_details

            rv = c.get(url_for('cluster_details', account="test-4",
                               cluster_id="J-starting-cluster"))

            # Make sure there were no errors
            assert '<p class="error"><strong>Error:</strong>' not in rv.data.decode('utf-8')

            # Test the cluster information is correct
            assert 'Account: <a href="/g/test-4">test-4</a>' in rv.data.decode('utf-8')
            assert 'Cluster: starting-cluster' in rv.data.decode('utf-8')
            assert 'State: STARTING' in rv.data.decode('utf-8')

            # Test that the launching message is displayed
            assert '<p>Launching. Please refresh again later.' in rv.data.decode('utf-8')

            #
            # Test a Running Cluster
            #
            cluster_details = {
                'Cluster': {
                    'Id': 'J-running-cluster',
                    'Name': 'running-cluster',
                    'Status': {
                        'State': 'RUNNING',
                    },
                    'MasterPublicDnsName': 'running.cluster',
                }
            }

            bootstrap_actions = {
                'BootstrapActions': [{
                    'Name': 'jupyter-provision',
                    'Args': ['running-password']
                }]
            }

            mock_describe_cluster.return_value = cluster_details
            mock_bootstrap_actions.return_value = bootstrap_actions

            rv = c.get(url_for('cluster_details', account="test-4",
                               cluster_id="J-running-cluster"))

            # Make sure there were no errors
            assert '<p class="error"><strong>Error:</strong>' not in rv.data.decode('utf-8')

            # Test the cluster information is correct
            assert 'Account: <a href="/g/test-4">test-4</a>' in rv.data.decode('utf-8')
            assert 'Cluster: running-cluster' in rv.data.decode('utf-8')
            assert 'State: RUNNING' in rv.data.decode('utf-8')

            # Test that the MasterPublicDnsName is returned from cluster_details
            assert '<a target="_blank" href="http://running.cluster:8888">' \
                   'http://running.cluster:8888</a>' in rv.data.decode('utf-8')
            # Test that the password is returned from bootstrap_actions
            assert '<p><b>Notebook Password:</b> running-password</p>' in rv.data.decode('utf-8')
            # Test that the SSH command correct
            assert '<pre>ssh -i ./tests/test_files/identity_file.pem hadoop@running.cluster' \
                   '</pre></p>' in rv.data.decode('utf-8')

            #
            # Test a Terminated Cluster
            #
            cluster_details = {
                'Cluster': {
                    'Id': 'J-terminated-cluster',
                    'Name': 'terminated-cluster',
                    'Status': {
                        'State': 'TERMINATED',
                    },
                    'MasterPublicDnsName': 'terminated.cluster',
                }
            }

            mock_describe_cluster.return_value = cluster_details

            rv = c.get(url_for('cluster_details', account="test-4",
                               cluster_id="J-terminated-cluster"))

            # Make sure there were no errors
            assert '<p class="error"><strong>Error:</strong>' not in rv.data.decode('utf-8')

            # Test the cluster information is correct
            assert 'Account: <a href="/g/test-4">test-4</a>' in rv.data.decode('utf-8')
            assert 'Cluster: terminated-cluster' in rv.data.decode('utf-8')
            assert 'State: TERMINATED' in rv.data.decode('utf-8')


if __name__ == '__main__':
    unittest.main()
