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
        self.expected = {"Name": "",
                         'LogUri': 's3://aws-logs-846273844940-us-east-1/elasticmapreduce/',
                         'ReleaseLabel': 'emr-5.6.0',
                         'VisibleToAllUsers': True,
                         'JobFlowRole': 'EMR_EC2_DefaultRole',
                         'ServiceRole': 'EMR_DefaultRole',
                         'Applications': [{'Name': 'Hadoop'}, {'Name': 'Spark'}],
                         'Instances': {'KeepJobFlowAliveWhenNoSteps': True,
                                       'TerminationProtected': False,
                                       'Ec2SubnetId': 'subnet-12345678',
                                       'Ec2KeyName': 'key_name',
                                       'InstanceGroups': []
                                       },
                         'BootstrapActions': [{'Name': 'jupyter-provision',
                                               'ScriptBootstrapAction': {
                                                   'Path': 's3://mas-dse-emr/jupyter-provision.sh',
                                                   'Args': ["password"]}
                                               },
                                              ],
                         'Steps': [],
                         'Tags': [{"Key": "cluster", "Value": "test-4@email"}]
                         }

    def tearDown(self):
        pass

    @patch('boto3.client', fake_boto.FakeBotoClient)
    @patch.object(fake_boto.FakeBotoClient, 'set_run_job_flow_expected')
    def test_emr_list_create(self, mock_run_job_flow_expected):
        with app.test_client() as c:
            c.get('/?config_path=%s' % self.test_config_file)

            # Verifying the correct credentials file is being used from the self.test_config_file
            rv = c.get(url_for('accounts'))
            assert '<!-- ./tests/test_files/test_credentials.yaml -->' in rv.data.decode('utf-8')

            # Verifying that there are currently not clusters running
            rv = c.get(url_for('cluster_list_create', account="test-4"))
            assert '<p>No clusters are running.</p>' in rv.data.decode('utf-8')

            #
            # Test launching a spot cluster
            #
            expected = dict(self.expected).copy()

            expected["Name"] = u"cluster-1"
            expected["Instances"]["InstanceGroups"] = [{'InstanceCount': 1,
                                                        'Name': 'Master nodes',
                                                        'InstanceRole': 'MASTER',
                                                        'BidPrice': '1.0',
                                                        'InstanceType': u'r3.xlarge',
                                                        'Market': 'SPOT'},
                                                       {'InstanceCount': 1,
                                                        'Name': 'Core nodes',
                                                        'InstanceRole': 'CORE',
                                                        'BidPrice': '1.0',
                                                        'InstanceType': u'r3.xlarge',
                                                        'Market': 'SPOT'}]

            mock_run_job_flow_expected.return_value = expected

            rv = c.post(url_for('cluster_list_create', account="test-4"),
                        data=dict(name="cluster-1",
                                  password="password",
                                  worker_count="1",
                                  subnet_id="subnet-12345678",
                                  instance_type="r3.xlarge",
                                  use_spot="true",
                                  spot_price="1.0"),
                        follow_redirects=True)

            # Make sure there were no errors
            assert '<p class="error"><strong>Error:</strong>' not in rv.data.decode('utf-8')

            assert '<div class="flash">Cluster launched: cluster-1</div>' in rv.data.decode('utf-8')
            assert '<p>Launching. Please refresh again later.' in rv.data.decode('utf-8')

            # Test that cluster-1 is in the cluster list
            assert 'Cluster: cluster-1'

            #
            # Test launching an on-demand cluster
            #
            expected = dict(self.expected).copy()

            expected["Name"] = u"cluster-2"
            expected["Instances"]["InstanceGroups"] = [{'InstanceCount': 1,
                                                        'Name': 'Master nodes',
                                                        'InstanceRole': 'MASTER',
                                                        'InstanceType': u'r3.xlarge',
                                                        'Market': 'ON_DEMAND'},
                                                       {'InstanceCount': 1,
                                                        'Name': 'Core nodes',
                                                        'InstanceRole': 'CORE',
                                                        'InstanceType': u'r3.xlarge',
                                                        'Market': 'ON_DEMAND'}]

            mock_run_job_flow_expected.return_value = expected

            rv = c.post(url_for('cluster_list_create', account="test-4"),
                        data=dict(name="cluster-2",
                                  password="password",
                                  worker_count="1",
                                  subnet_id="subnet-12345678",
                                  instance_type="r3.xlarge"),
                        follow_redirects=True)

            # Make sure there were no errors
            assert '<p class="error"><strong>Error:</strong>' not in rv.data.decode('utf-8')

            assert '<div class="flash">Cluster launched: cluster-2</div>' in rv.data.decode('utf-8')
            assert '<p>Launching. Please refresh again later.' in rv.data.decode('utf-8')

            # Test that cluster-1 & cluster-2 are in the cluster list
            assert 'Cluster: cluster-1'
            assert 'Cluster: cluster-2'


if __name__ == '__main__':
    unittest.main()
