#!/usr/bin/env python

import copy
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
                         'LogUri': 's3://aws-logs-123456789012-us-east-1/elasticmapreduce/',
                         'ReleaseLabel': 'emr-5.13.0',
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
                                                   'Path':
                                                       's3://mas-dse-emr/'
                                                       'jupyter-provision-v0.4.5.sh',
                                                   'Args': ["password"]}
                                               },
                                              {'Name': 'user-bootstrap-01',
                                               'ScriptBootstrapAction': {
                                                   'Path': 's3://test_bucket/test_script.sh',
                                                   'Args': []}
                                               }
                                              ],
                         'Steps': [],
                         'Tags': [{"Key": "cluster", "Value": "test-4@email"}]
                         }
        self.pyspark_python_3 = [
            {
                "Classification": "spark-env",
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties": {
                            "PYSPARK_PYTHON": "/usr/bin/python3",
                            "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3"
                        }
                    }
                ]
            },
            {
                "Classification": "spark-defaults",
                "Properties": {
                    "spark.yarn.appMasterEnv.PYSPARK_PYTHON": "/usr/bin/python3",
                    "spark.executorEnv.PYSPARK_PYTHON": "/usr/bin/python3"
                }
            }
        ]

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
            # Test launching a spot cluster with pyspark python 3
            #
            expected = copy.deepcopy(self.expected)

            expected["Name"] = u"cluster-1"
            expected["Instances"]["InstanceGroups"] = [{'InstanceCount': 1,
                                                        'Name': 'Master nodes',
                                                        'InstanceRole': 'MASTER',
                                                        'BidPrice': '1.0',
                                                        'InstanceType': u'r3.xlarge',
                                                        'Market': 'SPOT',
                                                        'Configurations': self.pyspark_python_3},
                                                       {'InstanceCount': 1,
                                                        'Name': 'Core nodes',
                                                        'InstanceRole': 'CORE',
                                                        'BidPrice': '1.0',
                                                        'InstanceType': u'r3.xlarge',
                                                        'Market': 'SPOT',
                                                        'Configurations': self.pyspark_python_3}]

            # Append bootstrap arg to specify Python 3
            expected["BootstrapActions"][0]["ScriptBootstrapAction"]["Args"].append("3")

            mock_run_job_flow_expected.return_value = expected

            rv = c.post(url_for('cluster_list_create', account="test-4"),
                        data=dict(name="cluster-1",
                                  password="password",
                                  worker_count="1",
                                  subnet_id="subnet-12345678",
                                  instance_type="r3.xlarge",
                                  use_spot="true",
                                  spot_price="1.0",
                                  bootstrap_path="s3://test_bucket/test_script.sh",
                                  pyspark_python_version="3"),
                        follow_redirects=True)

            # Make sure there were no errors
            assert '<p class="error"><strong>Error:</strong>' not in rv.data.decode('utf-8')

            assert '<div class="flash">Cluster launched: cluster-1</div>' in rv.data.decode('utf-8')
            assert '<p>Launching. Please refresh again later.' in rv.data.decode('utf-8')

            # Test that cluster-1 is in the cluster list
            assert 'Cluster: cluster-1'

            #
            # Test launching an on-demand cluster with pyspark python 2
            #
            expected = copy.deepcopy(self.expected)

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

            # Append bootstrap arg to specify Python 2
            expected["BootstrapActions"][0]["ScriptBootstrapAction"]["Args"].append("2")

            mock_run_job_flow_expected.return_value = expected

            rv = c.post(url_for('cluster_list_create', account="test-4"),
                        data=dict(name="cluster-2",
                                  password="password",
                                  worker_count="1",
                                  subnet_id="subnet-12345678",
                                  instance_type="r3.xlarge",
                                  bootstrap_path="s3://test_bucket/test_script.sh",
                                  pyspark_python_version="2"
                                  ),
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
