#!/usr/bin/env python

import unittest
from flask import url_for
from mock import patch
from spark_notebook.server import app
from tests import fakes


class SparkNotebookTestCase(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None

        self.test_config_file = "./tests/test_files/test_config.yaml"

    def tearDown(self):
        pass

    @patch('boto3.client', fakes.FakeBotoClient)
    def test_create_cluster(self):
        with app.test_client() as c:
            c.get('/?config_path=%s' % self.test_config_file)

            # Verifying the correct credentials file is being used from the self.test_config_file
            rv = c.get(url_for('accounts'))
            assert '<!-- ./tests/test_files/test_credentials.yaml -->' in rv.data.decode('utf-8')

            # Verifying that there are currently not clusters running
            rv = c.get(url_for('cluster_list_create', account="test-4"))
            assert '<p>No clusters are running.</p>' in rv.data.decode('utf-8')

            # Test invalid AWS credentials
            rv = c.post(url_for('cluster_list_create', account="test-4"),
                        data=dict(name="cluster-1",
                                  password="password",
                                  worker_count="1",
                                  instance_type="r3.xlarge",
                                  use_spot="true",
                                  spot_price="1.0"),
                        follow_redirects=True)

            assert '<div class="flash">Cluster launched: cluster-1</div>' in rv.data.decode('utf-8')
            assert '<p>Launching. Please refresh again later.' in rv.data.decode('utf-8')

            # TODO: Test that cluster-1 is in the cluster list


if __name__ == '__main__':
    unittest.main()
