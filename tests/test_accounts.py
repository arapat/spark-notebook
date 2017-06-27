#!/usr/bin/env python

import os
import unittest
import yaml
from flask import url_for
from mock import patch
from spark_notebook.server import app
from tests import fake_boto


class SparkNotebookTestCase(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None

        self.test_config_file = "./tests/test_files/test_accounts_config.yaml"
        self.temp_credentials_file = "/tmp/temp_credentials.yaml"
        self.good_credentials_file = "./tests/test_files/test_accounts_credentials.yaml"

        # Delete the tests credentials file
        if os.path.exists(self.temp_credentials_file):
            os.remove(self.temp_credentials_file)

        # Delete the test identity file
        if os.path.exists("/tmp/test-3_hostname_0.pem"):
            os.remove("/tmp/test-3_hostname_0.pem")

    def tearDown(self):
        pass

    @patch('boto3.client', fake_boto.FakeBotoClient)
    @patch('socket.gethostname')
    @patch('time.time')
    def test_accounts(self, mock_time, mock_get_hostname):
        with app.test_client() as c:
            c.get('/?config_path=%s' % self.test_config_file)

            # Test that no AWS accounts are present and that test_config_file was read properly
            # by verifying the temp credentials file is /tmp/temp_credentials.yaml
            rv = c.get(url_for('accounts'))

            assert '<p>No AWS accounts found.</p>' in rv.data.decode('utf-8')
            assert '<!-- /tmp/temp_credentials.yaml -->' in rv.data.decode('utf-8')

            # Test invalid AWS credentials
            rv = c.post(url_for('accounts'),
                        data=dict(name="bad-credentials",
                                  email_address="bad@email",
                                  access_key_id="bad_access_key_id",
                                  secret_access_key="bad_secret_access_key",
                                  ssh_key="existing",
                                  key_name="key_name",
                                  identity_file="./tests/test_files/identity_file.pem"),
                        follow_redirects=True)

            assert '<p class="error"><strong>Error:</strong> Invalid AWS access key id or aws ' \
                   'secret access key' in rv.data.decode('utf-8')

            # Test valid AWS credentials but invalid ssh key_name
            rv = c.post(url_for('accounts'),
                        data=dict(name="test-1",
                                  email_address="test-1@email",
                                  access_key_id="access_key_id",
                                  secret_access_key="secret_access_key",
                                  ssh_key="existing",
                                  key_name="bad_key_name",
                                  identity_file="./tests/test_files/identity_file.pem"),
                        follow_redirects=True)

            assert '<p class="error"><strong>Error:</strong> Key bad_key_name not found on AWS' \
                   in rv.data.decode('utf-8')

            # Test valid AWS credentials but invalid ssh_key file
            rv = c.post(url_for('accounts'),
                        data=dict(name="test-2",
                                  email_address="test-2@email",
                                  access_key_id="access_key_id",
                                  secret_access_key="secret_access_key",
                                  ssh_key="existing",
                                  key_name="key_name",
                                  identity_file="/invalid/ssh_key"),
                        follow_redirects=True)

            assert '<p class="error"><strong>Error:</strong> Key identity file /invalid/ssh_key ' \
                   'not found' in rv.data.decode('utf-8')

            # Test valid AWS credentials and generate ssh key
            mock_get_hostname.return_value = "hostname"
            mock_time.return_value = "0"

            rv = c.post(url_for('accounts'),
                        data=dict(name="test-3",
                                  email_address="test-3@email",
                                  access_key_id="access_key_id",
                                  secret_access_key="secret_access_key",
                                  ssh_key="generate"),
                        follow_redirects=True)

            # Make sure there were no errors
            assert '<p class="error"><strong>Error:</strong>' not in rv.data.decode('utf-8')

            # Verify account added message is displayed and the test-3 account is in account list
            assert '<div class="flash">Account test-3 added</div>' in rv.data.decode('utf-8')
            assert '<li><a href="/g/test-3">test-3</a></li>' in rv.data.decode('utf-8')

            # Test valid AWS credentials and valid SSH key
            rv = c.post(url_for('accounts'),
                        data=dict(name="test-4",
                                  email_address="test-4@email",
                                  access_key_id="access_key_id",
                                  secret_access_key="secret_access_key",
                                  ssh_key="existing",
                                  key_name="key_name",
                                  identity_file="./tests/test_files/identity_file.pem"),
                        follow_redirects=True)

            # Make sure there were no errors
            assert '<p class="error"><strong>Error:</strong>' not in rv.data.decode('utf-8')

            # Verify account added message is displayed and the test-3 & test-4 accounts are in
            # account list
            assert '<div class="flash">Account test-4 added</div>' in rv.data.decode('utf-8')
            assert '<li><a href="/g/test-3">test-3</a></li>' in rv.data.decode('utf-8')
            assert '<li><a href="/g/test-4">test-4</a></li>' in rv.data.decode('utf-8')

            # Verify the saved temp_credentials_file matches the expected output
            if os.path.isfile(self.temp_credentials_file):
                with open(self.temp_credentials_file, 'r') as stream:
                    temp_credentials_yaml = yaml.load(stream)
            else:
                self.fail("Missing: %s " % self.temp_credentials_file)

            if os.path.isfile(self.good_credentials_file):
                with open(self.good_credentials_file, 'r') as stream:
                    good_credentials_yaml = yaml.load(stream)
            else:
                self.fail("Missing: %s " % self.good_credentials_file)

            self.assertEqual(temp_credentials_yaml, good_credentials_yaml)


if __name__ == '__main__':
    unittest.main()
