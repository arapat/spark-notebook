#!/usr/bin/env python

import copy
import os
import spark_notebook.config
import unittest
import yaml
from flask import url_for
from spark_notebook.server import app


class SparkNotebookTestCase(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None

        self.temp_config_file = "/tmp/temp_config.yaml"
        self.temp_credentials_file = "/tmp/temp_credentials.yaml"

        # Delete the temp tests files
        if os.path.exists(self.temp_config_file):
            os.remove(self.temp_config_file)

        if os.path.exists(self.temp_credentials_file):
            os.remove(self.temp_credentials_file)

    def tearDown(self):
        pass

    def test_set_config_path(self):
        # The good config is the default_config with the credentials path changed
        good_config = copy.deepcopy(spark_notebook.config.default_config)
        good_config["credentials"]["path"] = self.temp_credentials_file

        with app.test_client() as c:
            c.get('/?config_path=%s' % self.temp_config_file)

            # Test saving the config file to a path that doesn't exist
            rv = c.post(url_for('save_config_location'), data=dict(path="bad/path.yaml"),
                        follow_redirects=True)

            assert '<strong>Error:</strong> Base directory bad does not exist.' \
                   in rv.data.decode('utf-8')

            # Test saving the config file to a path that does exist
            rv = c.post(url_for('save_config_location'), data=dict(path=self.temp_credentials_file),
                        follow_redirects=True)

            # Make sure there were no errors
            assert '<p class="error"><strong>Error:</strong>' not in rv.data.decode('utf-8')

            assert '<div class="flash">Credentials saved to %s</div>' % \
                   self.temp_credentials_file in rv.data.decode('utf-8')

            # Read the YAML from temp_config_file
            if os.path.isfile(self.temp_config_file):
                with open(self.temp_config_file, 'r') as stream:
                    test_config_yaml = yaml.load(stream)
            else:
                self.fail("Missing: %s " % self.temp_config_file)

            self.assertEqual(test_config_yaml, good_config)


if __name__ == '__main__':
    unittest.main()
