#!/usr/bin/env python

import os
import unittest
from flask import url_for
from spark_notebook.server import app


class SparkNotebookTestCase(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None

        self.temp_config_file = "/tmp/temp_config.yaml"

        # Delete the temp tests files
        if os.path.exists(self.temp_config_file):
            os.remove(self.temp_config_file)

    def tearDown(self):
        pass

    def test_root_redirect(self):
        with app.test_client() as c:
            # Test the root redirect for a config file that doesn't exist
            rv = c.get('/?config_path=%s' % self.temp_config_file)
            assert '<a href="%s">%s</a>' % (url_for('save_config_location'),
                                            url_for('save_config_location')) in \
                   rv.data.decode('utf-8')

            # Test the root redirect for a config file that does exist
            rv = c.get('/?config_path=./tests/test_files/test_config.yaml')
            assert '<a href="%s">%s</a>' % (url_for('accounts'),
                                            url_for('accounts')) in \
                   rv.data.decode('utf-8')


if __name__ == '__main__':
    unittest.main()
