#!/usr/bin/env python

import unittest

if __name__ == '__main__':

    # use the default shared TestLoader instance
    test_loader = unittest.defaultTestLoader

    # use the basic test runner that outputs to sys.stderr
    test_runner = unittest.TextTestRunner()

    # automatically discover all tests in the current dir of the form test*.py
    test_suite = test_loader.discover('tests', pattern="test_*.py")

    # run the test suite
    results = test_runner.run(test_suite)

    if len(results.errors) == 0 and len(results.failures) == 0:
        exit(0)
    else:
        exit(1)
