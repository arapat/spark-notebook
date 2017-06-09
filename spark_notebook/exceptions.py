#!/usr/bin/env python


class AWSException(Exception):

    def __init__(self, arg):
        self.msg = arg

    def __str__(self):
        return self.msg


class CredentialsException(Exception):

    def __init__(self, arg):
        self.msg = arg

    def __str__(self):
        return self.msg
