import os
import sys
import yaml


class Credentials:
    ec2_keys = ["aws-access-key-id", "aws-secret-access-key",
                "key-name", "identity-file"]
    s3_keys = ["aws-access-key-id", "aws-secret-access-key"]

    def __init__(self, file_path):
        self.set_file_path(file_path)

    def __getitem__(self, args):
        return self.credentials[args]

    def set_file_path(self, file_path):
        self.file_path = file_path
        self.load()

    def load(self):
        try:
            with open(self.file_path, 'r') as stream:
                self.credentials = yaml.load(stream)
        except:
            self.credentials = {"ec2": {}, "s3": {}}

    def add(self, cred, usage):
        if usage == "ec2":
            self.credentials["ec2"][cred["name"]] = \
                {key: cred[key] for key in self.ec2_keys}
        elif usage == "s3":
            self.credentials["s3"][cred["name"]] =  \
                {key: cred[key] for key in self.s3_keys}
        with open(self.file_path, 'w') as stream:
            stream.write(yaml.dump(self.credentials, default_flow_style=False))
