import os
import sys
import yaml

from .credentials import Credentials

default_config = {
    "credentials": {
        "path": "./credential.yaml"
    },
    "providers": {
        "ec2": {
            "region": "us-east-1",
            "availability-zone": "us-east-1b",
            "instance-type": "r3.2xlarge",
            "ami": "ami-6869aa05",
            "user": "ec2-user",
            "spot-price": 1.0
        }
    },
    "services": {
        "spark": {
            "version": "2.0.0"
        },
        "hdfs": {
            "version": "2.7.2"
        }
    },
    "launch": {
        "name": "demo-cluster",
        "num-slaves": 1,
        "install-hdfs": True,
        "password": "change-me-321"
    }
}


class Config:
    def __init__(self, file_path="./config.yaml"):
        self.file_path = file_path
        self.load()

    def __getitem__(self, args):
        return self.config[args]

    def get(self, args, default):
        if args not in self.config:
            return default
        return self.config[args]

    def set_file_path(self, file_path):
        self.file_path = file_path
        self.load()

    def set_credentials_file_path(self, file_path):
        if os.path.exists(os.path.dirname(file_path)):
            self.credentials.set_file_path(file_path)
            self.config["credentials"]["path"] = file_path
            self.save()
            return [{
                "alert-type": "alert-success",
                "message": "Path saved"
            }]
        else:
            return [{
                "alert-type": "alert-danger",
                "message": "Path not found"
            }]

    def load(self):
        def merge_dict(tgt, src):
            for key in src:
                if key not in tgt:
                    tgt[key] = src[key]
                elif type(src[key]) is dict:
                    if type(tgt[key]) is not dict:
                        sys.exit("Config error: %s should be a %s." %
                                 (key, type(tgt[key])))
                    merge_dict(tgt[key], src[key])
                else:
                    if type(tgt[key]) is dict:
                        sys.exit("Config error: %s." % key)
                    tgt[key] = src[key]

        config = dict(default_config)
        with open(self.file_path, 'r') as stream:
            user_defined = yaml.load(stream)
        merge_dict(config, user_defined)
        self.config = config
        self.credentials = Credentials(config["credentials"]["path"])

    def save(self):
        with open(self.file_path, 'w') as stream:
            stream.write(yaml.dump(self.config, default_flow_style=False))
