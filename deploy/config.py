import sys
import yaml

from credentials import Credentials

default_config = {
    "credential": {
        "path": "./credential.yaml"
    },
    "ec2": {
        "user": "root",
        "region": "us-east-1",
        "zone": "us-east-1b",
        "instance-type": "r3.2xlarge",
        "spot-price": 0.5
    },
    "launch": {
        "name": "demo-cluster",
        "num-slaves": 2,
        "password": "change-me-321"
    }
}


class Config:
    def __init__(self):
        self.file_path = "./config.yaml"
        self.load()

    def __getitem__(self, args):
        return self.config[args]

    def set_file_path(self, file_path):
        self.file_path = file_path
        self.load()

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
