import copy
import os
import os.path
# noinspection PyPackageRequirements
import yaml

default_config = {
    "credentials": {
        "path": "./credential.yaml"
    },
    "emr": {
        "name": "demo-cluster",
        "worker-count": 1,
        "region": "us-east-1",
        "instance-type": "r4.2xlarge",
        "spot-price": 1.0,
        "open-firewall": True
    },
    "jupyter": {
        "password": "change-me-321"
    }
}


class Config:

    def __init__(self, file_path="./config.yaml"):
        self.config = dict()
        self.file_path = file_path
        self.load()

    def load(self):
        self.config = copy.deepcopy(default_config)

        if os.path.isfile(self.file_path):
            with open(self.file_path, 'r') as stream:
                file_yaml = yaml.load(stream)
                self.config["credentials"].update(file_yaml["credentials"])
                self.config["emr"].update(file_yaml["emr"])
                self.config["jupyter"].update(file_yaml["jupyter"])

    def save(self):
        with open(self.file_path, 'w') as stream:
            stream.write(yaml.safe_dump(self.config, default_flow_style=False))
