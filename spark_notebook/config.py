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
        "instance-type": "r3.2xlarge",
        "spot-price": 1.0
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
        if os.path.isfile(self.file_path):
            try:
                with open(self.file_path, 'r') as stream:
                    self.config = yaml.load(stream)
            except IOError as e:
                print(e)
        else:
            self.config = copy.deepcopy(default_config)

    def save(self):
        try:
            with open(self.file_path, 'w') as stream:
                stream.write(yaml.safe_dump(self.config, default_flow_style=False))
        except IOError as e:
            print(e)
