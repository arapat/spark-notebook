import copy
import os
import os.path
# noinspection PyPackageRequirements
import yaml

default_config = {
    "credentials": {
        "path": "./credential.yaml"
    },
    "providers": {
        "ec2": {
            "region": "us-east-1",
            "availability-zone": "us-east-1b",
            "instance-type": "r3.2xlarge",
            "instance-initiated-shutdown-behavior": "terminate",
            "ami": "ami-e12ac3f7",
            "user": "ec2-user",
            "spot-price": 1.0
        }
    },
    "services": {
        "spark": {
            "version": "2.1.0"
        },
        "hdfs": {
            "version": "2.7.2"
        }
    },
    "launch": {
        "name": "demo-cluster",
        "num-slaves": 1,
        "install-hdfs": True,
        "install-spark": True,
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
