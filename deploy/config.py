import sys
import yaml

config_file_name = "./config.yaml"
default_config = {
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

config = default_config
with open(config_file_name, 'r') as stream:
    user_defined = yaml.load(stream)
merge_dict(config, user_defined)
