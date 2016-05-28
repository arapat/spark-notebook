import os
import sys
import yaml

config_file_name = "./credentials.yaml"
ec2_keys = ["aws-access-key-id", "aws-secret-access-key",
            "key-name", "identity-file"]
s3_keys = ["aws-access-key-id", "aws-secret-access-key"]


def load():
    if not os.path.exists(config_file_name):
        return {"ec2": {}, "s3": {}}
    with open(config_file_name, 'r') as stream:
        return yaml.load(stream)


def save(cred, usage):
    creds = load()
    if usage == "ec2":
        creds["ec2"][cred["name"]] = {key: cred[key] for key in ec2_keys}
    elif usage == "s3":
        creds["s3"][cred["name"]] = {key: cred[key] for key in s3_keys}
    with open(config_file_name, 'w') as stream:
        stream.write(yaml.dump(creds, default_flow_style=False))
    return creds
