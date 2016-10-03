import boto3
import botocore
import os
import socket
import time
import yaml

ec2_region = "us-east-1"


class Credentials:
    ec2_keys = ["aws-access-key-id", "aws-secret-access-key",
                "key-name", "identity-file", "email-address", "ssh-key"]
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
        add_status = []

        if usage == "ec2":
            # Generate an ssh key pair if the option was selected
            if cred["ssh-key"] == "generate":
                key_name, identity_file, ssh_key_status = \
                    self.create_ssh_key(cred["aws-access-key-id"],
                                        cred["aws-secret-access-key"],
                                        cred["email-address"])
                add_status = add_status + ssh_key_status
                # If no key was created then don't save the credentials and return immediately
                if key_name is None or identity_file is None:
                    return add_status
                self.credentials["ec2"][cred["name"]] = {
                    "aws-access-key-id": cred["aws-access-key-id"],
                    "aws-secret-access-key": cred["aws-secret-access-key"],
                    "key-name": key_name,
                    "email-address": cred["email-address"],
                    "identity-file": identity_file
                }
            else:
                # Record form the values if the option to generate an ssh key pair was not selected
                self.credentials["ec2"][cred["name"]] = \
                    {key: cred[key] for key in self.ec2_keys}

            # Test if AWS credentials are valid
            valid, test_credentials_status = self.test_ec2_credentials(cred["name"])

            # If the credentials are invalid then don't save the credentials and return immediately
            if not valid:
                add_status = add_status + test_credentials_status
                return add_status
        elif usage == "s3":
            self.credentials["s3"][cred["name"]] =  \
                {key: cred[key] for key in self.s3_keys}
        with open(self.file_path, 'w') as stream:
            stream.write(yaml.dump(self.credentials, default_flow_style=False))

        add_status = add_status + [{
            "alert-type": "alert-success",
            "message": "AWS Account Added"
        }]
        return add_status

    def test_ec2_credentials(self, name):
        test_credentials_status = []
        
        aws_access_key_id = self.credentials["ec2"][name]["aws-access-key-id"]
        aws_secret_access_key = self.credentials["ec2"][name]["aws-secret-access-key"]

        try:
            client = boto3.client('ec2',
                                  aws_access_key_id=aws_access_key_id,
                                  aws_secret_access_key=aws_secret_access_key,
                                  region_name=ec2_region)
        except Exception as e:
            test_credentials_status.append({
                "alert-type": "alert-danger",
                "message": "There was an error connecting to EC2: %s" % e
            })

        # Test the AWS Key and Secret and search EC2 for the key-name
        try:
            client.describe_key_pairs(KeyNames=[self.credentials["ec2"][name]["key-name"]])
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "AuthFailure":
                test_credentials_status.append({
                    "alert-type": "alert-danger",
                    "message": "Invalid AWS Access Key ID or AWS Secret Access Key"
                })
            elif e.response["Error"]["Code"] == "InvalidKeyPair.NotFound":
                test_credentials_status.append({
                    "alert-type": "alert-danger",
                    "message": "Key Name not found on AWS"
                })
            else:
                test_credentials_status.append({
                    "alert-type": "alert-danger",
                    "message": "There was an error describing the SSH key pairs: %s" %
                               e.response["Error"]["Code"]
                })

        # Verify the identity file exists
        if not os.path.isfile(self.credentials["ec2"][name]["identity-file"]):
            test_credentials_status.append({
                "alert-type": "alert-danger",
                "message": "Key Identity File not found"
            })

        if len(test_credentials_status) > 0:
            return False, test_credentials_status
        else:
            return True, test_credentials_status

    def create_ssh_key(self, aws_access_key_id, aws_secret_access_key, email_address):
        ssh_key_status = []

        try:
            client = boto3.client('ec2',
                                  aws_access_key_id=aws_access_key_id,
                                  aws_secret_access_key=aws_secret_access_key,
                                  region_name=ec2_region)
        except Exception as e:
            ssh_key_status.append({
                "alert-type": "alert-danger",
                "message": "There was an error connecting to EC2: %s" % e
            })
            return None, None, ssh_key_status

        ec2_ssh_key_name = "%s_%s_%s" % (str(email_address.split("@")[0]),
                                         str(socket.gethostname()),
                                         str(int(time.time())))

        ec2_ssh_key_pair_file = os.path.dirname(os.path.abspath(self.file_path)) + \
                                "/" + ec2_ssh_key_name + ".pem"

        # Create an EC2 key pair
        try:
            key = client.create_key_pair(KeyName=ec2_ssh_key_name)
            with open(ec2_ssh_key_pair_file, 'a') as out:
                out.write(key['KeyMaterial'] + '\n')
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "AuthFailure":
                ssh_key_status.append({
                    "alert-type": "alert-danger",
                    "message": "Invalid AWS Access Key ID or AWS Secret Access Key"
                })
            else:
                ssh_key_status.append({
                    "alert-type": "alert-danger",
                    "message": "There was an error creating a new SSH key pair: %s" %
                               e.response["Error"]["Code"]
                })
            return None, None, ssh_key_status
        except Exception as e:
            ssh_key_status.append({
                "alert-type": "alert-danger",
                "message": "Unknown Error: %s" % e
            })
            return None, None, ssh_key_status

        # Verify the key pair was saved locally
        if not os.path.isfile(ec2_ssh_key_pair_file):
            ssh_key_status.append({
                "alert-type": "alert-danger",
                "message": "SSH key not saved"
            })
            return None, None, ssh_key_status

        return ec2_ssh_key_name, ec2_ssh_key_pair_file, ssh_key_status
