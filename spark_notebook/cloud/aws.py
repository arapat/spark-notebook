import boto3
import botocore
import botocore.exceptions
import os
import socket
import time


class AWS:

    def __init__(self, access_key_id, secret_access_key, region_name):
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.region_name = region_name
        self.key_name = None
        self.identity_file = None

    def test_credentials(self):

        error_message = None

        try:
            boto3.client('sts',
                         aws_access_key_id=self.access_key_id,
                         aws_secret_access_key=self.secret_access_key,
                         region_name=self.region_name).get_caller_identity()['Arn']
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "AuthFailure" or \
                            e.response["Error"]["Code"] == "InvalidClientTokenId":
                error_message = "Invalid AWS access key id or aws secret access key"
        except Exception as e:
            error_message = e

        return error_message

    def test_ssh_key(self, key_name, identity_file):
        error_message = None

        self.key_name = key_name
        self.identity_file = identity_file

        try:
            client = boto3.client('ec2',
                                  aws_access_key_id=self.access_key_id,
                                  aws_secret_access_key=self.secret_access_key,
                                  region_name=self.region_name)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "AuthFailure":
                error_message = "Invalid AWS access key id or aws secret access key"
        except Exception as e:
            error_message = "There was an error connecting to EC2: %s" % e
            return error_message

        # Search EC2 for the key-name
        try:
            client.describe_key_pairs(KeyNames=[self.key_name])
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "AuthFailure":
                error_message = "Invalid AWS access key id or aws secret access key"
            elif e.response["Error"]["Code"] == "InvalidKeyPair.NotFound":
                error_message = "Key %s not found on AWS" % self.key_name
            else:
                error_message = "There was an error describing the SSH key pairs: %s" % \
                                e.response["Error"]["Message"]

        # Verify the identity file exists
        if not os.path.isfile(self.identity_file):
            error_message = "Key identity file %s not found" % self.identity_file

        return error_message

    def create_ssh_key(self, email_address, file_path):
        error_message = None

        try:
            client = boto3.client('ec2',
                                  aws_access_key_id=self.access_key_id,
                                  aws_secret_access_key=self.secret_access_key,
                                  region_name=self.region_name)
        except Exception as e:
            error_message = "There was an error connecting to EC2: %s" % e
            return error_message

        self.key_name = "%s_%s_%s" % (str(email_address.split("@")[0]),
                                      str(socket.gethostname()),
                                      str(int(time.time())))

        self.identity_file = file_path + "/" + self.key_name + ".pem"

        # Create an EC2 key pair
        try:
            key = client.create_key_pair(KeyName=self.key_name)
            with open(self.identity_file, 'a') as out:
                out.write(key['KeyMaterial'] + '\n')
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "AuthFailure":
                error_message = "Invalid AWS access key id or aws secret access key"
            else:
                error_message = "There was an error creating a new SSH key pair: %s" % \
                                e.response["Error"]["Message"]
        except Exception as e:
            error_message = "Unknown Error: %s" % e

        # Verify the key pair was saved locally
        if not os.path.isfile(self.identity_file):
            error_message = "SSH key %s not saved" % self.identity_file

        return error_message
