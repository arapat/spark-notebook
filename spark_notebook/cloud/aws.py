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
        self.cluster_id = None
        self.cluster_info = None
        self.cluster_list = None

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

    def create_cluster(self, cluster_name, key_name, instance_type, worker_count, instance_market,
                       bid_price, jupyter_password):
        error_message = None

        # TODO: Temp Vars
        log_uri = "s3://aws-logs-846273844940-us-east-1/elasticmapreduce/"
        version = "emr-5.2.0"

        if instance_market:
            market = "SPOT"
        else:
            market = "ON_DEMAND"

        try:
            client = boto3.client('emr',
                                  aws_access_key_id=self.access_key_id,
                                  aws_secret_access_key=self.secret_access_key,
                                  region_name=self.region_name)
        except Exception as e:
            error_message = "There was an error connecting to EMR: %s" % e
            return error_message

        # TODO: Add VPC (Ec2SubnetId) so m4 gen EC2 instances work
        # TODO: Make Core instance roles optional so a cluster can be launched with only a master
        # TODO: Add Tags
        # TODO: Remove BidPrice when not using spot instances
        try:
            response = client.run_job_flow(
                Name=cluster_name,
                LogUri=log_uri,
                ReleaseLabel=version,
                VisibleToAllUsers=True,
                JobFlowRole='EMR_EC2_DefaultRole',
                ServiceRole='EMR_DefaultRole',
                Applications=[
                    {
                        'Name': 'Spark'
                    },
                ],
                Instances={
                    'Ec2KeyName': key_name,
                    'KeepJobFlowAliveWhenNoSteps': True,
                    'TerminationProtected': False,
                    #'Ec2SubnetId': '<Your Subnet ID>',
                    'InstanceGroups': [
                        {
                            'Name': "Master nodes",
                            'Market': market,
                            'BidPrice': str(bid_price),
                            'InstanceRole': 'MASTER',
                            'InstanceType': instance_type,
                            'InstanceCount': 1,
                        },
                        {
                            'Name': "Slave nodes",
                            'Market': market,
                            'BidPrice': str(bid_price),
                            'InstanceRole': 'CORE',
                            'InstanceType': instance_type,
                            'InstanceCount': int(worker_count),
                        }
                    ],
                },
                Steps=[],
                BootstrapActions=[
                    {
                        'Name': 'jupyter-provision',
                        'ScriptBootstrapAction': {
                            'Path': 's3://mas-dse-emr/jupyter-provision.sh',
                            'Args': [
                                jupyter_password,
                            ]
                        }
                    },
                ],
                Tags=[
                    {
                        'Key': 'tag_name_1',
                        'Value': 'tab_value_1',
                    },
                    {
                        'Key': 'tag_name_2',
                        'Value': 'tag_value_2',
                    },
                ],
            )

            self.cluster_id = response['JobFlowId']

        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "AuthFailure":
                error_message = "Invalid AWS access key id or aws secret access key"
            else:
                error_message = "There was an error creating a new EMR cluster: %s" % \
                                e.response["Error"]["Message"]
        except Exception as e:
            error_message = "Unknown Error: %s" % e

        return error_message

    def list_clusters(self):
        error_message = None

        try:
            client = boto3.client('emr',
                                  aws_access_key_id=self.access_key_id,
                                  aws_secret_access_key=self.secret_access_key,
                                  region_name=self.region_name)
        except Exception as e:
            error_message = "There was an error connecting to EMR: %s" % e
            return error_message

        try:
            self.cluster_list = client.list_clusters()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "AuthFailure":
                error_message = "Invalid AWS access key id or aws secret access key"
            else:
                error_message = "There was an error creating a new EMR cluster: %s" % \
                                e.response["Error"]["Message"]
        except Exception as e:
            error_message = "Unknown Error: %s" % e

        return error_message

    def describe_cluster(self, cluster_id):
        error_message = None

        try:
            client = boto3.client('emr',
                                  aws_access_key_id=self.access_key_id,
                                  aws_secret_access_key=self.secret_access_key,
                                  region_name=self.region_name)
        except Exception as e:
            error_message = "There was an error connecting to EMR: %s" % e
            return error_message

        try:
            self.cluster_info = client.describe_cluster(ClusterId=cluster_id)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "AuthFailure":
                error_message = "Invalid AWS access key id or aws secret access key"
            else:
                error_message = "There was an error describing the EMR cluster: %s" % \
                                e.response["Error"]["Message"]
        except Exception as e:
            error_message = "Unknown Error: %s" % e

        return error_message

    def terminate_cluster(self, cluster_id):
        error_message = None

        try:
            client = boto3.client('emr',
                                  aws_access_key_id=self.access_key_id,
                                  aws_secret_access_key=self.secret_access_key,
                                  region_name=self.region_name)
        except Exception as e:
            error_message = "There was an error connecting to EMR: %s" % e
            return error_message

        try:
            self.cluster_info = client.terminate_job_flows(JobFlowIds=[cluster_id])
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "AuthFailure":
                error_message = "Invalid AWS access key id or aws secret access key"
            else:
                error_message = "There was an error terminating the EMR cluster: %s" % \
                                e.response["Error"]["Message"]
        except Exception as e:
            error_message = "Unknown Error: %s" % e

        return error_message
