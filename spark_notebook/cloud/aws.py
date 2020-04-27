import boto3
import botocore
import botocore.exceptions
import os
import socket
import time
from spark_notebook.exceptions import AWSException


class AWS:

    def __init__(self, access_key_id, secret_access_key, region_name):
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.region_name = region_name
        self.key_name = None
        self.identity_file = None

    def test_credentials(self):
        try:
            boto3.client('sts',
                         aws_access_key_id=self.access_key_id,
                         aws_secret_access_key=self.secret_access_key,
                         region_name=self.region_name).get_caller_identity()['Arn']
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "AuthFailure" or \
                    e.response["Error"]["Code"] == "InvalidClientTokenId":
                raise AWSException("Invalid AWS access key id or aws secret access key")
        except Exception as e:
            raise AWSException(str(e))

    def test_ssh_key(self, key_name, identity_file):
        client = None

        self.key_name = key_name
        self.identity_file = identity_file

        try:
            client = boto3.client('ec2',
                                  aws_access_key_id=self.access_key_id,
                                  aws_secret_access_key=self.secret_access_key,
                                  region_name=self.region_name)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "AuthFailure":
                raise AWSException("Invalid AWS access key id or aws secret access key")
        except Exception as e:
            raise AWSException("There was an error connecting to EC2: %s" % e)

        # Search EC2 for the key-name
        try:
            client.describe_key_pairs(KeyNames=[self.key_name])
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "AuthFailure":
                raise AWSException("Invalid AWS access key id or aws secret access key")
            elif e.response["Error"]["Code"] == "InvalidKeyPair.NotFound":
                raise AWSException("Key %s not found on AWS" % self.key_name)
            else:
                raise AWSException("There was an error describing the SSH key pairs: %s" %
                                   e.response["Error"]["Message"])

        # Verify the identity file exists
        if not os.path.isfile(self.identity_file):
            raise AWSException("Key identity file %s not found" % self.identity_file)

    def create_ssh_key(self, email_address, file_path):
        try:
            client = boto3.client('ec2',
                                  aws_access_key_id=self.access_key_id,
                                  aws_secret_access_key=self.secret_access_key,
                                  region_name=self.region_name)
        except Exception as e:
            raise AWSException("There was an error connecting to EC2: %s" % e)

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
                raise AWSException("Invalid AWS access key id or aws secret access key")
            else:
                raise AWSException("There was an error creating a new SSH key pair: %s" %
                                   e.response["Error"]["Message"])
        except Exception as e:
            raise AWSException("Unknown Error: %s" % e)

        # Verify the key pair was saved locally
        if not os.path.isfile(self.identity_file):
            raise AWSException("SSH key %s not saved" % self.identity_file)

    def get_subnets(self):
        try:
            client = boto3.client('ec2',
                                  aws_access_key_id=self.access_key_id,
                                  aws_secret_access_key=self.secret_access_key,
                                  region_name=self.region_name)
        except Exception as e:
            raise AWSException("There was an error connecting to EC2: %s" % e)

        # Search EC2 for the VPC subnets
        try:
            return client.describe_subnets()
        except botocore.exceptions.ClientError as e:
            raise AWSException("There was an error describing the VPC Subnets: %s" %
                               e.response["Error"]["Message"])
        except botocore.exceptions.ParamValidationError as e:
            raise AWSException("There was an error describing the VPC Subnets: %s" % e)

    def get_account_id(self):
        try:
            client = boto3.client('sts',
                                  aws_access_key_id=self.access_key_id,
                                  aws_secret_access_key=self.secret_access_key,
                                  region_name=self.region_name)
        except Exception as e:
            raise AWSException("There was an error connecting to EC2: %s" % e)

        try:
            return client.get_caller_identity()["Account"]
        except botocore.exceptions.ClientError as e:
            raise AWSException("There was an error getting the Account ID: %s" %
                               e.response["Error"]["Message"])

    def head_s3_bucket(self, bucket_name):
        try:
            client = boto3.client('s3',
                                  aws_access_key_id=self.access_key_id,
                                  aws_secret_access_key=self.secret_access_key,
                                  region_name=self.region_name)
        except Exception as e:
            raise AWSException("There was an error connecting to S3: %s" % e)

        try:
            client.head_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError as e:
            raise AWSException("There was an error getting the S3 bucket information (%s): %s" %
                               (bucket_name, e.response["Error"]["Message"]))

    def create_cluster(self, cluster_name, key_name, instance_type, worker_count, ec2_subnet_id,
                       instance_market, bid_price, user_bootstrap_path, pyspark_python_version,
                       tags, jupyter_password):
        # Latest known working version of EMR
        version = "emr-5.13.0"

        # Create the log_uri from the AWS account id and region_name
        log_uri = "s3://aws-logs-%s-%s/elasticmapreduce/" % \
                  (self.get_account_id(), self.region_name)

        # Fail if an ec2_subnet_id is not specified
        if ec2_subnet_id is None:
            raise AWSException("Subnet not specified")

        # Describe the compute instance groups
        if instance_market:
            market = "SPOT"
        else:
            market = "ON_DEMAND"

        master_instance_group = {'Name': "Master nodes",
                                 'Market': market,
                                 'InstanceRole': 'MASTER',
                                 'InstanceType': instance_type,
                                 'InstanceCount': 1,
                                 }

        core_instance_group = {'Name': "Core nodes",
                               'Market': market,
                               'InstanceRole': 'CORE',
                               'InstanceType': instance_type,
                               'InstanceCount': int(worker_count),
                               }

        if instance_market:
            master_instance_group["BidPrice"] = str(bid_price)
            core_instance_group["BidPrice"] = str(bid_price)

        if pyspark_python_version == "3":
            pyspark_python_3 = [
                {
                    "Classification": "spark-env",
                    "Configurations": [
                        {
                            "Classification": "export",
                            "Properties": {
                                "PYSPARK_PYTHON": "/usr/bin/python3",
                                "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3"
                            }
                        }
                    ]
                },
                {
                    "Classification": "spark-defaults",
                    "Properties": {
                        "spark.yarn.appMasterEnv.PYSPARK_PYTHON": "/usr/bin/python3",
                        "spark.executorEnv.PYSPARK_PYTHON": "/usr/bin/python3"
                    }
                }
            ]
            master_instance_group["Configurations"] = pyspark_python_3
            core_instance_group["Configurations"] = pyspark_python_3

        instance_groups = [master_instance_group, core_instance_group]

        # Describe the bootstrap actions
        bootstrap_actions = []

        # Default bootstrap action that is always used
        juypter_bootstrap_action = {
            'Name': 'jupyter-provision',
            'ScriptBootstrapAction': {
                'Path': 's3://mas-dse-emr/jupyter-provision-v0.4.5.sh',
                'Args': [
                    jupyter_password,
                    pyspark_python_version,
                ]
            }
        }
        bootstrap_actions.append(juypter_bootstrap_action)

        # User provided bootstrap actions
        if user_bootstrap_path is not None:
            user_bootstrap_action = {
                'Name': 'user-bootstrap-01',
                'ScriptBootstrapAction': {
                    'Path': user_bootstrap_path,
                    'Args': []
                }
            }
            bootstrap_actions.append(user_bootstrap_action)

        try:
            client = boto3.client('emr',
                                  aws_access_key_id=self.access_key_id,
                                  aws_secret_access_key=self.secret_access_key,
                                  region_name=self.region_name)
        except Exception as e:
            raise AWSException("There was an error connecting to EMR: %s" % e)

        # TODO: Make Core instance roles optional so a cluster can be launched with only a master
        # TODO: Add option to set EBS volume size
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
                        'Name': 'Hadoop'
                    },
                    {
                        'Name': 'Spark'
                    }
                ],
                Instances={
                    'Ec2KeyName': key_name,
                    'KeepJobFlowAliveWhenNoSteps': True,
                    'TerminationProtected': False,
                    'Ec2SubnetId': ec2_subnet_id,
                    'InstanceGroups': instance_groups,
                },
                Steps=[],
                BootstrapActions=bootstrap_actions,
                Tags=tags
            )

            return response['JobFlowId']

        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "AuthFailure":
                raise AWSException("Invalid AWS access key id or aws secret access key")
            else:
                raise AWSException("There was an error creating a new EMR cluster: %s" %
                                   e.response["Error"]["Message"])
        except Exception as e:
            raise AWSException("Unknown Error: %s" % e)

    def list_clusters(self):
        try:
            client = boto3.client('emr',
                                  aws_access_key_id=self.access_key_id,
                                  aws_secret_access_key=self.secret_access_key,
                                  region_name=self.region_name)
        except Exception as e:
            raise AWSException("There was an error connecting to EMR: %s" % e)

        try:
            cluster_list = client.list_clusters()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "AuthFailure":
                raise AWSException("Invalid AWS access key id or aws secret access key")
            else:
                raise AWSException("There was an error creating a new EMR cluster: %s" %
                                   e.response["Error"]["Message"])
        except Exception as e:
            raise AWSException("Unknown Error: %s" % e)

        return cluster_list

    def describe_cluster(self, cluster_id):
        try:
            client = boto3.client('emr',
                                  aws_access_key_id=self.access_key_id,
                                  aws_secret_access_key=self.secret_access_key,
                                  region_name=self.region_name)
        except Exception as e:
            raise AWSException("There was an error connecting to EMR: %s" % e)

        try:
            return client.describe_cluster(ClusterId=cluster_id)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "AuthFailure":
                raise AWSException("Invalid AWS access key id or aws secret access key")
            else:
                raise AWSException("There was an error describing the EMR cluster: %s" %
                                   e.response["Error"]["Message"])
        except Exception as e:
            raise AWSException("Unknown Error: %s" % e)

    def list_bootstrap_actions(self, cluster_id):
        try:
            client = boto3.client('emr',
                                  aws_access_key_id=self.access_key_id,
                                  aws_secret_access_key=self.secret_access_key,
                                  region_name=self.region_name)
        except Exception as e:
            raise AWSException("There was an error connecting to EMR: %s" % e)

        try:
            return client.list_bootstrap_actions(ClusterId=cluster_id)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "AuthFailure":
                raise AWSException("Invalid AWS access key id or aws secret access key")
            else:
                raise AWSException("There was an error describing the EMR cluster: %s" %
                                   e.response["Error"]["Message"])
        except Exception as e:
            raise AWSException("Unknown Error: %s" % e)

    def terminate_cluster(self, cluster_id):
        try:
            client = boto3.client('emr',
                                  aws_access_key_id=self.access_key_id,
                                  aws_secret_access_key=self.secret_access_key,
                                  region_name=self.region_name)
        except Exception as e:
            raise AWSException("There was an error connecting to EMR: %s" % e)

        try:
            client.terminate_job_flows(JobFlowIds=[cluster_id])
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "AuthFailure":
                raise AWSException("Invalid AWS access key id or aws secret access key")
            else:
                raise AWSException("There was an error terminating the EMR cluster: %s" %
                                   e.response["Error"]["Message"])
        except Exception as e:
            raise AWSException("Unknown Error: %s" % e)

    def get_security_group_port_open(self, security_group_id, port):
        try:
            client = boto3.client('ec2',
                                  aws_access_key_id=self.access_key_id,
                                  aws_secret_access_key=self.secret_access_key,
                                  region_name=self.region_name)
        except Exception as e:
            raise AWSException("There was an error connecting to EC2: %s" % e)

        try:
            response = client.describe_security_groups(GroupIds=[security_group_id])

            # Loop through all of the security group permissions and if the port
            for ip_permission in response["SecurityGroups"][0]["IpPermissions"]:
                if ip_permission["FromPort"] == port and ip_permission["ToPort"] == port:
                    return True
            return False
        except botocore.exceptions.ClientError as e:
            raise AWSException("There was an error describing the security group: %s" %
                               e.response["Error"]["Message"])

    def authorize_security_group_ingress(self, security_group_id, port, description):
        try:
            client = boto3.client('ec2',
                                  aws_access_key_id=self.access_key_id,
                                  aws_secret_access_key=self.secret_access_key,
                                  region_name=self.region_name)
        except Exception as e:
            raise AWSException("There was an error connecting to EC2: %s" % e)

        try:
            client.authorize_security_group_ingress(
                GroupId=security_group_id,
                IpPermissions=[
                    {'IpProtocol': 'tcp',
                     'FromPort': port,
                     'ToPort': port,
                     'IpRanges': [{'CidrIp': '0.0.0.0/0',
                                   'Description': description}]}
                ]
            )

        except botocore.exceptions.ClientError as e:
            raise AWSException("There was an error updating the security group: %s" %
                               e.response["Error"]["Message"])
