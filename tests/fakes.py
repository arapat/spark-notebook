import botocore.exceptions


class FakeBotoClient(object):

    def __init__(self, *args, **kwargs):
        if args[0] == "sts":
            if not kwargs["aws_access_key_id"] == "access_key_id" or \
                    not kwargs["aws_secret_access_key"] == "secret_access_key":
                error_response = {'Error': {'Code': 'AuthFailure'}}
                raise botocore.exceptions.ClientError(error_response, "sts")

    @staticmethod
    def get_caller_identity():
        return {"Arn": "arn"}

    @staticmethod
    def create_key_pair(KeyName=""):
        return {"KeyMaterial": KeyName}

    @staticmethod
    def describe_key_pairs(KeyNames=[]):
        if "key_name" in KeyNames:
            pass
        elif any("test-3" in s for s in KeyNames):
            pass
        else:
            error_response = {'Error': {'Code': 'InvalidKeyPair.NotFound'}}
            raise botocore.exceptions.ClientError(error_response, "ec2")

    @staticmethod
    def describe_subnets(Filters=None):
        return {'Subnets': [{'SubnetId': 'subnet-12345678', 'AvailabilityZone': 'us-east-1a'},
                            {'SubnetId': 'subnet-abcdefgh', 'AvailabilityZone': 'us-east-1b'},
                            {'SubnetId': 'subnet-a1b2c3d4', 'AvailabilityZone': 'us-east-1c'}]
                }

    @staticmethod
    def run_job_flow(*args, **kwargs):
        expected = {'Name': u'cluster-1',
                    'LogUri': 's3://aws-logs-846273844940-us-east-1/elasticmapreduce/',
                    'ReleaseLabel': 'emr-5.6.0',
                    'VisibleToAllUsers': True,
                    'JobFlowRole': 'EMR_EC2_DefaultRole',
                    'ServiceRole': 'EMR_DefaultRole',
                    'Applications': [{'Name': 'Hadoop'}, {'Name': 'Spark'}],
                    'Instances': {'KeepJobFlowAliveWhenNoSteps': True,
                                  'TerminationProtected': False,
                                  'Ec2SubnetId': 'subnet-12345678',
                                  'Ec2KeyName': 'key_name',
                                  'InstanceGroups': [{'InstanceCount': 1,
                                                      'Name': 'Master nodes',
                                                      'InstanceRole': 'MASTER',
                                                      'BidPrice': '1.0',
                                                      'InstanceType': u'r3.xlarge',
                                                      'Market': 'SPOT'},
                                                     {'InstanceCount': 1,
                                                      'Name': 'Slave nodes',
                                                      'InstanceRole': 'CORE',
                                                      'BidPrice': '1.0',
                                                      'InstanceType': u'r3.xlarge',
                                                      'Market': 'SPOT'}]},
                    'BootstrapActions': [{'Name': 'jupyter-provision',
                                          'ScriptBootstrapAction': {
                                              'Path': 's3://mas-dse-emr/jupyter-provision.sh',
                                              'Args': ["password"]}
                                          },
                                         ],
                    'Steps': [],
                    'Tags': [{'Key': 'tag_name_1',
                              'Value': 'tab_value_1'},
                             {'Key': 'tag_name_2',
                              'Value': 'tag_value_2'}]}

        if kwargs != expected:
            error_response = {'Error': {'Code': 'Failed'}}
            raise botocore.exceptions.ClientError(error_response, "emr")

        return {'JobFlowId': 'J-ABC123ABC123'}

    @staticmethod
    def describe_cluster(ClusterId):
        return {
            'Cluster': {
                'Id': 'J-ABC123ABC123',
                'Name': 'cluster-1',
                'Status': {
                    'State': 'STARTING',
                },
                'MasterPublicDnsName': 'test.cluster.com',
            }
        }
