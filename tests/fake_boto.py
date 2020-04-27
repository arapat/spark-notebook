import re
import botocore.exceptions


class FakeBotoClient(object):

    def __init__(self, *args, **kwargs):
        self.cluster_list = None

        if args[0] == "sts":
            if not kwargs["aws_access_key_id"] == "access_key_id" or \
                    not kwargs["aws_secret_access_key"] == "secret_access_key":
                error_response = {'Error': {'Code': 'AuthFailure'}}
                raise botocore.exceptions.ClientError(error_response, "sts")

    @staticmethod
    def get_caller_identity():
        return {"Arn": "arn", 'Account': '123456789012'}

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
    def set_run_job_flow_expected():
        return {'Name': u'expected-cluster'}

    def run_job_flow(self, *args, **kwargs):
        # set_run_job_flow_expected return value should be set from the test mock
        expected = self.set_run_job_flow_expected()

        if kwargs != expected:
            error_response = {'Error': {'Code': 'Failed'}}
            raise botocore.exceptions.ClientError(error_response, "emr")

        if self.cluster_list is None:
            self.cluster_list = {
                'Clusters': [{
                    'Id': 'J-' + expected["Name"],
                    'Name': expected["Name"],
                    'Status': {
                        'State': 'STARTING',
                    },
                }]
            }
        else:
            self.cluster_list["Clusters"].append({
                'Id': 'J-' + expected["Name"],
                'Name': expected["Name"],
                'Status': {
                    'State': 'STARTING',
                },
            })

        return {'JobFlowId': 'J-' + expected["Name"]}

    @staticmethod
    def describe_cluster(ClusterId):
        return {
            'Cluster': {
                'Id': 'J-expected-cluster',
                'Name': 'expected-cluster',
                'Status': {
                    'State': 'STARTING',
                },
                'MasterPublicDnsName': 'expected.cluster',
                'Ec2InstanceAttributes': {
                    'EmrManagedMasterSecurityGroup': 'sg-1234567a'
                }
            }
        }

    @staticmethod
    def list_bootstrap_actions(ClusterId):
        return {}

    def list_clusters(self):
        return self.cluster_list

    @staticmethod
    def describe_security_groups(*args, **kwargs):
        return {
            "SecurityGroups": [
                {
                    "IpPermissions": [
                        {
                            "FromPort": 22,
                            "ToPort": 22,
                        },
                        {
                            "FromPort": 8088,
                            "ToPort": 8088,
                        },
                        {
                            "FromPort": 8888,
                            "ToPort": 8888,
                        },
                    ]
                }
            ]
        }

    @staticmethod
    def authorize_security_group_ingress(*args, **kwargs):
        pass

    @staticmethod
    def head_bucket(*args, **kwargs):
        if re.match("^[a-zA-Z0-9.\-_]{1,255}$", kwargs["Bucket"]):
            return
        else:
            report = "Invalid bucket name \"%s\": Bucket name must match the regex " \
                     "\"^[a-zA-Z0-9.\-_]{1,255}$\"" % kwargs["Bucket"]
            raise botocore.exceptions.ParamValidationError(report=report)
