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
                'Id': 'J-cluster-1',
                'Name': 'cluster-1',
                'Status': {
                    'State': 'STARTING',
                },
                'MasterPublicDnsName': 'test.cluster.com',
            }
        }

    def list_clusters(self):
        return self.cluster_list
