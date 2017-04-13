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
