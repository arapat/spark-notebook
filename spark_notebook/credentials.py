import os
# noinspection PyPackageRequirements
import yaml
from spark_notebook.exceptions import CredentialsException


class Credentials:

    def __init__(self, file_path):
        self.credentials = dict()
        self.file_path = file_path
        self.load()

    def load(self):
        if os.path.isfile(self.file_path):
            with open(self.file_path, 'r') as stream:
                self.credentials = yaml.load(stream)

    def add(self, name, email_address, access_key_id, secret_access_key, key_name, ssh_key):
        new_credentials = dict()
        new_credentials[name] = dict(email_address=email_address,
                                     access_key_id=access_key_id,
                                     secret_access_key=secret_access_key,
                                     key_name=key_name,
                                     ssh_key=ssh_key)

        temp_credentials = dict(self.credentials).copy()
        temp_credentials.update(new_credentials)

        self.credentials = temp_credentials
        self.save()

    def save(self):
        # Check if the base directory exists
        if os.path.exists(os.path.dirname(self.file_path)):
            with open(self.file_path, 'w') as stream:
                stream.write(yaml.safe_dump(self.credentials, default_flow_style=False))
        else:
            raise CredentialsException("Base directory %s does not exist." %
                                       os.path.dirname(self.file_path))
