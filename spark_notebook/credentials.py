import os
# noinspection PyPackageRequirements
import yaml


class Credentials:

    def __init__(self, file_path):
        self.credentials = dict()
        self.file_path = file_path
        self.load()

    def load(self):
        error_message = None

        if os.path.isfile(self.file_path):
            try:
                with open(self.file_path, 'r') as stream:
                    self.credentials = yaml.load(stream)
            except IOError as e:
                error_message = e
        if self.credentials is None:
            self.credentials = dict()

        return error_message

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
        error_message = self.save()

        return error_message

    def save(self):
        error_message = None

        # Check if the base directory exists
        if os.path.exists(os.path.dirname(self.file_path)):
            try:
                with open(self.file_path, 'w') as stream:
                    stream.write(yaml.safe_dump(self.credentials, default_flow_style=False))
            except IOError as e:
                error_message = e
        else:
            error_message = "Base directory %s does not exist." % \
                            os.path.dirname(self.file_path)

        return error_message
