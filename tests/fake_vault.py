from functools import partial

from dotmap import DotMap


class FakeVaultClient():
    def __init__(self, aki, sak):
        self.access_key_id = aki
        self.secret_access_key = sak

        self.secrets = DotMap({
            'kv': {
                'v1': {
                    'read_secret': partial(FakeVaultClient.read_secret, self)
                }
            }
        })

    def auth_kubernetes(self, _role, _jwt):
        return {}

    @staticmethod
    def read_secret(self, _path, mount_point='mp'):
        return {
            'data': {
                'aws_access_key_id': self.access_key_id,
                'aws_secret_access_key': self.secret_access_key
            }
        } 