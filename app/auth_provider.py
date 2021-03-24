from os import environ, path

import boto3
import botocore
import hvac
from cachetools import LRUCache, cached

DEFAULT_VAULT_URL = 'http://vault.vault:8200'
DEFAULT_TOKEN_FILE_PATH = '/var/run/secrets/kubernetes.io/serviceaccount/token'
VAULT_ROLE = environ.get('VAULT_ROLE', '')
VAULT_CREDENTIALS_KEY = environ.get('VAULT_CREDENTIALS_KEY', '')


@cached(cache=LRUCache(maxsize=128))
def get_credentials(vault_role, vault_credentials_key, vault_url=DEFAULT_VAULT_URL, token_file_path=DEFAULT_TOKEN_FILE_PATH):
    if path.isfile(token_file_path):
        client = hvac.Client(vault_url)
        f = open(token_file_path)
        jwt = f.read()
        client.auth_kubernetes(vault_role, jwt)
        response = client.secrets.kv.v1.read_secret(f"smart_city/aws_keys/{vault_credentials_key}", mount_point="secrets")

        return response['data']
    else:
        return {}


def authorized_s3_resource():
    credentials = get_credentials(
        vault_role=VAULT_ROLE,
        vault_credentials_key=VAULT_CREDENTIALS_KEY
    )
    config = botocore.config.Config(
        max_pool_connections=50,
    )
    session = boto3.Session(**credentials)
    return session.resource('s3', config=config)