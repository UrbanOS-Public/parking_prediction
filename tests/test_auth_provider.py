import builtins
from io import StringIO
from os import path

import hvac
from mockito import any

from app import auth_provider
from tests.fake_vault import FakeVaultClient


def test_get_credentials_returns_dictionary_with_id_and_key(when):
    access_key_id_from_vault = 'my_first_access_key_id'
    secret_access_key_from_vault = 'my_first_secret_key_value'
    fake_vault_client = FakeVaultClient(
        access_key_id_from_vault,
        secret_access_key_from_vault
    )

    when(hvac).Client(any).thenReturn(fake_vault_client)
    when(builtins).open(any).thenReturn(StringIO('hello world'))
    when(path).isfile(any).thenReturn(True)
        
    credentials = auth_provider.get_credentials.__wrapped__(
        vault_role='my-vault-role',
        vault_credentials_key='my_cred_key'
    )

    assert credentials == {
        'aws_access_key_id': access_key_id_from_vault,
        'aws_secret_access_key': secret_access_key_from_vault
    }


def test_get_credentials_returns_empty_dictionary_with_no_token_file():
    credentials = auth_provider.get_credentials.__wrapped__(
        vault_role='my-vault-role',
        vault_credentials_key='my_cred_key'
    )

    assert credentials == {}
