import importlib

import pytest

import app.fybr.zone_info


def test_zone_info_module_is_deprecated():
    with pytest.deprecated_call() as warning_messages:
        importlib.reload(app.fybr.zone_info)

        assert len(warning_messages.list) == 1

        warning = warning_messages.list[0]
        expected_warning = ('The zone_info module is no longer in service and '
                            'will be removed soon™.')
        assert warning.message.args[0] == expected_warning