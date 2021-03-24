from functools import wraps
from typing import Iterable, Type

import hypothesis.strategies as st
from hypothesis import given
from pydantic import BaseModel

from app.data_formats import APIPredictionRequest
from tests.conftest import ALL_VALID_ZONE_IDS


def partially_constructed_model_strategy(
    model_cls: Type[BaseModel],
    unrestricted_parameters: Iterable
) -> st.SearchStrategy:
    restricted_parameters = {
        parameter_name: st.from_type(parameter.annotation)
        for parameter_name, parameter in model_cls.__signature__.parameters.items()
        if parameter_name not in unrestricted_parameters
    }

    def model_cls_restricted_constructor_constructor(**restricted_parameters):
        @wraps(model_cls.__init__)
        def model_cls_restricted_constructor(**unrestricted_parameters):
            return model_cls(**unrestricted_parameters, **restricted_parameters)
        return model_cls_restricted_constructor

    return st.builds(model_cls_restricted_constructor_constructor, **restricted_parameters)


@given(
    model_constructor=partially_constructed_model_strategy(
        APIPredictionRequest,
        unrestricted_parameters={'zone_ids'}
    ),
    zone_ids=st.lists(
        st.sampled_from(ALL_VALID_ZONE_IDS),
        min_size=1,
        max_size=20
    )
)
def test_valid_zone_ids_are_preserved_in_order_within_requests_without_duplication(with_warmup, model_constructor, zone_ids):
    request = model_constructor(zone_ids=zone_ids)

    added = set()
    add = added.add
    unique_zone_ids_in_order = [zone_id for zone_id in zone_ids
                                if not (zone_id in added or add(zone_id))]
    assert request.zone_ids == unique_zone_ids_in_order