import pandas as pd
from pydantic import ValidationError

from app import keeper_of_the_state
from app.data_formats import APIPrediction, APIPredictionRequest
from app.model import ModelFeatures


def predict(input_datetime, zone_ids='All', model_tag='latest'):
    """
    Predict the availability of parking in all parking zones at a given
    time.

    Parameters
    ----------
    input_datetime : datetime.datetime
        The date and time at which parking meter availability should be
        predicted.
    zone_ids : str or collection of hashable, optional
        The parking zones where availability estimates are being requested.
        The default is 'All', which will result in availability predictions
        for all parking zones.
    model_tag : str, optional
        The identifier of the model parameters to use (default: 'latest').

    Returns
    -------
    dict of {str : float}
        A mapping of zone IDs to their predicted parking availability
        values. Parking availability is expressed as a ratio of available
        parking spots to total parking spots in each zone, represented as a
        float between 0 and 1.
    """
    if not during_hours_of_operation(input_datetime):
        predictions = {}
    else:
        try:
            predictions = keeper_of_the_state.provide_model(model_tag).predict(
                ModelFeatures.from_request(
                    APIPredictionRequest(
                        timestamp=input_datetime,
                        zone_ids=zone_ids
                    )
                )
            )
        except ValidationError as e:
            predictions = {}
    return predictions


def during_hours_of_operation(input_datetime):
    return input_datetime.weekday() < 6 and 8 <= input_datetime.hour < 22


def predict_with(models, input_datetime, zone_ids='All'):
    return pd.DataFrame(
        data={model: predict(input_datetime, zone_ids, model)
              for model in models}
    ).rename(
        columns=lambda model_tag: f'{model_tag}Prediction'
    ).assign(
        zoneId=[zone_id
                for zone_id in APIPredictionRequest(zone_ids=zone_ids).zone_ids
                if any(
                    zone_id in keeper_of_the_state.provide_model(model).supported_zones
                    for model in models
                )]
    ).to_dict('records')


def predict_formatted(input_datetime, zone_ids='All', model='latest'):
    """
    Predict the availability of parking in a list of parking zones at a given
    time, returning a list of predictions in the current prediction API format.

    Parameters
    ----------
    input_datetime : datetime.datetime
        The date and time at which parking meter availability should be
        predicted.
    zone_ids : str or collection of hashable, optional
        The parking zones where availability estimates are being requested. The
        default is 'All', which will result in availability predictions for all
        parking zones.
    model : str, optional
        The identifier of the model parameters to use (default: 'latest').

    Returns
    -------
    list of dict of {str : str or float}
        The predictions for each of the `zone_ids` converted to the current
        prediction API format.

    See Also
    --------
    APIPrediction : Defines the current prediction API record format
    """
    predictions = predict(input_datetime, zone_ids, model)
    return to_api_format(predictions)


def to_api_format(predictions):
    """
    Transform a dictionary of predictions into a list of outputs in API format.

    Parameters
    ----------
    predictions : dict of {str : float}
        A dictionary of `parking zone id -> availability prediction` pairs.

    Returns
    -------
    list of dict of {str : str or float}
        The predictions in `indexed_predictions` converted to the current
        prediction API format.

    See Also
    --------
    predict : Predict parking availability given feature lists
    APIPrediction : Defines the current prediction API record format
    """
    return [
        APIPrediction(
            zoneId=zone_id,
            availabilityPrediction=availability
        ).dict()
        for zone_id, availability in predictions.items()
    ]
