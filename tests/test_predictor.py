from datetime import datetime

from app import predictor


def test_predict_results_are_ordered(with_warmup, all_valid_zone_ids):
    zone_ids = all_valid_zone_ids[:10:5]
    prediction_zone_ids = [
        prediction['zoneId']
        for prediction in predictor.predict_formatted(datetime(2020, 2, 8, 14), zone_ids)
    ]
    assert prediction_zone_ids == zone_ids


def test_predict_returns_availability_for_zone_ids_during_normal_hours(with_warmup):
    predictions = predictor.predict_formatted(datetime(2020, 2, 8, 13, 29, 0))

    assert predictions
    for prediction in predictions:
        assert 'zoneId' in prediction
        assert 'availabilityPrediction' in prediction


def test_predict_returns_no_predictions_after_hours(with_warmup):
    predictions = predictor.predict_formatted(datetime(2020, 2, 6, 22, 0, 0))

    assert len(predictions) == 0


def test_predict_returns_no_predictions_on_sundays(with_warmup):
    predictions = predictor.predict_formatted(datetime(2020, 2, 9, 12, 0, 0))

    assert len(predictions) == 0


def test_predict_for_provided_zone_ids(with_warmup, all_valid_zone_ids):
    zone_ids = all_valid_zone_ids[:12:6]
    predictions = predictor.predict_formatted(datetime(2020, 2, 8, 13, 29, 0), zone_ids)

    assert predictions
    for prediction in predictions:
        assert prediction['zoneId'] in zone_ids
