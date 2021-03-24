#!/usr/bin/env python3
"""
Generate a report in S3 consisting of a given model's parking availability
predictions for all parking zones in Columbus, at all times of day, for thirty
days before and after today's date.
"""
import argparse
import csv
from datetime import date, datetime, timedelta
from os import environ

from app import auth_provider, keeper_of_the_state, predictor

LOCAL_FILE_NAME = "report.csv"
S3_FILE_NAME = "reports/parking_predictions_daily.csv"

parser = argparse.ArgumentParser()
parser.add_argument("--model", help=f"The model to report on. Defaults to the current day's model: {keeper_of_the_state.historical_model_name(date.today())}")


def _annotate_predictions(predictions, date, report_time, model):
    for prediction in predictions:
        prediction["time"] = date
        prediction["report_time"] = report_time
        prediction["model"] = model

    return predictions


def _bucket_for_environment():
    s3 = auth_provider.authorized_s3_resource()
    environment = environ.get('SCOS_ENV', 'dev')
    return s3.Bucket(environment + '-parking-prediction-public')


def _beginning_of_day(day):
    return day.replace(hour=0, minute=0, second=0, microsecond=0)


if __name__ == "__main__":
    args = parser.parse_args()
    model = args.model or keeper_of_the_state.historical_model_name(date.today())
    keeper_of_the_state.warm_caches_synchronously([model])

    bucket = _bucket_for_environment()
    bucket.delete_objects(Delete={'Objects': [{"Key": S3_FILE_NAME}]})

    window_start = _beginning_of_day(datetime.now() - timedelta(days=30))
    window_end = _beginning_of_day(datetime.now() + timedelta(days=30))

    semihour_cursor = window_start
    report_run = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")

    with open(LOCAL_FILE_NAME, mode='w') as csv_file:
        fieldnames = ['zoneId', 'availabilityPrediction', 'time', 'report_time', 'model', 'supplierID']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        while semihour_cursor < window_end:
            semihour_cursor = semihour_cursor + timedelta(minutes=30)
            prediction_output = predictor.predict_formatted(semihour_cursor, 'All', model)
            _annotate_predictions(prediction_output, datetime.strftime(semihour_cursor, "%Y-%m-%d %H:%M:%S"), report_run, model)
            if prediction_output:
                for prediction in prediction_output:
                    writer.writerow(prediction)

    bucket.upload_file(LOCAL_FILE_NAME, S3_FILE_NAME, ExtraArgs={'ACL':'public-read'})
