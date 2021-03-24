"""
Responsible for defining the model backend for prediction requests, including
how those requests are converted into features that the current parking
availability predictor accepts.
"""
from app._models.moving_average import AvailabilityAverager, AverageFeatures

ModelFeatures = AverageFeatures
ParkingAvailabilityModel = AvailabilityAverager