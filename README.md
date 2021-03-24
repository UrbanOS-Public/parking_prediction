# Project Description

***Smart Columbus Parking Prediction***

This repository contains the code needed to train a model to make predictions about the availability of parking in the city of Columbus, Ohio. It includes notebooks describing the process taken to arrive at this model version, as well as a Dockerfile and helm charts needed to package and deploy it as an api to a Kubernetes cluster.

This code and model are specifically tuned to the parking situation in Columbus, but could be used as a base to build more generic parking availability models.

For an example of the data transformation Columbus used to turn parking transaction data into the parking occupancy data needed for this model, see the [parking prediction orchestrator](https://github.com/Datastillery/parking_prediction_orchestration) project.

# Prerequisites

Requires:
- Python >= 3.5
- Poetry
- Docker
- Helm (if using Kubernetes)

### Install Python dependencies
```bash
    pip3 install poetry
    poetry install --dev
```
If you are on OS X Catalina try this if `fbprophet` fails to install.
```bash
    pip3 install poetry
    brew install gcc@7
    CXX=/usr/local/Cellar/gcc@7/7.5.0_2/bin/g++-7 CC=/usr/local/Cellar/gcc@7/7.5.0_2/bin/gcc-7 poetry install
```

### Install MicroSoft ODBC Driver 17 for SQL Server
On macOS, this can be done using Homebrew as follows:
```bash
    brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
    brew update
    HOMEBREW_NO_ENV_FILTERING=1 ACCEPT_EULA=Y brew install msodbcsql17 mssql-tools
```

### Configure Notebooks
If you're planning on running the repository's notebooks, you'll want to enable
IPython widgets to avoid problems.
```bash
    poetry run jupyter nbextension enable --py widgetsnbextension
```

# Usage

## Building
While the app does not need to be built to be run locally, to deploy it somewhere a docker image will be required.

Step 1: Build Docker image:
```
docker build . -t parking_prediction:latest
```

Step 2: Run Docker image:
```
docker run parking_prediction:latest
```

## Testing
This python project uses pytest

### Running tests
```bash
poetry run pytest
```

## Execution

### Running Notebooks
With the above dependencies installed, the notebooks can be run using:
```sh
poetry run jupyter notebook
```

### Running the application locally
```bash
export QUART_APP=app:app
export QUART_DEBUG=true # if you want debug messages on slow calls, etc.
poetry run quart run
```

### Deploying to Kubernetes
Check the `chart/values.yaml` file for necessary configuration values. Add those values to a custom values file for deployments.

```sh
helm upgrade --install predictive-parking ./chart --values your_values.yaml
```

# Additional Notes

### Notes for Data Scientists
This repository has been architected in such a way there are only a few places
where changes need to be made in order to upgrade the parking availability
prediction model. These files are as follows:
- the `app.model` Python module. This is where you should implement all feature
  engineering code, model implementation code, etc. Specifically, the following
  classes must be defined
  - `ModelFeatures`: A `pydantic` model specifying all of the features expected
    by your model.
    - This class must also provide a static `from_request` method for
      converting `APIPredictionRequest` objects into `ModelFeatures`.
  - `ParkingAvailabilityModel`: This is the actual trained model. It should
    include a `predict` method that takes a `ModelFeatures` object `features`
    and returns prediction values as an iterable of `float`s where the `i`-th
    `float` gives the parking availability prediction (as a probability) for
    the parking zone with `i`-th ID in `features.zone_id`.
- the `train.py` script, which contains code to retrieve training data, train a
  model, compare its performance to its recent predecessors, and upload
  newly-trained models to cloud storage. When updating the model, changes may
  be necessary here to control
  - how features are derived from the retrieved dataset,
    - Ideally, this would be done by converting dataset records into
      `PredictionAPIRequest`s and calling `ModelFeatures.from_request` on the
      requests. If the training dataset diverges from the production data in
      structure, however, this can be an alternative location for said code.
  - the core training procedure,
  - how the model is packaged into a self-contained, serializable object for
    storage purposes.
  Other code modifications in `train.py` should only be necessary when a
  fundamental change has occurred in our data sources, how model performance is
  evaluated, etc.
- unit tests for `app.model` in `tests/test_model.py`
  - These should be largely left unmodified or expanded upon.

# Version History and Retention

**Status:** This project is in the release phase.

**Release Frequency:** This project is complete and will not be updated further beyond critical bug fixes

**Release History: See [CHANGELOG.md](CHANGELOG.md)**

**Retention:** Indefinitely

# License
This project is licensed under the Apache 2.0 License - see the `LICENSE.MD` for more details. 

# Contributions
Follow the guidelines in the [main organization repo](https://github.com/Datastillery/smartcitiesdata/wiki/Contribute)

# Contact Information

# Acknowledgements

## Contributors
- [Dr. Dan Moore](https://github.com/drmrd) for his data science expertise in refining the model and for his detailed notebooks explaining the process.
- [Yuxiao Zhao](https://github.com/kldyzyx) for the initial model design
- [Ben Brewer](https://github.com/bennyhat), [Tim Regan](https://github.com/LtChae) and the rest of the Smart Columbus OS team for turning this into a usable and performant API.
