FROM python:3.8 as base-python

RUN apt-get clean \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get -y update

RUN apt-get -y install nginx \
    && apt-get -y install python3-dev \
    && apt-get -y install libpcre3 libpcre3-dev \
    && apt-get -y install build-essential \
    && apt-get -y install unixodbc-dev \
    && apt-get -y install locales \
    && ACCEPT_EULA=Y apt-get -y install msodbcsql17 \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install 'poetry==1.0.9'

ADD pyproject.toml poetry.lock /

RUN poetry export -f requirements.txt -o requirements.txt \
    && pip3 install --requirement requirements.txt

COPY app /app
RUN chmod +x app/start.sh
RUN chmod +x app/train.sh


FROM base-python as test
COPY ./tests /tests
RUN apt-get -y update \
    && apt-get -y install libspatialindex-dev \
    && poetry install
RUN poetry run python -m pytest /tests


FROM base-python as production
COPY train.py report.py /

COPY nginx.conf /etc/nginx

WORKDIR /app
