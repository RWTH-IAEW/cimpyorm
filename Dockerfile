FROM python:3.11

COPY requirements.txt dev-requirements.txt /
RUN pip install -r requirements.txt &&\
    pip install -r dev-requirements.txt

COPY . /app

RUN pip install /app/

WORKDIR app
