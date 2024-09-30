# A lightweight superset container to visualise Pigeon data.
#
# We are not using Superset's provided containers because we want a very minimal installation.

FROM ubuntu:24.04

RUN mkdir /opt/pigeon
WORKDIR /opt/pigeon

COPY pyproject.toml .
COPY pixi.lock .
COPY src .

RUN apt-get update
RUN apt-get -y install curl g++
RUN curl -fsSL https://pixi.sh/install.sh | bash
ARG PATH=/root/.pixi/bin:${PATH}
RUN pixi install

ARG SUPERSET_CONFIG_PATH=/opt/pigeon/superset_config.py
ARG FLASK_APP=superset
RUN mkdir data
COPY superset/superset_config.py .

RUN pixi run superset db upgrade
RUN openssl rand -base64 12 >/opt/pigeon/data/ADMIN_PASSWORD
RUN pixi run superset fab create-admin \
    --username admin --firstname the --lastname admin --email "the.admin@example.com" \
    --password $(cat /opt/pigeon/data/ADMIN_PASSWORD)
RUN pixi run superset load_examples
RUN pixi run superset init

COPY superset/startup.sh .

CMD ./startup.sh
