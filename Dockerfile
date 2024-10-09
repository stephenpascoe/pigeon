# A lightweight superset container to visualise Pigeon data.
#
# We are not using Superset's provided containers because we want a very minimal installation.

FROM ubuntu:24.04

RUN mkdir /opt/pigeon
WORKDIR /opt/pigeon

# Add key project files to the container
COPY pyproject.toml .
COPY pixi.lock .
COPY src .

# Install non-python build dependencies, install pixi and create the
# pixi environment
RUN apt-get update
RUN apt-get -y install curl g++
RUN curl -fsSL https://pixi.sh/install.sh | bash
ARG PATH=/root/.pixi/bin:${PATH}
RUN pixi install

# Configure superset
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

# Import datasources
COPY superset/dashboard_export.zip ./data
RUN pixi run superset import_dashboards -u admin -p /opt/pigeon/data/dashboard_export.zip

CMD ./startup.sh
