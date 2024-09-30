# A lightweight superset container to visualise Pigeon data.
#
# We are not using Superset's provided containers because we want a very minimal installation.

FROM ubuntu:24.04

RUN mkdir /opt/pigeon
WORKDIR /opt/pigeon

ADD pyproject.toml .
ADD pixi.lock .

RUN apt-get update
RUN apt-get -y install curl g++
RUN curl -fsSL https://pixi.sh/install.sh | bash
RUN /root/.pixi/bin/pixi install
