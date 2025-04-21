FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    smartmontools \
    mosquitto-clients \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip3 install -r requirements.txt pytest

COPY src/ /app/src/
COPY tests/ /app/tests/
