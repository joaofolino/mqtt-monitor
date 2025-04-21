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
COPY tests/mqtt-monitor.conf /app/mqtt-monitor.conf

# Ensure config file is readable
RUN chmod 644 /app/mqtt-monitor.conf

ENV PYTHONPATH=/app/src
ENV CONFIG_PATH=/app/mqtt-monitor.conf
