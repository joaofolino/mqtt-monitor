FROM eclipse-mosquitto:2.0

COPY config/mosquitto.conf /mosquitto/config/mosquitto.conf
COPY config/passwd /mosquitto/config/passwd

RUN chown mosquitto:mosquitto /mosquitto/config/mosquitto.conf /mosquitto/config/passwd \
    && chmod 644 /mosquitto/config/mosquitto.conf /mosquitto/config/passwd
