FROM python:3.8-buster

ENV DB_HOST=localhost
ENV DB_PORT=8086
ENV DB_NAME=fitbit
ENV DB_ORG=Home
ENV DB_TOKEN=0

ENV CLIENT_ID=0
ENV CLIENT_SECRET=0
ENV ACCESS_TOKEN=0
ENV REFRESH_TOKEN=0
ENV EXPIRES_AT=604800
ENV CALLBACK_URL=http://localhost:8080/
ENV UNITS=None

ENV CONFIG_PATH=.

RUN pip install fitbit influxdb-client

COPY api_poller.py /
RUN chmod +x /api_poller.py 

CMD ["/api_poller.py"]

