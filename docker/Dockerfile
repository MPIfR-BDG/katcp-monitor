FROM python:2.7-alpine
RUN apk upgrade -U -a && \
    apk add git && \
    git clone https://github.com/MPIfR-BDG/katcp-monitor.git && \
    cd katcp-monitor/ && \
    pip install -r requirements.txt && \
    rm /var/cache/apk/*
WORKDIR katcp-monitor/
ENTRYPOINT ["python","exporter.py"]

