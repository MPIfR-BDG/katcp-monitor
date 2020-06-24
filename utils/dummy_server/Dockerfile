FROM python:2.7

RUN pip install coloredlogs katcp==0.6.2
COPY dummy_server.py .
CMD exec python dummy_server.py -H 0.0.0.0 -p 5000 --log-level=DEBUG

