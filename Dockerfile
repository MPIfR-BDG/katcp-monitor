FROM python:2.7
COPY igui_exporter.py .
COPY requirements.txt .
RUN pip install -r requirements.txt
ENTRYPOINT ["python", "igui_exporter.py"]
