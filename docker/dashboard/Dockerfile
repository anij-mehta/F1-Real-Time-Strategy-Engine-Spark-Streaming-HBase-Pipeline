FROM python:3.11.9-slim-bookworm
WORKDIR /app
COPY docker/dashboard/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt
COPY dashboard/app.py /app/app.py
COPY spark_scripts/hbase_sink.py /app/hbase_sink.py
EXPOSE 8501
CMD ["streamlit", "run", "/app/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
