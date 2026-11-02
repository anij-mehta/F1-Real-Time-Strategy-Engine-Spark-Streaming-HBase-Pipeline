FROM python:3.11.9-slim-bookworm
WORKDIR /app
COPY docker/init/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt
COPY docker/init/init.py /app/init.py
ENTRYPOINT ["python", "/app/init.py"]
