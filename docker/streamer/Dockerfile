FROM python:3.11.9-slim-bookworm
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY docker/streamer/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt
COPY f1_streamer.py /app/f1_streamer.py
CMD ["python", "f1_streamer.py"]
