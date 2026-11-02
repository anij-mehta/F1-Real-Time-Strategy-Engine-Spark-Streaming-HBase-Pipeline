FROM apache/spark:3.5.1
USER root
COPY docker/spark/requirements.txt /tmp/requirements.txt
RUN apt-get update && apt-get install -y --no-install-recommends python3-pip \
    && python3 -m pip install --no-cache-dir -r /tmp/requirements.txt \
    && rm -rf /var/lib/apt/lists/* /tmp/requirements.txt
COPY docker/spark/submit-gold.sh /opt/f1/bin/submit-gold.sh
COPY docker/spark/wait-for-services.py /opt/f1/bin/wait-for-services.py
RUN chmod 0555 /opt/f1/bin/submit-gold.sh /opt/f1/bin/wait-for-services.py
USER spark
