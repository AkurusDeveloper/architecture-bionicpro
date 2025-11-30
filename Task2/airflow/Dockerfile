FROM apache/airflow:2.8.0-python3.11

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    docker.io sudo && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd -g 999 docker || true && \
    usermod -aG docker airflow && \
    usermod -aG root airflow && \
    echo "airflow ALL=(ALL) NOPASSWD: /bin/chmod" >> /etc/sudoers

COPY entrypoint.sh /usr/local/bin/entrypoint-wrapper.sh
RUN chmod +x /usr/local/bin/entrypoint-wrapper.sh

USER airflow

RUN pip install --no-cache-dir apache-airflow-providers-docker==3.9.1

ENTRYPOINT ["/usr/local/bin/entrypoint-wrapper.sh"]
CMD ["scheduler"]

