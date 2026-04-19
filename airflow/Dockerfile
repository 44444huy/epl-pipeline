FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    default-jdk-headless \
    python3-venv \
    git \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# ── dbt in isolated venv ─────────────────────────────────────────
# dbt-athena-community có deps xung đột với Airflow 2.8.1 (protobuf, boto3, providers-amazon).
# Cài trong venv riêng + symlink vào PATH → BashOperator gọi `dbt` bình thường.
RUN python3 -m venv /opt/dbt-venv \
    && PIP_USER=false /opt/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
    && PIP_USER=false /opt/dbt-venv/bin/pip install --no-cache-dir dbt-athena-community==1.8.3 \
    && ln -s /opt/dbt-venv/bin/dbt /usr/local/bin/dbt

USER airflow
COPY requirements-airflow.txt .
RUN pip install --no-cache-dir -r requirements-airflow.txt
