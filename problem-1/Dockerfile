FROM apache/airflow:2.8.1-python3.11

USER root
# 1. ติดตั้ง Java (จำเป็นสำหรับ Spark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. ตั้งค่า JAVA_HOME ให้ตรงกับ Mac M1/M2 (ARM64)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# 3. ติดตั้ง Library เป็น `airflow` user (ตาม docs) เพื่อให้ pip ทำงานถูกต้อง
USER airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-postgres \
    psycopg2-binary \
    pyspark==3.5.0