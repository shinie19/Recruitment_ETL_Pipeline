FROM apache/spark-py

USER root
ENV PYSPARK_MAJOR_PYTHON_VERSION=3

RUN apt-get update
RUN apt install -y python3 python3-pip
RUN pip3 install --upgrade pip setuptools --user
RUN rm -r /root/.cache && rm -rf /var/cache/apt/*

WORKDIR /opt/application
COPY requirements.txt .

RUN pip3 install -r requirements.txt --user

COPY mysql-connector-java-8.0.30.jar /opt/spark/jars
COPY cassandra_jar/* /opt/spark/jars
COPY etl_pipeline.py etl_pipeline.py

CMD ["python3", "etl_pipeline.py"]