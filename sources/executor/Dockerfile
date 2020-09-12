FROM python:3.7

RUN apt-get update && apt-get install -y openjdk-8-jdk

WORKDIR /usr/src/app

COPY ./requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY ./sources ./sources

CMD [ "spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5", "./source/consumer.py"]