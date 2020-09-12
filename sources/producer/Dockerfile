FROM python:latest

WORKDIR /usr/src/app

COPY ./requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY ./source ./source

CMD [ "python", "./source/producer.py"]