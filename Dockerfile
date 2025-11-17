FROM python:3.11-slim

RUN apt-get update && apt-get install -y cron

WORKDIR /services

COPY . /services

RUN pip install --no-cache-dir -r requirements.txt || true

CMD ["cron", "-f"]
