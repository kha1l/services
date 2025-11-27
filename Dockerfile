FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/services

RUN apt-get update \
    && apt-get install -y --no-install-recommends cron \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /services

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /services

CMD ["cron", "-f"]
