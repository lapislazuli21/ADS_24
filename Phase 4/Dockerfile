FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY app/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./
COPY texts/ ./texts/

CMD ["python", "/app/server.py"]
