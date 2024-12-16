FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pg_to_ch.py .

CMD ["python", "pg_to_ch.py"]