# Dockerfile

FROM python:3.9

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generate_iot_data.py .

CMD ["python", "generate_iot_data.py"]