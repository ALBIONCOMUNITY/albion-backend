FROM python:3.11-slim

WORKDIR /app

COPY albion_backend.py .

EXPOSE 8000

CMD ["python", "albion_backend.py"]
