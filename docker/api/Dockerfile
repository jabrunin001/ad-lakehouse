FROM python:3.11-slim
RUN pip install --no-cache-dir "fastapi>=0.115" "uvicorn>=0.32" "pydantic>=2.6"
WORKDIR /opt/app
EXPOSE 8000
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
