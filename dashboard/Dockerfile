FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dashboard.py .
CMD ["streamlit", "run", "dashboard.py", "--server.address=0.0.0.0"]
