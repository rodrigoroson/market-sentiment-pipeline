FROM python:3.11-slim

# Install java for Spark
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependencies
COPY requirements.txt .

# Install liberies
RUN pip install --no-cache-dir -r requirements.txt

# NLP (TextBlob) requires downloading corpora
RUN python -m textblob.download_corpora

# Copy the source code into the container
COPY src/ ./src/
COPY main.py .

# Configure environment variables for PySpark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONPATH=/app

CMD ["python", "main.py"]