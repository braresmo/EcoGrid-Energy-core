FROM python:3.11-slim

WORKDIR /app

# Copy requirements
COPY wallet-service/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY wallet-service/ ./
COPY shared/ ./shared/

# Expose port
EXPOSE 5000

# Run the application
CMD ["python", "wallet_service.py"]
