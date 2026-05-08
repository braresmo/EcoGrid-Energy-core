FROM python:3.11-slim

WORKDIR /app

# Copy requirements
COPY marketplace-service/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY marketplace-service/ ./
COPY shared/ ./shared/

# Expose port
EXPOSE 5000

# Run the application
CMD ["python", "marketplace_service.py"]
