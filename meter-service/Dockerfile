FROM python:3.11-slim

WORKDIR /app

# Copy requirements
COPY meter-service/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY meter-service/ ./
COPY shared/ ./shared/

# Expose port
EXPOSE 5000

# Run the application
CMD ["python", "meter_service.py"]
