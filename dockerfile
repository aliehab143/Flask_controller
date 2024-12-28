# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy only requirements.txt first to take advantage of Docker layer caching
COPY requirements.txt .

# Install dependencies first (only re-runs if requirements.txt changes)
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose Kafka default port
EXPOSE 8200

# Command to run the application
CMD ["python", "app.py"]
