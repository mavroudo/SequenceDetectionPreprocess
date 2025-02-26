# Use an official Python image as the base image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY python_scripts/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the Python script and input files
COPY python_scripts/stream_withTimestamp.py /app/stream_withTimestamp.py

COPY experiments/input/test1.withTimestamp /app/experiments/test1.withTimestamp

# Specify the entry point and pass script arguments
ENTRYPOINT ["python", "/app/stream_withTimestamp.py"]

# Default arguments to the script (can be overridden)
CMD ["/app/experiments/test1.withTimestamp"]
