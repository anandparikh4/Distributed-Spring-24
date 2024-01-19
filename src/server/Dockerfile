# Stage 1: Build stage
FROM python:3.10-alpine as builder

# Copy only the requirements file to optimize caching
COPY requirements.txt /

# Install dependencies
RUN pip install --upgrade pip && \
	pip wheel --no-cache-dir --wheel-dir /wheels --requirement /requirements.txt

# Stage 2: Final stage
FROM python:3.10-alpine

# Set the working directory
WORKDIR /app

# Copy only the necessary files from the builder stage
COPY --from=builder /wheels /wheels
COPY --from=builder /requirements.txt .

# Install the dependencies from the wheels
RUN pip install --no-cache-dir --no-index --find-links /wheels --requirement requirements.txt

# Remove the wheels
RUN rm -rf /wheels /requirements.txt

# Expose the port
EXPOSE 5000

# Copy the rest of the files
COPY . .

# Run the application
CMD ["python", "server.py"]
