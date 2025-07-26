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
RUN pip install --upgrade pip && \
	pip install --no-cache-dir --no-index --find-links /wheels --requirement requirements.txt

# Remove the wheels and apk cache
RUN rm -rf /wheels /requirements.txt

# Copy the rest of the files
COPY . .

# Run the application
CMD ["python", "load_balancer.py"]
