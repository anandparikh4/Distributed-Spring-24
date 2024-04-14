FROM python:3.11-alpine3.19

# Set time zone
RUN ln -s /usr/share/zoneinfo/Asia/Kolkata /etc/localtime

ENV PYTHONUNBUFFERED 1

# install docker
RUN apk --no-cache update && \
	apk --no-cache upgrade && \
	apk --no-cache add docker-cli

# Enable venv
RUN python -m venv /opt/venv
ENV PATH "/opt/venv/bin:$PATH"
ENV PYTHONPATH "/opt/venv/lib/python3.11/site-packages:$PYTHONPATH"

# Copy only the requirements file to optimize caching
COPY requirements.txt /

# Install dependencies
RUN pip install --upgrade pip && \
	pip install --upgrade --no-cache-dir --requirement /requirements.txt

# Set the working directory
WORKDIR /app

# Copy the application
COPY --chmod=777 . .

# Deploy the application
CMD ["./deploy.sh"]
