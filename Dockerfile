# Use a slim Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install CA certificates
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Set environment to make sure Python/LDAP trust system certs
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

# Install OS tools for LDAP debugging and editing
RUN apt-get update && apt-get install -y --no-install-recommends \
    nano \
    netcat-openbsd \
    ldap-utils \
    slapd \
 && rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app source code
COPY app/ ./app/

# Expose port
EXPOSE 8000

# Run with uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
