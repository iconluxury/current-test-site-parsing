# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies in a single RUN to reduce layers
RUN apt-get clean && \
    apt-get update --fix-missing && \
    apt-get install -y --no-install-recommends \
        wget \
        unzip \
        gnupg2 \
        libglib2.0-0 \
        libnss3 \
        libfontconfig1 \
        libxrender1 \
        libxi6 \
        libx11-6 \
        libxss1 \
        libappindicator3-1 \
        libasound2 \
        libatk1.0-0 \
        libatk-bridge2.0-0 \
        libpango-1.0-0 \
        libpangocairo-1.0-0 \
        libgtk-3-0 \
        xvfb && \
    # Install Google Chrome
    wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt-get install -y ./google-chrome-stable_current_amd64.deb && \
    rm ./google-chrome-stable_current_amd64.deb && \
    # Clean up to reduce image size
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install ChromeDriver (pinned to a known compatible version)
RUN wget -qO /tmp/chromedriver_linux64.zip "https://storage.googleapis.com/chrome-for-testing-public/127.0.6533.88/linux64/chromedriver-linux64.zip" && \
    unzip -oj /tmp/chromedriver_linux64.zip chromedriver-linux64/chromedriver -d /usr/bin && \
    chmod 755 /usr/bin/chromedriver && \
    rm /tmp/chromedriver_linux64.zip && \
    chromedriver --version

# Copy the requirements file first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY main.py .

# Expose the port for the FastAPI application
EXPOSE 8080

ENV AWS_ACCESS_KEY_ID="AKIA2CUNLEV6V627SWI7"
ENV AWS_SECRET_ACCESS_KEY="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"
ENV AWS_REGION="us-east-2"
ENV SETTINGS_URL="https://raw.githubusercontent.com/iconluxurygroup/current-test-settings-API_Parser/refs/heads/main/settings.json"

# Run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--log-level", "info"]