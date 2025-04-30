# Use an official Python runtime as a parent image
FROM python:3.10-slim
#FROM rayproject/ray
# Set the working directory in the container
WORKDIR /app

# Copy the requirements file first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt
USER root

# Clean the apt cache and update with --fix-missing
RUN apt-get clean && \
    apt-get update --fix-missing

# Install necessary libraries, wget, and unzip
RUN apt-get update && apt-get install -y wget unzip \
    libglib2.0-0 libnss3 libgconf-2-4 libfontconfig1 libxrender1 libxi6 libx11-6 \
    xvfb  # add xvfb if you need a virtual framebuffer
# Install Chrome
RUN apt-get update && apt-get install -y wget gnupg2 unzip \
    && wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get install -y ./google-chrome-stable_current_amd64.deb \
    && rm ./google-chrome-stable_current_amd64.deb

# Download the ChromeDriver
RUN wget -qO /tmp/chromedriver_linux64.zip "https://storage.googleapis.com/chrome-for-testing-public/127.0.6533.88/linux64/chromedriver-linux64.zip"

# Unzip the ChromeDriver
RUN unzip -oj /tmp/chromedriver_linux64.zip -d /usr/bin

# Make the ChromeDriver executable
RUN chmod 755 /usr/bin/chromedriver

# Verify the ChromeDriver installation
RUN chromedriver --version

COPY main.py .
ENV AWS_ACCESS_KEY_ID="AKIA2CUNLEV6V627SWI7"
ENV AWS_SECRET_ACCESS_KEY="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"
ENV AWS_REGION="us-east-2"
EXPOSE 8080
CMD ["python", "main.py"]