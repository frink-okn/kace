# Copyright Onai Inc.


# Use Ubuntu 24.04 as the base image
FROM ubuntu:24.04

# Set environment variables to avoid interactive prompts during package installations
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

# Update and install necessary packages
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       python3 \
       python3-pip \
       build-essential \
       libssl-dev \
       libffi-dev \
       python3-dev \
       python3-cryptography \
       tzdata \
       vim \
    && rm -rf /var/lib/apt/lists/*

# Install cryptography package using pip
#RUN pip3 install --no-cache-dir cryptography

# Set the default Python version to be used when running commands
RUN ln -s /usr/bin/python3 /usr/bin/python

# Set the working directory inside the container
WORKDIR /app

# Copy your application code into the container
COPY . /app

# Command to run when the container starts
#CMD ["python", "--version"]
