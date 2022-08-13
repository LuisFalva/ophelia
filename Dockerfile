#   Pull Python 3.7 slim buster image as base image for Ophelia
FROM godatadriven/pyspark:latest AS builder
#   Set name of working dir. By convention 'app'
WORKDIR /ophelia
#   Copy all txt requirements files for ophelia, dev and test.
COPY requirements.txt requirements_dev.txt requirements_test.txt .
#   Copy *.md README files for setup.py configuration.
RUN pip install --no-cache-dir -r requirements_dev.txt
#   Copy Ophelia source
COPY ophelia .
#   Copy Ophelia test source
COPY tests .

#   Package Step
FROM python:3.7-slim-buster AS package
#   Set name of working dir. By convention 'app'
WORKDIR /ophelia
#   Add curl
RUN apt update && apt install -y curl
#   Copy files from builder container
COPY --from=builder /ophelia .

