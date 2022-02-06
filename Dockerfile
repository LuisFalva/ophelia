#   Pull Python 3.7 slim buster image as base image for Ophelia
FROM python:3.7-slim-buster AS builder
#   Set name of working dir. By convention 'app'
WORKDIR /app
#   Copy requirements.txt, requirements_dev.txt and requirements_test.txt dep files
COPY requirements.txt requirements_dev.txt requirements_test.txt /app/
#   Copy Ophelia source
COPY ophelia /app/ophelia
#   Copy Ophelia test source
COPY tests /app/tests
#   Docker run pip install dependencies
RUN pip install --no-cache-dir -r /app/requirements_dev.txt
