#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status.

REPOSITORY_NAME="$1"
IMAGE_TAG="$2"
DOCKER_USERNAME="luisfalva"

# Check if repository name and image tag are provided
if [ -z "$REPOSITORY_NAME" ] || [ -z "$IMAGE_TAG" ]; then
  echo "Usage: $0 <repository_name> <image_tag>"
  exit 1
fi

# Full image name including the Docker Hub username
FULL_IMAGE_NAME="${DOCKER_USERNAME}/${REPOSITORY_NAME}:${IMAGE_TAG}"

# Build the Docker image locally using docker 'buildx' for Apple M1/M2 architectures
docker buildx build --platform linux/amd64 -t "${REPOSITORY_NAME}:${IMAGE_TAG}" -f Dockerfile .

# Tag the Docker image with the full image name
docker tag "${REPOSITORY_NAME}:${IMAGE_TAG}" "${FULL_IMAGE_NAME}"

# Push the image to Docker registry
if ! docker push "${FULL_IMAGE_NAME}"; then
  echo "Error: Failed to push the image to ${FULL_IMAGE_NAME}"
  exit 1
fi

echo "Image successfully pushed to ${FULL_IMAGE_NAME}"

# Clean up Docker system
docker system prune -f