#!/bin/bash -e

container_name=luisfalva/ophelia
TAG=${1:-"0.1.0"}

docker_image_name=${container_name}:${TAG}

echo "[Docker build]: building ophelia image"
docker build -t "$docker_image_name" .
echo "[Docker push]: pushing image to container"
docker push "$docker_image_name"
