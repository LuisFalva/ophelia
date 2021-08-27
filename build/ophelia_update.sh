#!/usr/bin/env bash

echo '================================================================='
echo "[Ophelia] Updating and fetching all new files from latest commits"
echo '================================================================='

# Lets fetch all new generated metadata from latest push
git fetch --all

# Pull all the latest changes from Ophelia git repo
git pull origin main

# Now lets update ophelia package
./build/ophelia_install.sh
