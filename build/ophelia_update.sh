#!/usr/bin/env bash

echo '================================================================='
echo "[Ophelia] Updating and fetching all new files from latest commits"
echo '================================================================='

# Lets fetch all new generated metadata from latest push
git fetch --all

# Get back the 2 input arguments, 1 remote: {origin, upstream}, 2 branch: {develop, main}
GIT_REMOTE=${1}
GIT_BRANCH=${2}

# Now lets test if the above arguments are given to the env
test -z ${GIT_REMOTE} && echo "Git remote must be specified." && exit 1
test -z ${GIT_BRANCH} && echo "Git branch must be specified." && exit 1

# Pull all the latest changes from Ophelia git repo
git pull "${GIT_REMOTE}" "${GIT_BRANCH}"

# Now lets update ophelia package
./build/ophelia_install.sh

echo '********************************************'
echo '[Ophelia] Successfully built Ophelia \o/ !!!'
echo '********************************************'
