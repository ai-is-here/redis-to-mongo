#!/bin/bash


echo "Loading environment variables from .env.prod..."
if [ -f ".env.prod" ]; then
    source .env.prod
else
    echo "Error: .env.prod file not found."
    exit 1
fi

if [ -z "$DBS_PATH" ]; then
    echo "Error: DBS_PATH environment variable is not set."
    exit 1
fi

echo "Creating directories in the path to $DBS_PATH..."
mkdir -p $DBS_PATH

echo "Generating MongoDB keyfile for authentication..."
openssl rand -base64 756 > mongodb-keyfile

echo "Setting permissions on the keyfile..."
sudo chmod 400 mongodb-keyfile

echo "Moving the keyfile to the $DBS_PATH folder..."
sudo mv mongodb-keyfile $DBS_PATH/
sudo chown 999:999 $DBS_PATH/mongodb-keyfile

echo "Please manually log in to the MongoDB instance to proceed with the configuration."

echo "127.0.0.1 localhost" | sudo tee -a /etc/hosts
echo "127.0.0.1 mongodb-entity" | sudo tee -a /etc/hosts

# Reminder message for manual steps
echo "Remember to perform the MongoDB shell steps for replica set initiation and configuration."
