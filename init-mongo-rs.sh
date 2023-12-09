#!/bin/bash

# Step 1: Load environment variables from .env.prod
echo "Loading environment variables from .env.prod..."
if [ -f ".env.prod" ]; then
    source .env.prod
else
    echo "Error: .env.prod file not found."
    exit 1
fi

# Check if MongoDB environment variables are set
if [ -z "$MONGO_DB_USER" ] || [ -z "$MONGO_DB_PASSWORD" ] || [ -z "$MONGO_DB_NAME" ]; then
    echo "Error: MongoDB environment variables are not set."
    exit 1
fi

# Step 2: Access the Docker container and initiate the MongoDB replica set
echo "Initiating MongoDB replica set in prod-mongodb-entity-compose container..."
docker exec -it prod-mongodb-entity-compose bash -c "echo 'rs.initiate()' | mongosh -u $MONGO_DB_USER -p $MONGO_DB_PASSWORD --authenticationDatabase admin $MONGO_DB_NAME"

echo "Replica set initiation process completed."
