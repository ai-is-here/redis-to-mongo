#!/bin/bash

# Function to check if the Docker Compose environment is already running
is_running() {
    # Check if any container in the compose is running for the specific environment
    local containers=$(docker ps --filter "label=com.docker.compose.project=$1" -q)
    if [ -n "$containers" ]; then
        return 0
    else
        return 1
    fi
}

# Print usage instructions and additional information
print_usage() {
    echo "Usage: $0 {test|prod} {up|down}"
    echo "You can run both 'test' and 'prod' environments on the same system."
    echo "The script checks if the Docker Compose environment is already running before starting it."
    exit 1
}

# Check if arguments are supplied
if [ $# -ne 2 ]; then
    print_usage
fi

# Setting the file names and project name based on the environment
if [ "$1" == "test" ]; then
    COMPOSE_FILE="docker-compose.test.yml"
    ENV_FILE=".env.test"
    PROJECT_NAME="test"
elif [ "$1" == "prod" ]; then
    COMPOSE_FILE="docker-compose.prod.yml"
    ENV_FILE=".env.prod"
    PROJECT_NAME="prod"
else
    echo "Invalid argument. Use 'test' or 'prod'."
    exit 1
fi

# Perform the action based on the second argument
case "$2" in
    up)
        if is_running "$PROJECT_NAME"; then
            echo "Docker Compose for $1 environment is already running."
        else
            echo "Building Docker Compose for $1 environment..."
            docker compose --env-file "$ENV_FILE" -p "$PROJECT_NAME" build redis-to-mongo-sync-script
            echo "Starting Docker Compose for $1 environment..."
            docker compose --env-file "$ENV_FILE" -p "$PROJECT_NAME" up -d
        fi
        ;;
    down)
        echo "Stopping Docker Compose for $1 environment..."
        docker compose --env-file "$ENV_FILE" -p "$PROJECT_NAME" down
        ;;
    *)
        echo "Invalid command. Use 'up' or 'down'."
        exit 1
        ;;
esac
