#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Load environment variables from .env file if present
if [ -f .env ]; then
    echo "Loading environment variables from .env file"
    set -a
    source .env
    set +a
fi

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source .venv/bin/activate

# Install or update dependencies
echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Run migrations if needed
# Uncomment if you have Alembic set up
# echo "Running database migrations..."
# alembic upgrade head

# Start the server with production settings
echo "Starting server in production mode..."

# Calculate optimal number of workers
# Gunicorn recommends (2 * NUM_CORES) + 1
NUM_CORES=$(nproc --all || echo 1)
NUM_WORKERS=$(( 2 * $NUM_CORES + 1 ))
echo "Starting with $NUM_WORKERS workers..."

# Define the port, default to 8000 if not set
PORT=${PORT:-8000}

# Run with Gunicorn using Uvicorn workers
exec gunicorn app.main:app \
    --bind 0.0.0.0:$PORT \
    --workers $NUM_WORKERS \
    --worker-class uvicorn.workers.UvicornWorker \
    --timeout 1200 \
    --keep-alive 5 \
    --log-level warning \
    --access-logfile - \
    --error-logfile - \
    --forwarded-allow-ips='*' \
    --proxy-protocol

# The exec command replaces the current process, ensuring signals like SIGTERM
# are properly forwarded to the Gunicorn process 