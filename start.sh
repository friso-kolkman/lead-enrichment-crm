#!/bin/bash

# Lead Enrichment CRM Startup Script

echo "Starting Lead Enrichment CRM..."

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt --quiet

# Check if .env exists
if [ ! -f ".env" ]; then
    echo "Warning: .env file not found. Copying from .env.example..."
    if [ -f ".env.example" ]; then
        cp .env.example .env
        echo "Please edit .env file with your configuration before running the application."
        exit 1
    fi
fi

# Start the application
echo "Starting CRM web interface..."
echo "Open your browser to: http://localhost:8000"
echo ""
uvicorn app:app --host 0.0.0.0 --port 8000 --reload
