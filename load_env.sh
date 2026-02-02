#!/bin/bash
# Load environment variables from .env file
# Usage: source load_env.sh

ENV_FILE="$(dirname "${BASH_SOURCE[0]}")/.env"

if [ -f "$ENV_FILE" ]; then
    # Export variables from .env, ignoring comments and empty lines
    export $(grep -v '^#' "$ENV_FILE" | grep -v '^$' | xargs)
    echo "✓ Environment variables loaded from .env"
    echo "  Resource Group: $AZURE_RESOURCE_GROUP"
    echo "  Data Factory: $AZURE_DATA_FACTORY_NAME"
else
    echo "✗ .env file not found!"
    return 1
fi
