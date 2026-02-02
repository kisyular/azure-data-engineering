#!/usr/bin/env fish
# Load environment variables from .env file
# Usage: source load_env.fish

set -l env_file (dirname (status --current-filename))/.env

if test -f $env_file
    for line in (cat $env_file | grep -v '^#' | grep -v '^$')
        set -gx (echo $line | cut -d '=' -f 1) (echo $line | cut -d '=' -f 2- | tr -d '"')
    end
    echo "✓ Environment variables loaded from .env"
    echo "  Resource Group: $AZURE_RESOURCE_GROUP"
    echo "  Data Factory: $AZURE_DATA_FACTORY_NAME"
else
    echo "✗ .env file not found!"
    exit 1
end
