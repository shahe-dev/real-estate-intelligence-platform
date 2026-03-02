#!/bin/bash
set -e

echo ""
echo "============================================================"
echo "Setting up Dubai Real Estate Intel"
echo "============================================================"
echo ""

# Install Python dependencies
echo "1. Installing Python dependencies..."
pip install -r requirements.txt --quiet

# Create data directories
echo "2. Creating data directories..."
mkdir -p data/database

# Download database files from GitHub Release
echo "3. Downloading database files from GitHub Release..."
REPO="shahe-dev/re-intelligence"
TAG="data-v2"

# Remove any failed downloads first
rm -f data/database/*.db 2>/dev/null || true

# Use GITHUB_TOKEN (automatically available in Codespaces)
if [ -n "$GITHUB_TOKEN" ]; then
    echo "   Fetching release info..."
    RELEASE=$(curl -s -H "Authorization: token $GITHUB_TOKEN" \
        "https://api.github.com/repos/$REPO/releases/tags/$TAG")

    if echo "$RELEASE" | grep -q '"assets"'; then
        for asset_name in "property_monitor.db" "dubai_land.db"; do
            # Extract asset ID for this file
            ASSET_ID=$(echo "$RELEASE" | grep -B3 "\"name\": \"$asset_name\"" | grep '"id"' | head -1 | grep -o '[0-9]*')

            if [ -n "$ASSET_ID" ]; then
                echo "   Downloading $asset_name..."
                curl -L -H "Authorization: token $GITHUB_TOKEN" \
                    -H "Accept: application/octet-stream" \
                    "https://api.github.com/repos/$REPO/releases/assets/$ASSET_ID" \
                    -o "data/database/$asset_name" --progress-bar
            else
                echo "   Warning: $asset_name not found in release"
            fi
        done
    else
        echo "   Warning: Release '$TAG' not found or no assets"
        echo "   You can run: python scripts/run_property_monitor.py refresh (if BigQuery credentials are set)"
    fi
else
    echo "   Warning: GITHUB_TOKEN not available"
    echo "   Please run: python scripts/run_property_monitor.py refresh (if BigQuery credentials are set)"
fi

# Verify downloads
echo ""
echo "4. Verifying setup..."
if [ -f "data/database/property_monitor.db" ]; then
    echo "   [OK] property_monitor.db found"
else
    echo "   [--] property_monitor.db not found"
fi

if [ -f "data/database/dubai_land.db" ]; then
    echo "   [OK] dubai_land.db found"
else
    echo "   [--] dubai_land.db not found"
fi

# Check for required secrets
echo ""
echo "5. Checking configuration..."
if [ -n "$ANTHROPIC_API_KEY" ]; then
    echo "   [OK] ANTHROPIC_API_KEY is set"
else
    echo "   [--] ANTHROPIC_API_KEY not set (needed for content generation)"
fi

if [ -n "$GOOGLE_PROJECT_ID" ] && [ -n "$GOOGLE_PRIVATE_KEY" ]; then
    echo "   [OK] BigQuery credentials are set (can refresh from BigQuery)"
else
    echo "   [--] BigQuery credentials not set (optional - for data refresh)"
fi

echo ""
echo "============================================================"
echo "Setup complete!"
echo "============================================================"
echo ""
echo "Quick commands:"
echo "  python scripts/run_property_monitor.py status    # Check data status"
echo "  python scripts/run_property_monitor.py api       # Start API server"
echo "  python scripts/run_property_monitor.py refresh   # Refresh from BigQuery"
echo ""
