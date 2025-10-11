#!/bin/bash

REPO_ID="piebro/osm-changeset-data"
DATA_DIR="./changeset_data"
for year_folder in $DATA_DIR/year=*; do
    if [ -d "$year_folder" ]; then
        folder_name=$(basename "$year_folder")
        echo "Uploading $folder_name..."
        
        uv run hf upload "$REPO_ID" "$year_folder" "changeset_data/$folder_name" \
            --repo-type=dataset \
            --commit-message="Add $folder_name data"
        
        echo "Finished $folder_name"
        echo "---"
    fi
done