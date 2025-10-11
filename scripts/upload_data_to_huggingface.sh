#!/bin/bash

REPO_ID="piebro/osm-data"

# Upload changeset data (partitioned by year)
CHANGESET_DATA_DIR="./changeset_data"
echo "Uploading changeset data..."
for year_folder in $CHANGESET_DATA_DIR/year=*; do
    if [ -d "$year_folder" ]; then
        folder_name=$(basename "$year_folder")
        echo "  Uploading $folder_name..."

        uv run hf upload "$REPO_ID" "$year_folder" "changeset_data/$folder_name" \
            --repo-type=dataset \
            --commit-message="Add changeset $folder_name data"

        echo "  Finished $folder_name"
        echo "  ---"
    fi
done

# Upload changeset comments data
CHANGESET_COMMENTS_DATA_DIR="./changeset_comments_data"
if [ -d "$CHANGESET_COMMENTS_DATA_DIR" ]; then
    echo "Uploading changeset comments data..."

    uv run hf upload "$REPO_ID" "$CHANGESET_COMMENTS_DATA_DIR" "changeset_comments_data" \
        --repo-type=dataset \
        --commit-message="Add changeset comments data"

    echo "Finished changeset comments data"
    echo "---"
fi

# Upload notes data
NOTES_DATA_DIR="./notes_data"
if [ -d "$NOTES_DATA_DIR" ]; then
    echo "Uploading notes data..."

    uv run hf upload "$REPO_ID" "$NOTES_DATA_DIR" "notes_data" \
        --repo-type=dataset \
        --commit-message="Add notes data"

    echo "Finished notes data"
    echo "---"
fi

# Upload notes comments data
NOTES_COMMENTS_DATA_DIR="./notes_comments_data"
if [ -d "$NOTES_COMMENTS_DATA_DIR" ]; then
    echo "Uploading notes comments data..."

    uv run hf upload "$REPO_ID" "$NOTES_COMMENTS_DATA_DIR" "notes_comments_data" \
        --repo-type=dataset \
        --commit-message="Add notes comments data"

    echo "Finished notes comments data"
    echo "---"
fi

echo "All uploads complete!"