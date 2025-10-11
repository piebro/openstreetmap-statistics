import argparse
import shutil
import sys
import time
from pathlib import Path

import osmium
import pyarrow as pa
import pyarrow.parquet as pq


class ChangesetHandler(osmium.SimpleHandler):
    def __init__(self, batch_size, output_path, schema):
        osmium.SimpleHandler.__init__(self)
        self.batch_size = batch_size
        self.output_path = output_path
        self.schema = schema
        self.count = 0
        self.batch_count = 0

        # Initialize columnar data storage
        self._init_data_lists()

    def _init_data_lists(self):
        """Initialize or clear all data lists"""
        self.year = []
        self.month = []
        self.edit_count = []
        self.user_name = []
        self.tags = []
        self.bottom_left_lon = []
        self.bottom_left_lat = []
        self.top_right_lon = []
        self.top_right_lat = []

    def _save_batch(self):
        """Save current batch to disk and clear memory"""

        # Create table from current batch
        data_dict = {
            "year": self.year,
            "month": self.month,
            "edit_count": self.edit_count,
            "user_name": self.user_name,
            "tags": self.tags,
            "bottom_left_lon": self.bottom_left_lon,
            "bottom_left_lat": self.bottom_left_lat,
            "top_right_lon": self.top_right_lon,
            "top_right_lat": self.top_right_lat,
        }
        table = pa.table(data_dict, schema=self.schema)

        # Save as partitioned dataset (append mode)
        pq.write_to_dataset(
            table,
            root_path=self.output_path,
            partition_cols=["year", "month"],
            basename_template=f"part-{self.batch_count}-{{i}}.parquet",
            existing_data_behavior="overwrite_or_ignore",
        )

        # Clear data for next batch
        self._init_data_lists()

        self.batch_count += 1
        print(f"Saved batch {self.batch_count}, processed {self.count} changesets total")
        sys.stdout.flush()

    def changeset(self, c):
        self.count += 1

        # Store bounding box coordinates
        if c.bounds.valid():
            self.bottom_left_lon.append(c.bounds.bottom_left.lon)
            self.bottom_left_lat.append(c.bounds.bottom_left.lat)
            self.top_right_lon.append(c.bounds.top_right.lon)
            self.top_right_lat.append(c.bounds.top_right.lat)
        else:
            self.bottom_left_lon.append(None)
            self.bottom_left_lat.append(None)
            self.top_right_lon.append(None)
            self.top_right_lat.append(None)

        # Store basic changeset data
        self.year.append(c.created_at.year)
        self.month.append(c.created_at.month)
        self.edit_count.append(c.num_changes)

        # Clean and store user name
        user_name = c.user.replace("%20%", " ").replace("%2c%", ",")
        self.user_name.append(user_name)

        # Store all tags as-is for later enrichment
        self.tags.append(dict(c.tags))

        # Check if we need to save a batch
        if len(self.year) >= self.batch_size:
            self._save_batch()

    def finalize(self):
        """Save any remaining data in the final batch"""
        if self.year:  # If there's remaining data
            self._save_batch()
            print(f"Finished processing. Total: {self.count} changesets in {self.batch_count} batches")


def main():
    parser = argparse.ArgumentParser(description="Process OSM changesets and convert to partitioned Parquet dataset")
    parser.add_argument("changeset_path", help="Path to the OSM changeset file")
    parser.add_argument("output_path", help="Path to the output directory")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1_000_000,
        help="Number of changesets to process in each batch (default: 1_000_000)",
    )
    parser.add_argument("--overwrite", action="store_true", help="Delete output directory if it exists")

    args = parser.parse_args()

    # Handle existing output directory
    output_path = Path(args.output_path)
    if output_path.exists():
        if args.overwrite:
            print(f"Removing existing directory: {output_path}")
            shutil.rmtree(output_path)
        else:
            raise FileExistsError(
                f"Output directory '{output_path}' already exists. Use --overwrite to delete it or choose a different path."
            )

    schema_fields = [
        pa.field("year", pa.int16()),
        pa.field("month", pa.int8()),
        pa.field("edit_count", pa.int32()),
        pa.field("user_name", pa.string()),
        pa.field("bottom_left_lon", pa.float64()),
        pa.field("bottom_left_lat", pa.float64()),
        pa.field("top_right_lon", pa.float64()),
        pa.field("top_right_lat", pa.float64()),
        pa.field("tags", pa.map_(pa.string(), pa.string())),
    ]

    print(f"Processing {args.changeset_path} in batches of {args.batch_size} changesets...")
    start_time = time.time()

    handler = ChangesetHandler(
        batch_size=args.batch_size, output_path=args.output_path, schema=pa.schema(schema_fields)
    )
    handler.apply_file(args.changeset_path)
    handler.finalize()

    elapsed_time = time.time() - start_time
    print(f"Processing completed in {int(elapsed_time // 60)}:{int(elapsed_time % 60):02d} minutes")


if __name__ == "__main__":
    main()
