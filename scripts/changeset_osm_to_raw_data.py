import argparse
import bz2
import shutil
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


class ChangesetParser:
    def __init__(
        self,
        changeset_batch_size,
        discussion_batch_size,
        changeset_output_path,
        discussion_output_path,
        changeset_schema,
        discussion_schema,
        ignore_current_month=False,
    ):
        self.changeset_batch_size = changeset_batch_size
        self.discussion_batch_size = discussion_batch_size
        self.changeset_output_path = changeset_output_path
        self.discussion_output_path = discussion_output_path
        self.changeset_schema = changeset_schema
        self.discussion_schema = discussion_schema
        self.changeset_count = 0
        self.changeset_batch_count = 0
        self.discussion_count = 0
        self.discussion_batch_count = 0

        self.ignore_current_month = ignore_current_month
        if self.ignore_current_month:
            now = datetime.now()
            self.current_year = now.year
            self.current_month = now.month

        self._init_changeset_data()
        self._init_discussion_data()

    def _init_changeset_data(self):
        self.changeset_id = []
        self.year = []
        self.month = []
        self.edit_count = []
        self.user_name = []
        self.tags = []
        self.bottom_left_lon = []
        self.bottom_left_lat = []
        self.top_right_lon = []
        self.top_right_lat = []

    def _init_discussion_data(self):
        self.discussion_changeset_id = []
        self.discussion_date = []
        self.discussion_user_name = []
        self.discussion_text = []

    def _save_changeset_batch(self):
        """Save current changeset batch to disk and clear changeset data"""
        if not self.changeset_id:
            return

        changeset_data_dict = {
            "changeset_id": self.changeset_id,
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
        changeset_table = pa.table(changeset_data_dict, schema=self.changeset_schema)

        # Save as partitioned dataset
        pq.write_to_dataset(
            changeset_table,
            root_path=self.changeset_output_path,
            partition_cols=["year", "month"],
            basename_template=f"part-{self.changeset_batch_count}-{{i}}.parquet",
            existing_data_behavior="overwrite_or_ignore",
        )

        self._init_changeset_data()
        self.changeset_batch_count += 1
        print(f"Saved changeset batch {self.changeset_batch_count}, processed {self.changeset_count} changesets total")
        sys.stdout.flush()

    def _save_discussion_batch(self):
        """Save current discussion batch to disk and clear discussion data"""
        if not self.discussion_changeset_id:
            return

        discussion_data_dict = {
            "changeset_id": self.discussion_changeset_id,
            "date": self.discussion_date,
            "user_name": self.discussion_user_name,
            "text": self.discussion_text,
        }
        discussion_table = pa.table(discussion_data_dict, schema=self.discussion_schema)

        discussion_dir = Path(self.discussion_output_path)
        discussion_dir.mkdir(parents=True, exist_ok=True)
        discussion_file = discussion_dir / f"part-{self.discussion_batch_count}.parquet"
        pq.write_table(discussion_table, discussion_file)

        self._init_discussion_data()
        self.discussion_batch_count += 1
        print(f"Saved discussion batch {self.discussion_batch_count}, processed {self.discussion_count} comments total")
        sys.stdout.flush()

    def _parse_timestamp(self, timestamp_str):
        """Parse ISO 8601 timestamp string to datetime object"""
        return datetime.fromisoformat(timestamp_str[:-1] + "+00:00")

    def _process_changeset(self, elem):
        """Process a single changeset element"""
        self.changeset_count += 1

        # Get changeset attributes
        attribs = elem.attrib
        changeset_id = int(attribs.get("id"))
        created_at = self._parse_timestamp(attribs.get("created_at"))

        # Store basic changeset data
        self.changeset_id.append(changeset_id)
        self.year.append(created_at.year)
        self.month.append(created_at.month)
        self.edit_count.append(int(attribs.get("num_changes", 0)))

        # Store user name
        user_name = attribs.get("user", "")
        self.user_name.append(user_name)

        # Store bounding box coordinates
        min_lat = attribs.get("min_lat")
        min_lon = attribs.get("min_lon")
        max_lat = attribs.get("max_lat")
        max_lon = attribs.get("max_lon")

        self.bottom_left_lon.append(float(min_lon) if min_lon else None)
        self.bottom_left_lat.append(float(min_lat) if min_lat else None)
        self.top_right_lon.append(float(max_lon) if max_lon else None)
        self.top_right_lat.append(float(max_lat) if max_lat else None)

        # Extract tags
        tags_dict = {}
        for child in elem:
            if child.tag == "tag":
                key = child.attrib.get("k", "")
                value = child.attrib.get("v", "")
                tags_dict[key] = value
        self.tags.append(tags_dict)

        # Extract discussion comments
        for child in elem:
            if child.tag == "discussion":
                for comment_elem in child:
                    if comment_elem.tag == "comment":
                        comment_attribs = comment_elem.attrib
                        comment_date = self._parse_timestamp(comment_attribs.get("date"))

                        if (
                            self.ignore_current_month
                            and comment_date.year == self.current_year
                            and comment_date.month == self.current_month
                        ):
                            continue

                        self.discussion_count += 1

                        comment_text = ""
                        for text_elem in comment_elem:
                            if text_elem.tag == "text":
                                comment_text = text_elem.text or ""
                                break

                        # Store discussion data
                        self.discussion_changeset_id.append(changeset_id)
                        self.discussion_date.append(comment_date)
                        self.discussion_user_name.append(comment_attribs.get("user", ""))
                        self.discussion_text.append(comment_text)

                        # Check if we need to save discussion batch
                        if len(self.discussion_changeset_id) >= self.discussion_batch_size:
                            self._save_discussion_batch()

        # Check if we need to save changeset batch
        if len(self.changeset_id) >= self.changeset_batch_size:
            self._save_changeset_batch()

        # Clear the element to free memory
        elem.clear()

    def parse_file(self, file_path):
        """Parse OSM changeset bz2 XML file using iterparse for memory efficiency"""
        with bz2.open(file_path, "rb") as file_handle:
            context = ET.iterparse(file_handle, events=("end",))
            for event, elem in context:
                if elem.tag == "changeset":
                    self._process_changeset(elem)

    def finalize(self):
        """Save any remaining data in the final batches"""
        self._save_changeset_batch()
        self._save_discussion_batch()
        print(
            f"Finished processing. Total: {self.changeset_count} changesets in {self.changeset_batch_count} batches, "
            f"{self.discussion_count} comments in {self.discussion_batch_count} batches"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Process OSM changesets (with discussions) and convert to partitioned Parquet datasets"
    )
    parser.add_argument("changeset_path", help="Path to the OSM changeset .bz2 file")
    parser.add_argument("changeset_output_path", help="Path to the output directory for changeset data")
    parser.add_argument("discussion_output_path", help="Path to the output directory for discussion data")
    parser.add_argument(
        "--changeset-batch-size",
        type=int,
        default=1_000_000,
        help="Number of changesets to process in each batch (default: 1_000_000)",
    )
    parser.add_argument(
        "--discussion-batch-size",
        type=int,
        default=1_000_000,
        help="Number of discussion comments to process in each batch (default: 1_000_000)",
    )
    parser.add_argument("--overwrite", action="store_true", help="Delete output directories if they exist")
    parser.add_argument(
        "--comments-ignore-current-month",
        action="store_true",
        help="Skip processing discussion comments created in the current month (useful for avoiding incomplete data)",
    )

    args = parser.parse_args()

    # Handle existing output directories
    changeset_output_path = Path(args.changeset_output_path)
    discussion_output_path = Path(args.discussion_output_path)

    if changeset_output_path.exists():
        if args.overwrite:
            print(f"Removing existing changeset directory: {changeset_output_path}")
            shutil.rmtree(changeset_output_path)
        else:
            raise FileExistsError(
                f"Changeset output directory '{changeset_output_path}' already exists. Use --overwrite to delete it or choose a different path."
            )

    if discussion_output_path.exists():
        if args.overwrite:
            print(f"Removing existing discussion directory: {discussion_output_path}")
            shutil.rmtree(discussion_output_path)
        else:
            raise FileExistsError(
                f"Discussion output directory '{discussion_output_path}' already exists. Use --overwrite to delete it or choose a different path."
            )

    # Define schemas
    changeset_schema_fields = [
        pa.field("changeset_id", pa.int64()),
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

    discussion_schema_fields = [
        pa.field("changeset_id", pa.int64()),
        pa.field("date", pa.timestamp("us", tz="UTC")),
        pa.field("user_name", pa.string()),
        pa.field("text", pa.string()),
    ]

    print(
        f"Processing {args.changeset_path} with changeset batch size {args.changeset_batch_size} "
        f"and discussion batch size {args.discussion_batch_size}..."
    )
    start_time = time.time()

    changeset_parser = ChangesetParser(
        changeset_batch_size=args.changeset_batch_size,
        discussion_batch_size=args.discussion_batch_size,
        changeset_output_path=args.changeset_output_path,
        discussion_output_path=args.discussion_output_path,
        changeset_schema=pa.schema(changeset_schema_fields),
        discussion_schema=pa.schema(discussion_schema_fields),
        ignore_current_month=args.comments_ignore_current_month,
    )
    changeset_parser.parse_file(args.changeset_path)
    changeset_parser.finalize()

    elapsed_time = time.time() - start_time
    print(f"Processing completed in {int(elapsed_time // 60)}:{int(elapsed_time % 60):02d} minutes")


if __name__ == "__main__":
    main()
