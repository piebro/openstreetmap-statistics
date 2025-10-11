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


class NotesParser:
    def __init__(
        self,
        notes_batch_size,
        comments_batch_size,
        notes_output_path,
        comments_output_path,
        notes_schema,
        comments_schema,
        ignore_current_month=False,
    ):
        self.notes_batch_size = notes_batch_size
        self.comments_batch_size = comments_batch_size
        self.notes_output_path = notes_output_path
        self.comments_output_path = comments_output_path
        self.notes_schema = notes_schema
        self.comments_schema = comments_schema
        self.ignore_current_month = ignore_current_month
        self.notes_count = 0
        self.notes_batch_count = 0
        self.comments_count = 0
        self.comments_batch_count = 0

        # Get current year and month if we need to filter
        if self.ignore_current_month:
            now = datetime.now()
            self.current_year = now.year
            self.current_month = now.month

        self._init_notes_data()
        self._init_comments_data()

    def _init_notes_data(self):
        self.note_id = []
        self.lat = []
        self.lon = []
        self.created_at = []
        self.closed_at = []
        self.mid_pos_x = []
        self.mid_pos_y = []

    def _init_comments_data(self):
        self.comment_note_id = []
        self.comment_action = []
        self.comment_timestamp = []
        self.comment_user_name = []
        self.comment_text = []

    def _save_notes_batch(self):
        """Save current notes batch to disk and clear notes data"""
        if not self.note_id:
            return

        notes_data_dict = {
            "note_id": self.note_id,
            "lat": self.lat,
            "lon": self.lon,
            "created_at": self.created_at,
            "closed_at": self.closed_at,
            "mid_pos_x": self.mid_pos_x,
            "mid_pos_y": self.mid_pos_y,
        }
        notes_table = pa.table(notes_data_dict, schema=self.notes_schema)

        notes_dir = Path(self.notes_output_path)
        notes_dir.mkdir(parents=True, exist_ok=True)
        notes_file = notes_dir / f"part-{self.notes_batch_count}.parquet"
        pq.write_table(notes_table, notes_file)

        self._init_notes_data()
        self.notes_batch_count += 1
        print(f"Saved notes batch {self.notes_batch_count}, processed {self.notes_count} notes total")
        sys.stdout.flush()

    def _save_comments_batch(self):
        """Save current comments batch to disk and clear comments data"""
        if not self.comment_note_id:
            return

        comments_data_dict = {
            "note_id": self.comment_note_id,
            "action": self.comment_action,
            "timestamp": self.comment_timestamp,
            "user_name": self.comment_user_name,
            "text": self.comment_text,
        }
        comments_table = pa.table(comments_data_dict, schema=self.comments_schema)

        comments_dir = Path(self.comments_output_path)
        comments_dir.mkdir(parents=True, exist_ok=True)
        comments_file = comments_dir / f"part-{self.comments_batch_count}.parquet"
        pq.write_table(comments_table, comments_file)

        self._init_comments_data()
        self.comments_batch_count += 1
        print(f"Saved comments batch {self.comments_batch_count}, processed {self.comments_count} comments total")
        sys.stdout.flush()

    def _parse_timestamp(self, timestamp_str):
        """Parse ISO 8601 timestamp string to datetime object"""
        if timestamp_str:
            return datetime.fromisoformat(timestamp_str[:-1] + "+00:00")
        return None

    def _process_note(self, elem):
        """Process a single note element"""
        # Get note attributes
        attribs = elem.attrib
        note_id = int(attribs.get("id"))
        lat = float(attribs.get("lat"))
        lon = float(attribs.get("lon"))
        created_at = self._parse_timestamp(attribs.get("created_at"))
        closed_at = self._parse_timestamp(attribs.get("closed_at"))

        # Check if we should skip this note
        if self.ignore_current_month:
            if created_at.year == self.current_year and created_at.month == self.current_month:
                elem.clear()
                return

            # If note was closed in current month, treat it as still open
            if closed_at and closed_at.year == self.current_year and closed_at.month == self.current_month:
                closed_at = None

        self.notes_count += 1

        self.note_id.append(note_id)
        self.lat.append(lat)
        self.lon.append(lon)
        self.created_at.append(created_at)
        self.closed_at.append(closed_at)
        self.mid_pos_x.append(round((lon + 180) % 360))
        self.mid_pos_y.append(round((lat + 90) % 180))

        # Extract comments
        for child in elem:
            if child.tag == "comment":
                comment_attribs = child.attrib
                comment_timestamp = self._parse_timestamp(comment_attribs.get("timestamp"))

                # Skip comments from current month if flag is set
                if self.ignore_current_month:
                    if comment_timestamp.year == self.current_year and comment_timestamp.month == self.current_month:
                        continue

                self.comments_count += 1

                self.comment_note_id.append(note_id)
                self.comment_action.append(comment_attribs.get("action", ""))
                self.comment_timestamp.append(comment_timestamp)
                self.comment_user_name.append(comment_attribs.get("user", ""))

                comment_text = child.text or ""
                self.comment_text.append(comment_text)

                # Check if we need to save comments batch
                if len(self.comment_note_id) >= self.comments_batch_size:
                    self._save_comments_batch()

        # Check if we need to save notes batch
        if len(self.note_id) >= self.notes_batch_size:
            self._save_notes_batch()

        elem.clear()

    def parse_file(self, file_path):
        """Parse OSM notes bz2 XML file using iterparse for memory efficiency"""
        with bz2.open(file_path, "rb") as file_handle:
            context = ET.iterparse(file_handle, events=("end",))
            for event, elem in context:
                if elem.tag == "note":
                    self._process_note(elem)

    def finalize(self):
        """Save any remaining data in the final batches"""
        self._save_notes_batch()
        self._save_comments_batch()
        print(
            f"Finished processing. Total: {self.notes_count} notes in {self.notes_batch_count} batches, "
            f"{self.comments_count} comments in {self.comments_batch_count} batches"
        )


def main():
    parser = argparse.ArgumentParser(description="Process OSM notes (with comments) and convert to Parquet datasets")
    parser.add_argument("notes_path", help="Path to the OSM notes .bz2 file")
    parser.add_argument("notes_output_path", help="Path to the output directory for notes data")
    parser.add_argument("comments_output_path", help="Path to the output directory for comments data")
    parser.add_argument(
        "--notes-batch-size",
        type=int,
        default=1_000_000,
        help="Number of notes to process in each batch (default: 1_000_000)",
    )
    parser.add_argument(
        "--comments-batch-size",
        type=int,
        default=1_000_000,
        help="Number of comments to process in each batch (default: 1_000_000)",
    )
    parser.add_argument("--overwrite", action="store_true", help="Delete output directories if they exist")
    parser.add_argument(
        "--ignore-current-month",
        action="store_true",
        help="Skip processing notes created in the current month (useful for avoiding incomplete data)",
    )

    args = parser.parse_args()

    # Handle existing output directories
    notes_output_path = Path(args.notes_output_path)
    comments_output_path = Path(args.comments_output_path)

    if notes_output_path.exists():
        if args.overwrite:
            print(f"Removing existing notes directory: {notes_output_path}")
            shutil.rmtree(notes_output_path)
        else:
            raise FileExistsError(
                f"Notes output directory '{notes_output_path}' already exists. Use --overwrite to delete it or choose a different path."
            )

    if comments_output_path.exists():
        if args.overwrite:
            print(f"Removing existing comments directory: {comments_output_path}")
            shutil.rmtree(comments_output_path)
        else:
            raise FileExistsError(
                f"Comments output directory '{comments_output_path}' already exists. Use --overwrite to delete it or choose a different path."
            )

    # Define schemas
    notes_schema_fields = [
        pa.field("note_id", pa.int64()),
        pa.field("lat", pa.float64()),
        pa.field("lon", pa.float64()),
        pa.field("created_at", pa.timestamp("us", tz="UTC")),
        pa.field("closed_at", pa.timestamp("us", tz="UTC")),
        pa.field("mid_pos_x", pa.int32()),
        pa.field("mid_pos_y", pa.int32()),
    ]

    comments_schema_fields = [
        pa.field("note_id", pa.int64()),
        pa.field("action", pa.string()),
        pa.field("timestamp", pa.timestamp("us", tz="UTC")),
        pa.field("user_name", pa.string()),
        pa.field("text", pa.string()),
    ]

    if args.ignore_current_month:
        print("Ignoring notes from the current month")

    print(
        f"Processing {args.notes_path} with notes batch size {args.notes_batch_size} "
        f"and comments batch size {args.comments_batch_size}..."
    )
    start_time = time.time()

    notes_parser = NotesParser(
        notes_batch_size=args.notes_batch_size,
        comments_batch_size=args.comments_batch_size,
        notes_output_path=args.notes_output_path,
        comments_output_path=args.comments_output_path,
        notes_schema=pa.schema(notes_schema_fields),
        comments_schema=pa.schema(comments_schema_fields),
        ignore_current_month=args.ignore_current_month,
    )
    notes_parser.parse_file(args.notes_path)
    notes_parser.finalize()

    elapsed_time = time.time() - start_time
    print(f"Processing completed in {int(elapsed_time // 60)}:{int(elapsed_time % 60):02d} minutes")


if __name__ == "__main__":
    main()
