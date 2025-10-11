import argparse
import bz2


def create_test_dataset(input_file, output_file, skip_interval=5000):
    # First collect all changesets
    with bz2.open(input_file, "rt", encoding="utf-8") as infile:
        selected_changesets = []
        current_changeset = []
        changeset_count = 0
        collected_count = 0
        in_changeset = False

        for line_num, line in enumerate(infile):
            if line_num % 5_000_000 == 0:
                print(
                    f"Processed {line_num:,} lines, found {changeset_count:,} changesets, collected {collected_count} changesets"
                )

            # Skip XML header lines
            if line.strip().startswith("<?xml") or line.strip().startswith("<osm"):
                continue

            # End of file
            if line.strip() == "</osm>":
                break

            # Detect start of changeset
            if line.strip().startswith("<changeset"):
                in_changeset = True
                current_changeset = [line]
                continue

            # Detect end of changeset
            if line.strip() == "</changeset>":
                in_changeset = False
                current_changeset.append(line)

                # Check if this is a changeset we want to keep (every skip_interval-th one)
                if changeset_count % skip_interval == 0:
                    selected_changesets.extend(current_changeset)
                    collected_count += 1

                changeset_count += 1
                current_changeset = []
                continue

            # Collect lines within a changeset
            if in_changeset:
                current_changeset.append(line)

    print(f"Processed {changeset_count:,} changesets and collected {collected_count} changesets")

    # Then write all collected changesets
    with bz2.open(output_file, "wt", encoding="utf-8") as outfile:
        outfile.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        outfile.write('<osm version="0.6" generator="create_test_dataset.py">\n')
        for line in selected_changesets:
            outfile.write(line)
        outfile.write("</osm>\n")


def main():
    parser = argparse.ArgumentParser(description="Create test dataset by extracting every Nth changeset from OSM file")
    parser.add_argument("input_file", help="Path to the input OSM changeset file (.osm.bz2)")
    parser.add_argument("output_file", help="Path to the output test file (.osm.bz2)")
    parser.add_argument("--skip-interval", type=int, default=5_000, help="Extract every Nth changeset (default: 5000)")

    args = parser.parse_args()
    print(f"Creating test dataset: every {args.skip_interval}th changeset")
    create_test_dataset(args.input_file, args.output_file, args.skip_interval)
    print("Completed!")


if __name__ == "__main__":
    main()
