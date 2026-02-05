#!/usr/bin/env python3
"""
File Organization Script

Categorizes and organizes files from a source directory into
subdirectories based on file type. The source directory is passed
as a command-line argument.

Usage:
    python organize_files.py /path/to/directory
    python organize_files.py /path/to/directory --dry-run
"""

import argparse
import os
import shutil
import sys

# File type categories mapped to their extensions
CATEGORIES = {
    "Images": {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".svg", ".webp", ".ico", ".tiff", ".tif"},
    "Documents": {".pdf", ".doc", ".docx", ".txt", ".rtf", ".odt", ".xls", ".xlsx", ".ppt", ".pptx", ".csv"},
    "Audio": {".mp3", ".wav", ".flac", ".aac", ".ogg", ".wma", ".m4a"},
    "Video": {".mp4", ".avi", ".mkv", ".mov", ".wmv", ".flv", ".webm", ".m4v"},
    "Archives": {".zip", ".tar", ".gz", ".bz2", ".rar", ".7z", ".xz", ".tgz"},
    "Code": {".py", ".js", ".ts", ".java", ".c", ".cpp", ".h", ".cs", ".go", ".rs", ".rb", ".php", ".html", ".css", ".sh", ".bat"},
    "Data": {".json", ".xml", ".yaml", ".yml", ".toml", ".ini", ".cfg", ".sql", ".db", ".sqlite"},
    "Fonts": {".ttf", ".otf", ".woff", ".woff2", ".eot"},
    "Executables": {".exe", ".msi", ".dmg", ".app", ".deb", ".rpm", ".bin"},
}


def get_category(filename):
    """Return the category name for a given filename based on its extension."""
    ext = os.path.splitext(filename)[1].lower()
    for category, extensions in CATEGORIES.items():
        if ext in extensions:
            return category
    return "Other"


def organize_files(source_dir, dry_run=False):
    """Organize files in source_dir into categorized subdirectories.

    Args:
        source_dir: Path to the directory to organize.
        dry_run: If True, only print what would happen without moving files.

    Returns:
        A dict mapping category names to lists of moved filenames.
    """
    source_dir = os.path.abspath(source_dir)

    if not os.path.isdir(source_dir):
        print(f"Error: '{source_dir}' is not a valid directory.", file=sys.stderr)
        sys.exit(1)

    moved = {}

    for entry in os.listdir(source_dir):
        entry_path = os.path.join(source_dir, entry)

        # Skip directories
        if os.path.isdir(entry_path):
            continue

        category = get_category(entry)
        category_dir = os.path.join(source_dir, category)
        dest_path = os.path.join(category_dir, entry)

        # Handle name collisions by appending a number
        if os.path.exists(dest_path):
            base, ext = os.path.splitext(entry)
            counter = 1
            while os.path.exists(dest_path):
                dest_path = os.path.join(category_dir, f"{base}_{counter}{ext}")
                counter += 1

        if dry_run:
            print(f"  [DRY RUN] {entry} -> {category}/")
        else:
            os.makedirs(category_dir, exist_ok=True)
            shutil.move(entry_path, dest_path)
            print(f"  {entry} -> {category}/")

        moved.setdefault(category, []).append(entry)

    return moved


def print_summary(moved):
    """Print a summary of how many files were placed in each category."""
    if not moved:
        print("\nNo files to organize.")
        return

    print("\n--- Summary ---")
    total = 0
    for category in sorted(moved):
        count = len(moved[category])
        total += count
        print(f"  {category}: {count} file(s)")
    print(f"  Total: {total} file(s)")


def main():
    parser = argparse.ArgumentParser(
        description="Organize files in a directory into categorized subdirectories."
    )
    parser.add_argument("directory", help="Path to the directory to organize")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without moving any files",
    )
    args = parser.parse_args()

    mode = "DRY RUN" if args.dry_run else "LIVE"
    print(f"Organizing files in: {os.path.abspath(args.directory)}  [{mode}]\n")

    moved = organize_files(args.directory, dry_run=args.dry_run)
    print_summary(moved)


if __name__ == "__main__":
    main()
