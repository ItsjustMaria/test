#!/usr/bin/env python3
"""
organize_archive_files.py

Reorganizes files scattered across UUID-named subfolders on a drive into
flat "archivenumber" folders, driven by an Excel sheet that lists which
(uuid, archivenumber, filename) combinations are supposed to exist.

------------------------------------------------------------------------
WHAT IT DOES
------------------------------------------------------------------------
1. Reads the Excel file and figures out the uuid / archivenumber / filename
   columns (auto-detected, can be overridden with CLI flags).
2. Walks SOURCE_ROOT once, looking for any folder whose *name* contains a
   UUID. That folder is treated as the root for that uuid (it does not
   matter how it's prefixed, e.g. "287_001-<uuid>", or how deep it sits).
3. For every uuid that the Excel file actually needs, it walks *inside*
   that uuid's folder (any depth, any number of subfolders) and indexes
   every file found there.
4. For every row in the Excel file, it looks for the expected filename
   inside that uuid's file index (matching is tried on the exact name
   first, then on the name without extension, in case the sheet omits
   extensions). If found, the file is moved (or copied) into
   DEST_ROOT/<archivenumber>/.
5. Everything is logged:
      - success_log.csv   -> every file that was moved successfully
      - error_log.csv     -> every file that could NOT be moved, and why
      - run_log_<ts>.log  -> full human-readable run log (progress, warnings)
      - a short console summary at the end

------------------------------------------------------------------------
IMPORTANT SAFETY NOTES
------------------------------------------------------------------------
- ALWAYS do a --dry-run first. It performs every step (including writing
  success/error logs) EXCEPT it doesn't touch any files. Check the logs,
  then re-run without --dry-run.
- Default action is MOVE (as requested). Pass --copy if you'd rather copy
  and keep the originals in place until you've verified everything.
- The script is safe to re-run: if a destination file already exists and
  is the same size as the source, it is skipped (not re-moved, not
  duplicated) and logged as "already present".
- File size is compared before/after every move/copy as a cheap integrity
  check. A mismatch is logged as an error rather than silently accepted.

------------------------------------------------------------------------
USAGE
------------------------------------------------------------------------
    python organize_archive_files.py \\
        --excel "Q:\\path\\to\\overview.xlsx" \\
        --source "D:\\drive_root" \\
        --dest   "D:\\organized_by_archive" \\
        --dry-run

    # once you're happy with the dry run:
    python organize_archive_files.py \\
        --excel "Q:\\path\\to\\overview.xlsx" \\
        --source "D:\\drive_root" \\
        --dest   "D:\\organized_by_archive"

Optional flags:
    --sheet NAME_OR_INDEX     which sheet to read (default: first sheet)
    --uuid-col / --archive-col / --filename-col
                              force column names if auto-detection fails
    --copy                    copy instead of move
    --dry-run                 simulate only, no file operations
    --log-dir PATH            where to write logs (default: ./logs)
"""

import argparse
import csv
import logging
import os
import re
import shutil
import sys
from collections import defaultdict
from datetime import datetime

import pandas as pd

UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)

UUID_COL_CANDIDATES = ["uuid", "UUID"]
ARCHIVE_COL_CANDIDATES = [
    "TOEGANGSNUMMER", "toegangsnummer"
]
FILENAME_COL_CANDIDATES = [
    "BESTANDSNAAM", "bestandsnaam"
]


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def norm(s):
    return re.sub(r"[^a-z0-9]", "", str(s).lower())


def find_column(columns, candidates):
    normalized = {col: norm(col) for col in columns}
    cand_norm = [norm(c) for c in candidates]
    for col, n in normalized.items():
        if n in cand_norm:
            return col
    return None


def clean_archivenumber(value):
    """Excel sometimes turns '287' into 287.0 -- undo that, keep everything
    else (leading zeros, alphanumeric archive numbers, etc.) untouched."""
    s = str(value).strip()
    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]
    return s


def sanitize_folder_name(name):
    """Strip characters that are illegal in Windows folder names, just in
    case an archivenumber ever contains something unexpected."""
    return re.sub(r'[<>:"/\\|?*]', "_", str(name)).strip()


def setup_logging(log_dir):
    os.makedirs(log_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(log_dir, f"run_log_{ts}.log")

    logger = logging.getLogger("organizer")
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger, log_path


# --------------------------------------------------------------------------
# core steps
# --------------------------------------------------------------------------

def load_excel(path, sheet, uuid_col, archive_col, filename_col, logger):
    logger.info(f"Reading Excel file: {path}")
    df = pd.read_excel(path, sheet_name=sheet if sheet is not None else 0, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]

    uuid_col = uuid_col or find_column(df.columns, UUID_COL_CANDIDATES)
    archive_col = archive_col or find_column(df.columns, ARCHIVE_COL_CANDIDATES)
    filename_col = filename_col or find_column(df.columns, FILENAME_COL_CANDIDATES)

    missing = [
        name for name, col in
        [("uuid", uuid_col), ("archivenumber", archive_col), ("filename", filename_col)]
        if col is None
    ]
    if missing:
        raise SystemExit(
            f"Could not auto-detect column(s): {missing}. "
            f"Available columns are: {list(df.columns)}. "
            f"Re-run with --uuid-col / --archive-col / --filename-col to specify them manually."
        )

    logger.info(f"Using columns -> uuid: '{uuid_col}', archivenumber: '{archive_col}', filename: '{filename_col}'")

    df = df[[uuid_col, archive_col, filename_col]].copy()
    df.columns = ["uuid", "archivenumber", "filename"]

    before = len(df)
    df = df.dropna(subset=["uuid", "archivenumber", "filename"])
    df["uuid"] = df["uuid"].astype(str).str.strip().str.lower()
    df["archivenumber"] = df["archivenumber"].apply(clean_archivenumber)
    df["filename"] = df["filename"].astype(str).str.strip()
    df = df[(df["uuid"] != "") & (df["archivenumber"] != "") & (df["filename"] != "")]

    dropped = before - len(df)
    if dropped:
        logger.warning(f"Dropped {dropped} row(s) with missing uuid/archivenumber/filename.")

    dup_mask = df.duplicated(subset=["uuid", "archivenumber", "filename"], keep="first")
    if dup_mask.any():
        logger.warning(f"Found {dup_mask.sum()} exact duplicate row(s) in the sheet; keeping the first occurrence of each.")
        df = df[~dup_mask]

    logger.info(f"Loaded {len(df)} usable row(s) covering {df['uuid'].nunique()} unique uuid(s) "
                f"and {df['archivenumber'].nunique()} unique archivenumber(s).")
    return df


def find_uuid_folders(source_root, logger):
    """Single pass over the whole tree. Any directory whose name contains a
    UUID is registered and NOT descended into further (we search inside it
    separately, only for uuids we actually need)."""
    logger.info(f"Scanning {source_root} for uuid folders (this can take a while on large drives)...")
    uuid_folders = {}
    dup_count = 0
    scanned_dirs = 0

    for dirpath, dirnames, _filenames in os.walk(source_root):
        keep = []
        for d in dirnames:
            scanned_dirs += 1
            m = UUID_RE.search(d)
            if m:
                uuid = m.group(0).lower()
                full = os.path.join(dirpath, d)
                if uuid in uuid_folders:
                    dup_count += 1
                    logger.warning(
                        f"Duplicate folder found for uuid {uuid}: '{full}' "
                        f"(already using '{uuid_folders[uuid]}'). Keeping the first one found."
                    )
                else:
                    uuid_folders[uuid] = full
                # do not keep -> os.walk will not descend into this folder here;
                # we index its contents separately in index_files_for_uuid()
            else:
                keep.append(d)
        dirnames[:] = keep

    logger.info(f"Found {len(uuid_folders)} uuid folder(s) on the drive ({dup_count} duplicate name conflict(s)).")
    return uuid_folders


def index_files_for_uuid(uuid_root, logger):
    """Recursively index every file under uuid_root. Returns two dicts:
    exact filename (lowercased) -> [full paths], and stem-without-extension
    (lowercased) -> [full paths]."""
    by_name = defaultdict(list)
    by_stem = defaultdict(list)
    for dirpath, _dirnames, filenames in os.walk(uuid_root):
        for f in filenames:
            full = os.path.join(dirpath, f)
            by_name[f.lower()].append(full)
            stem = os.path.splitext(f)[0].lower()
            by_stem[stem].append(full)
    return by_name, by_stem


def resolve_file(filename, by_name, by_stem):
    """Try exact match first, then match on filename stem (in case the
    sheet lists filenames without extensions). Returns list of candidate
    full paths (empty if none found)."""
    key = filename.lower()
    if key in by_name:
        return by_name[key]
    stem_key = os.path.splitext(filename)[0].lower()
    if stem_key in by_stem:
        return by_stem[stem_key]
    return []


def safe_destination(dest_dir, filename):
    """If filename already exists in dest_dir, append _dup1, _dup2, ... so
    we never silently overwrite a different file."""
    dest_path = os.path.join(dest_dir, filename)
    if not os.path.exists(dest_path):
        return dest_path, False
    stem, ext = os.path.splitext(filename)
    i = 1
    while True:
        candidate = os.path.join(dest_dir, f"{stem}_dup{i}{ext}")
        if not os.path.exists(candidate):
            return candidate, True
        i += 1


def process(df, source_root, dest_root, mode, dry_run, logger):
    successes = []  # dicts for success_log.csv
    errors = []     # dicts for error_log.csv

    uuid_folders = find_uuid_folders(source_root, logger)

    needed_uuids = sorted(df["uuid"].unique())
    file_indexes = {}  # uuid -> (by_name, by_stem)

    for i, uuid in enumerate(needed_uuids, 1):
        if uuid not in uuid_folders:
            continue
        if i % 200 == 0 or i == len(needed_uuids):
            logger.info(f"Indexed files for {i}/{len(needed_uuids)} needed uuid folders...")
        file_indexes[uuid] = index_files_for_uuid(uuid_folders[uuid], logger)

    missing_uuid_folders = set(needed_uuids) - set(uuid_folders.keys())
    if missing_uuid_folders:
        logger.warning(f"{len(missing_uuid_folders)} uuid(s) from the sheet were not found as folders on the drive at all.")

    total = len(df)
    for n, row in enumerate(df.itertuples(index=False), 1):
        uuid, archivenumber, filename = row.uuid, row.archivenumber, row.filename

        if n % 5000 == 0 or n == total:
            logger.info(f"Processed {n}/{total} rows "
                        f"({len(successes)} moved, {len(errors)} errors so far)...")

        if uuid not in uuid_folders:
            errors.append({
                "archivenumber": archivenumber, "uuid": uuid, "filename": filename,
                "reason": "uuid folder not found on drive",
            })
            continue

        by_name, by_stem = file_indexes[uuid]
        candidates = resolve_file(filename, by_name, by_stem)

        if not candidates:
            errors.append({
                "archivenumber": archivenumber, "uuid": uuid, "filename": filename,
                "reason": "file not found inside uuid folder",
            })
            continue

        if len(candidates) > 1:
            logger.warning(
                f"Multiple files matched '{filename}' for uuid {uuid} "
                f"({len(candidates)} matches); using the first one: {candidates[0]}"
            )

        src = candidates[0]
        dest_dir = os.path.join(dest_root, sanitize_folder_name(archivenumber))

        try:
            if not dry_run:
                os.makedirs(dest_dir, exist_ok=True)

            src_basename = os.path.basename(src)
            existing_path = os.path.join(dest_dir, src_basename)

            if not dry_run and os.path.exists(existing_path):
                if os.path.getsize(existing_path) == os.path.getsize(src):
                    successes.append({
                        "archivenumber": archivenumber, "uuid": uuid,
                        "filename_expected": filename, "file_found": src_basename,
                        "source_path": src, "dest_path": existing_path,
                        "status": "already present (skipped)",
                    })
                    continue
                else:
                    dest_path, renamed = safe_destination(dest_dir, src_basename)
                    if renamed:
                        logger.warning(f"Name collision for '{src_basename}' in {dest_dir}; "
                                        f"saved as '{os.path.basename(dest_path)}' instead.")
            else:
                dest_path = existing_path

            src_size = os.path.getsize(src) if not dry_run else None

            if dry_run:
                status = f"DRY RUN - would {'copy' if mode == 'copy' else 'move'}"
            else:
                if mode == "copy":
                    shutil.copy2(src, dest_path)
                else:
                    shutil.move(src, dest_path)
                dest_size = os.path.getsize(dest_path)
                if dest_size != src_size:
                    raise IOError(f"size mismatch after transfer: source {src_size} bytes, dest {dest_size} bytes")
                status = "moved" if mode == "move" else "copied"

            successes.append({
                "archivenumber": archivenumber, "uuid": uuid,
                "filename_expected": filename, "file_found": src_basename,
                "source_path": src, "dest_path": dest_path,
                "status": status,
            })

        except Exception as e:
            errors.append({
                "archivenumber": archivenumber, "uuid": uuid, "filename": filename,
                "reason": f"error during transfer: {e}",
            })

    return successes, errors


def write_success_log(successes, path):
    successes_sorted = sorted(successes, key=lambda r: (str(r["archivenumber"]), r["uuid"], r["filename_expected"]))
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "archivenumber", "uuid", "filename_expected", "file_found",
            "source_path", "dest_path", "status",
        ])
        writer.writeheader()
        writer.writerows(successes_sorted)


def write_error_log(errors, path):
    errors_sorted = sorted(errors, key=lambda r: (str(r["archivenumber"]), r["uuid"], r["filename"]))
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["archivenumber", "uuid", "filename", "reason"])
        writer.writeheader()
        writer.writerows(errors_sorted)


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Organize archive files by archivenumber, driven by an Excel manifest.")
    parser.add_argument("--excel", required=True, help="Path to the Excel file")
    parser.add_argument("--sheet", default=None, help="Sheet name or index (default: first sheet)")
    parser.add_argument("--source", required=True, help="Root folder to search for uuid folders")
    parser.add_argument("--dest", required=True, help="Root folder where archivenumber folders will be created")
    parser.add_argument("--uuid-col", default=None, help="Override: name of the uuid column")
    parser.add_argument("--archive-col", default=None, help="Override: name of the archivenumber column")
    parser.add_argument("--filename-col", default=None, help="Override: name of the filename column")
    parser.add_argument("--copy", action="store_true", help="Copy instead of move")
    parser.add_argument("--dry-run", action="store_true", help="Simulate only, no files are touched")
    parser.add_argument("--log-dir", default="./logs", help="Directory for log/CSV output (default: ./logs)")
    args = parser.parse_args()

    logger, log_path = setup_logging(args.log_dir)
    mode = "copy" if args.copy else "move"

    logger.info(f"Mode: {mode.upper()}{'  (DRY RUN - no files will be touched)' if args.dry_run else ''}")
    logger.info(f"Source: {args.source}")
    logger.info(f"Destination: {args.dest}")

    if not os.path.isdir(args.source):
        raise SystemExit(f"Source folder does not exist: {args.source}")
    if not args.dry_run:
        os.makedirs(args.dest, exist_ok=True)

    df = load_excel(args.excel, args.sheet, args.uuid_col, args.archive_col, args.filename_col, logger)

    successes, errors = process(df, args.source, args.dest, mode, args.dry_run, logger)

    success_csv = os.path.join(args.log_dir, "success_log.csv")
    error_csv = os.path.join(args.log_dir, "error_log.csv")
    write_success_log(successes, success_csv)
    write_error_log(errors, error_csv)

    archives_touched = len({s["archivenumber"] for s in successes})
    logger.info("=" * 70)
    logger.info("RUN SUMMARY")
    logger.info(f"  Total rows in sheet processed : {len(df)}")
    logger.info(f"  Successfully moved/copied     : {len(successes)}")
    logger.info(f"  Errors / missing files        : {len(errors)}")
    logger.info(f"  Archive folders touched       : {archives_touched}")
    logger.info(f"  Success log (CSV)             : {success_csv}")
    logger.info(f"  Error log (CSV)               : {error_csv}")
    logger.info(f"  Full run log                  : {log_path}")
    logger.info("=" * 70)
    if args.dry_run:
        logger.info("This was a DRY RUN. No files were moved or copied. Review the logs, "
                     "then re-run without --dry-run when you're confident.")


if __name__ == "__main__":
    main()