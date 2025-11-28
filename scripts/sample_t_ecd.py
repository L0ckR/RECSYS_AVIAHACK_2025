#!/usr/bin/env python3
"""
Sample T-ECD Parquet data by user hash while preserving partitions.

The script lists Parquet objects under a source prefix (local path or S3),
filters users based on a deterministic hash rule, and writes a partition-
preserving Parquet subset to the destination.
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
from typing import Iterable, Optional, Set

import pyarrow as pa
import pyarrow.dataset as ds
import s3fs

LOG = logging.getLogger("sample_t_ecd")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, help="Source prefix (S3 URI or local path) containing Parquet data")
    parser.add_argument("--dest", required=True, help="Destination prefix (S3 URI or local path) for sampled Parquet output")
    parser.add_argument(
        "--user-count",
        type=int,
        default=None,
        help=(
            "Approximate number of unique users to keep. The hash filter still applies; "
            "new users beyond this limit are skipped, but rows for already-kept users are retained."
        ),
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="AWS profile name used for S3 access (passed to s3fs).",
    )
    parser.add_argument(
        "--hash-mod",
        type=int,
        default=1000,
        help="Hash modulus for sampling (default 1000 keeps ~0.1% where hash(user_id) % hash_mod == 0)",
    )
    return parser.parse_args()


def build_filesystem(path: str, profile: Optional[str]) -> Optional[s3fs.S3FileSystem]:
    if path.startswith("s3://"):
        return s3fs.S3FileSystem(profile=profile)
    return None


def deterministic_hash(value: str, modulus: int) -> int:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return int(digest, 16) % modulus


def should_keep_user(user_id: str, modulus: int) -> bool:
    return deterministic_hash(user_id, modulus) == 0


def list_candidate_files(dataset: ds.Dataset) -> Iterable[str]:
    for fragment in dataset.get_fragments():
        for file in fragment.files:
            yield file


def filter_batch(
    batch: pa.RecordBatch,
    modulus: int,
    target_users: Optional[int],
    kept_users: Set[str],
) -> Optional[pa.RecordBatch]:
    try:
        user_col = batch.column("user_id")
    except KeyError as exc:
        raise KeyError("Input data must include a 'user_id' column") from exc

    user_ids = user_col.to_pylist()
    mask = []
    for user_id in user_ids:
        keep = should_keep_user(user_id, modulus)
        if not keep:
            mask.append(False)
            continue

        if target_users is None:
            mask.append(True)
            kept_users.add(user_id)
            continue

        if user_id in kept_users:
            mask.append(True)
            continue

        if len(kept_users) < target_users:
            kept_users.add(user_id)
            mask.append(True)
        else:
            mask.append(False)

    keep_mask = pa.array(mask, type=pa.bool_())
    if keep_mask.any():
        return batch.filter(keep_mask)
    return None


def sample_dataset(source: str, dest: str, profile: Optional[str], target_users: Optional[int], modulus: int) -> None:
    source_fs = build_filesystem(source, profile)
    dest_fs = build_filesystem(dest, profile)

    partitioning = "hive"
    dataset = ds.dataset(source, format="parquet", filesystem=source_fs, partitioning=partitioning)

    LOG.info("Discovered %s candidate files", sum(1 for _ in list_candidate_files(dataset)))
    kept_users: Set[str] = set()
    filtered_batches = []

    for fragment in dataset.get_fragments():
        for batch in fragment.to_batches():
            filtered = filter_batch(batch, modulus, target_users, kept_users)
            if filtered is not None:
                filtered_batches.append(filtered)
        if target_users is not None and len(kept_users) >= target_users:
            LOG.info("Reached target unique users=%s; continuing with already-kept users only.", target_users)

    if not filtered_batches:
        LOG.warning("No rows matched the sampling criteria; nothing to write.")
        return

    reader = pa.RecordBatchReader.from_batches(dataset.schema, filtered_batches)
    ds.write_dataset(
        data=reader,
        base_dir=dest,
        filesystem=dest_fs,
        format="parquet",
        partitioning=partitioning,
        existing_data_behavior="overwrite_or_ignore",
        create_dir=True,
    )

    LOG.info(
        "Completed sampling: kept %s unique users into %s (hash modulus=%s)", len(kept_users), dest, modulus
    )


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def main() -> None:
    configure_logging()
    args = parse_args()

    if args.hash_mod < 1:
        raise ValueError("--hash-mod must be >= 1")

    os.environ.setdefault("AWS_PROFILE", args.profile or "")
    sample_dataset(
        source=args.source,
        dest=args.dest,
        profile=args.profile,
        target_users=args.user_count,
        modulus=args.hash_mod,
    )


if __name__ == "__main__":
    main()
