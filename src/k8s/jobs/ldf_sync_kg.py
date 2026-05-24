"""Job entry — pulls a single KG's HDT files from lakefs into the LDF PVC.

Runs inside a kubernetes Job pod. Mounts the LDF PVC at /data and writes
HDT files atomically into /data/deploy/<shortname>/.
"""

import argparse
import asyncio
import logging
import os
import sys

from lakefs_util.io_util import download_hdt_files_to_dir


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--repo", required=True)
    p.add_argument("--ref", required=True, help="Branch, tag, or commit ID")
    p.add_argument("--shortname", required=True)
    p.add_argument("--hdt-path", default="hdt")
    p.add_argument("--dest-root", default="/data/deploy")
    return p.parse_args()


async def main_async(args):
    dest_dir = os.path.join(args.dest_root, args.shortname)
    logging.info(f"Syncing {args.repo}@{args.ref}/{args.hdt_path} -> {dest_dir}")
    written = await download_hdt_files_to_dir(
        repo=args.repo, ref=args.ref, dest_dir=dest_dir, hdt_path=args.hdt_path
    )
    if not written:
        raise SystemExit(f"No HDT files found at {args.repo}@{args.ref}/{args.hdt_path}")
    logging.info(f"Wrote {len(written)} files: {written}")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
