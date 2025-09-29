#!/usr/bin/env python3
"""Batch downloader for the C2DB database.

This utility crawls the public C2DB website to discover all available
material identifiers and downloads both the JSON dataset and the CIF
structure file for each entry.  The script honours a configurable delay
between requests so that it stays polite towards the public server.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path
from typing import Iterable, List, Optional

import requests

BASE_URL = "https://c2db.fysik.dtu.dk"
TABLE_ENDPOINT = f"{BASE_URL}/table"
MATERIAL_PATH_RE = re.compile(r"href=/material/([^\s\"']+)")
PAGE_RE = re.compile(r"/table\?sid=\d+&page=(\d+)")
DOWNLOAD_TYPES = {"json": "json", "cif": "cif"}
USER_AGENT = "c2db-downloader/1.0 (+https://github.com/)"


def fetch(session: requests.Session, url: str, *, stream: bool = False) -> requests.Response:
    """Perform an HTTP GET request and raise an error on failure."""
    response = session.get(url, timeout=60, stream=stream)
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:  # pragma: no cover - defensive logging
        logging.error("Request failed for %s: %s", url, exc)
        raise
    return response


def extract_last_page(html: str, *, fallback: int = 0) -> int:
    """Return the highest page number (inclusive) listed in the pagination."""
    pages = [int(value) for value in PAGE_RE.findall(html)]
    if not pages:
        logging.warning(
            "Could not locate any page markers; defaulting to %s", fallback
        )
        return fallback
    return max(pages)


def extract_material_slugs(html: str) -> List[str]:
    """Parse material slugs from the HTML table."""
    seen = set()
    ordered_slugs: List[str] = []
    for match in MATERIAL_PATH_RE.finditer(html):
        slug = match.group(1)
        if slug not in seen:
            ordered_slugs.append(slug)
            seen.add(slug)
    return ordered_slugs


def collect_slugs(session: requests.Session, sid: int, *, delay: float) -> List[str]:
    """Collect all unique material slugs for the given search identifier."""
    first_page_url = f"{TABLE_ENDPOINT}?sid={sid}&page=0"
    html = fetch(session, first_page_url).text
    last_page = extract_last_page(html)
    logging.info("Detected %s pages (0-indexed).", last_page + 1)

    slugs: List[str] = []
    seen = set()
    for slug in extract_material_slugs(html):
        if slug not in seen:
            slugs.append(slug)
            seen.add(slug)

    for page in range(1, last_page + 1):
        page_url = f"{TABLE_ENDPOINT}?sid={sid}&page={page}"
        logging.debug("Fetching page %s", page)
        html = fetch(session, page_url).text
        for slug in extract_material_slugs(html):
            if slug not in seen:
                slugs.append(slug)
                seen.add(slug)
        time.sleep(delay)

    logging.info("Collected %s unique slugs.", len(slugs))
    return slugs


def download_file(
    session: requests.Session,
    url: str,
    destination: Path,
    *,
    delay: float,
) -> None:
    """Download a single file when it does not exist yet."""
    if destination.exists():
        logging.debug("Skipping existing file %s", destination)
        return

    logging.debug("Downloading %s", url)
    response = fetch(session, url, stream=True)
    with destination.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                handle.write(chunk)
    time.sleep(delay)


def download_material(
    session: requests.Session,
    slug: str,
    out_dir: Path,
    *,
    delay: float,
) -> dict:
    """Download JSON and CIF files for a single material."""
    material_dir = out_dir / slug
    material_dir.mkdir(parents=True, exist_ok=True)

    manifest_entry = {"slug": slug}
    for kind, suffix in DOWNLOAD_TYPES.items():
        url = f"{BASE_URL}/material/{slug}/download/{kind}"
        destination = material_dir / f"{slug}.{suffix}"
        download_file(session, url, destination, delay=delay)
        manifest_entry[kind] = str(destination.relative_to(out_dir))
    return manifest_entry


def write_manifest(manifest: Iterable[dict], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8") as handle:
        json.dump(list(manifest), handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download data from C2DB.")
    parser.add_argument(
        "--sid",
        type=int,
        default=1542,
        help="Search identifier to crawl (default: 1542, the main dataset).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("downloads/c2db"),
        help="Directory where the dataset will be stored.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Politeness delay (seconds) between HTTP requests.",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help=(
            "Optional path for a manifest JSON file describing where each "
            "material's files were stored (defaults to <output>/manifest.json)."
        ),
    )
    parser.add_argument(
        "--max-materials",
        type=int,
        default=None,
        help="Limit the number of materials to download (useful for testing).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, …).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))

    out_dir: Path = args.output
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = args.manifest or (out_dir / "manifest.json")

    with requests.Session() as session:
        session.headers["User-Agent"] = USER_AGENT
        slugs = collect_slugs(session, args.sid, delay=args.delay)

        if args.max_materials is not None:
            slugs = slugs[: args.max_materials]
            logging.info("Limiting download to the first %s materials.", len(slugs))

        manifest = []
        for index, slug in enumerate(slugs, start=1):
            logging.info("[%s/%s] Downloading %s", index, len(slugs), slug)
            manifest.append(
                download_material(session, slug, out_dir, delay=args.delay)
            )

    write_manifest(manifest, manifest_path)
    logging.info("Wrote manifest to %s", manifest_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
