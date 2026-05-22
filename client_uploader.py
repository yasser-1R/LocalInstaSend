"""
LAN Photo Uploader — Companion Client Script
=============================================
Run this on ANY other PC/laptop on the same Wi-Fi to automatically
scan that device's disk for images and push them to the server.

Usage:
    python client_uploader.py --server http://192.168.1.5:8000

Optional flags:
    --server   SERVER_URL    e.g. http://192.168.1.5:8000  (required)
    --roots    FOLDER ...    custom folders to scan (default: Pictures, Desktop, Downloads)
    --ext      .jpg .png ... image extensions to include
    --dry-run               show what would be sent without actually sending
"""

import argparse
import mimetypes
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    print("❌  'requests' is not installed. Run:  pip install requests")
    sys.exit(1)

# ── Defaults ─────────────────────────────────────────────────────────────────
DEFAULT_ROOTS = [
    Path.home() / "Pictures",
    Path.home() / "Desktop",
    Path.home() / "Downloads",
    Path.home() / "Documents",
]
DEFAULT_EXTS  = {".jpg", ".jpeg", ".png", ".gif", ".bmp",
                 ".webp", ".tiff", ".tif", ".heic", ".heif"}
MAX_FILES     = 5000
CHUNK_SIZE    = 20     # files per batch request


def scan(roots: list[Path], exts: set[str]) -> list[Path]:
    found = []
    for root in roots:
        if not root.exists():
            print(f"  ⚠  Skipping (not found): {root}")
            continue
        print(f"  🔍  Scanning: {root}")
        try:
            for p in root.rglob("*"):
                if len(found) >= MAX_FILES:
                    break
                if p.is_file() and p.suffix.lower() in exts:
                    found.append(p)
        except (PermissionError, OSError) as e:
            print(f"     ⚠  {e}")
    return found


def upload_batch(server: str, batch: list[Path]) -> tuple[int, int]:
    files = []
    for p in batch:
        mime = mimetypes.guess_type(str(p))[0] or "application/octet-stream"
        try:
            files.append(("files", (p.name, p.read_bytes(), mime)))
        except (PermissionError, OSError):
            pass

    if not files:
        return 0, 0

    try:
        r = requests.post(f"{server}/api/upload", files=files, timeout=60)
        r.raise_for_status()
        data = r.json()
        return data.get("total_saved", 0), len(data.get("errors", []))
    except Exception as e:
        print(f"  ❌  Batch failed: {e}")
        return 0, len(batch)


def main():
    parser = argparse.ArgumentParser(description="Auto-scan and upload images to the LAN server.")
    parser.add_argument("--server",  required=True, help="Server URL, e.g. http://192.168.1.5:8000")
    parser.add_argument("--roots",   nargs="+",     help="Folders to scan")
    parser.add_argument("--ext",     nargs="+",     help="Extensions to include, e.g. .jpg .png")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    server = args.server.rstrip("/")
    roots  = [Path(r) for r in args.roots] if args.roots else DEFAULT_ROOTS
    exts   = set(args.ext) if args.ext else DEFAULT_EXTS

    print(f"\n{'='*54}")
    print(f"  📷  LAN Photo Client Uploader")
    print(f"{'='*54}")
    print(f"  Server : {server}")
    print(f"  Folders: {', '.join(str(r) for r in roots)}")
    print(f"  Dry run: {args.dry_run}")
    print(f"{'='*54}\n")

    # Verify server reachable
    try:
        requests.get(server, timeout=5).raise_for_status()
    except Exception as e:
        print(f"❌  Cannot reach server at {server}\n    {e}")
        sys.exit(1)

    print("📁  Scanning for images…")
    images = scan(roots, exts)
    total  = len(images)
    print(f"\n  Found {total} image(s).\n")

    if total == 0 or args.dry_run:
        if args.dry_run and total > 0:
            print("Dry run — files that would be uploaded:")
            for p in images[:50]:
                print(f"  {p}")
            if total > 50:
                print(f"  … and {total - 50} more")
        return

    # Upload in batches
    saved_total  = 0
    errors_total = 0
    batches      = [images[i:i+CHUNK_SIZE] for i in range(0, total, CHUNK_SIZE)]

    for idx, batch in enumerate(batches, 1):
        print(f"  ⬆  Batch {idx}/{len(batches)}  ({len(batch)} files)…", end=" ", flush=True)
        s, e = upload_batch(server, batch)
        saved_total  += s
        errors_total += e
        pct = round(((idx * CHUNK_SIZE) / total) * 100, 1)
        print(f"✓  {saved_total} saved so far  [{min(pct,100)}%]")

    print(f"\n{'='*54}")
    print(f"  ✅  Done!  {saved_total} saved  |  {errors_total} errors")
    print(f"{'='*54}\n")


if __name__ == "__main__":
    main()
