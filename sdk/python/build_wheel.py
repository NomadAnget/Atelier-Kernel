#!/usr/bin/env python3
"""
Zero-dependency PEP 427 Wheel Builder for `atelier-sdk`.
Builds a standard `atelier_sdk-0.1.7-py3-none-any.whl` package using only the
Python standard library (`zipfile`, `hashlib`, `base64`), so automated builds
succeed in both offline/sandboxed environments and CI/CD runners.
"""
import base64
import hashlib
import os
import sys
import zipfile
from pathlib import Path

PKG_NAME = "atelier_sdk"
DIST_NAME = "atelier-sdk"
VERSION = "0.1.7"
TAG = "py3-none-any"


def sha256_b64_nopad(data: bytes) -> str:
    digest = hashlib.sha256(data).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def build_wheel(out_dir: Path) -> Path:
    here = Path(__file__).resolve().parent
    readme_md = (here / "README.md").read_text(encoding="utf-8")

    dist_info = f"{PKG_NAME}-{VERSION}.dist-info"
    wheel_filename = f"{PKG_NAME}-{VERSION}-{TAG}.whl"
    out_dir.mkdir(parents=True, exist_ok=True)
    wheel_path = out_dir / wheel_filename

    metadata_content = (
        "Metadata-Version: 2.1\n"
        f"Name: {DIST_NAME}\n"
        f"Version: {VERSION}\n"
        "Summary: Official Python Domain SDK for Atelier Kernel\n"
        "License: MIT\n"
        "Requires-Python: >=3.8\n"
        "Description-Content-Type: text/markdown\n\n"
        f"{readme_md}"
    ).encode("utf-8")

    wheel_content = (
        "Wheel-Version: 1.0\n"
        "Generator: atelier-sdk-wheel-builder (0.1.7)\n"
        "Root-Is-Purelib: true\n"
        f"Tag: {TAG}\n"
    ).encode("utf-8")

    top_level_content = f"{PKG_NAME}\n".encode("utf-8")

    files = [
        *((f"{PKG_NAME}/{path.relative_to(here / PKG_NAME).as_posix()}", path.read_bytes())
          for path in sorted((here / PKG_NAME).rglob("*.py"))),
        (f"{dist_info}/METADATA", metadata_content),
        (f"{dist_info}/WHEEL", wheel_content),
        (f"{dist_info}/top_level.txt", top_level_content),
    ]

    record_lines = []
    for arcname, raw_bytes in files:
        h = sha256_b64_nopad(raw_bytes)
        record_lines.append(f"{arcname},sha256={h},{len(raw_bytes)}")
    record_lines.append(f"{dist_info}/RECORD,,")
    record_content = ("\n".join(record_lines) + "\n").encode("utf-8")

    with zipfile.ZipFile(wheel_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for arcname, raw_bytes in files:
            zf.writestr(arcname, raw_bytes)
        zf.writestr(f"{dist_info}/RECORD", record_content)

    return wheel_path


if __name__ == "__main__":
    target_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent / "dist"
    whl = build_wheel(target_dir)
    print(f"Built Python wheel: {whl}")
