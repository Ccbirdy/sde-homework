"""Verify that deployment uploads the unchanged synthetic SAP source files."""
import hashlib
import json
from pathlib import Path

source_dir = Path(__file__).resolve().parents[1] / "data" / "source"
manifest = json.loads((source_dir / "sha256.json").read_text(encoding="utf-8"))
actual_names = {path.name for path in source_dir.glob("*.csv")}
if actual_names != set(manifest) or len(manifest) != 7:
    raise SystemExit("Expected exactly the seven source CSV files.")
for name, expected in manifest.items():
    if hashlib.sha256((source_dir / name).read_bytes()).hexdigest() != expected:
        raise SystemExit(f"Source checksum mismatch: {name}")
print("Verified all seven source CSV checksums.")
