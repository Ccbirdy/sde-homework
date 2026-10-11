"""One-time state migration: unlink only unused dev Landing resources, never delete.

Run from any directory. Temporarily restores the old resource keys for the CLI,
in the existing Bundle root so identity and remote state location stay unchanged.
The temporary definitions are removed before deployment, even on failure.
"""
import subprocess
from pathlib import Path


BUNDLE = Path(__file__).resolve().parents[1] / "databricks"
LEGACY = """resources:
  schemas:
    landing_schema:
      name: ${bundle.target}_landing
      catalog_name: ${var.catalog}
      lifecycle:
        prevent_destroy: true
  volumes:
    landing_files:
      name: ${bundle.target}_source_files
      catalog_name: ${var.catalog}
      schema_name: ${resources.schemas.landing_schema.name}
      volume_type: MANAGED
      lifecycle:
        prevent_destroy: true
"""


def unbind(bundle=BUNDLE, runner=subprocess.run):
    config = bundle / "databricks.yml"
    overlay = bundle / "resources" / "_unbind_unused_landing.yml"
    original = config.read_bytes()
    text = original.decode("utf-8")
    if text.count("include:\n") != 1:
        # Match Windows checkouts while restoring their exact original bytes afterwards.
        text = text.replace("\r\n", "\n")
    if text.count("include:\n") != 1:
        raise ValueError("Expected one Bundle include list; no files changed.")
    if overlay.exists():
        raise ValueError("Migration overlay already exists; inspect it before retrying.")
    try:
        overlay.write_text(LEGACY, encoding="utf-8")
        config.write_text(text.replace("include:\n", "include:\n  - resources/_unbind_unused_landing.yml\n", 1), encoding="utf-8")
        for key in ("landing_files", "landing_schema"):
            print(f"Unbinding {key} from dev Bundle state; cloud object is retained.", flush=True)
            runner(["databricks", "bundle", "deployment", "unbind", key, "-t", "dev"],
                   cwd=bundle, check=True)
    finally:
        config.write_bytes(original)
        overlay.unlink(missing_ok=True)
    print("Dev Landing resources unbound. Deploy normally, then run the empty-Volume cleanup notebook.")


if __name__ == "__main__":
    unbind()
