"""Build a source-only Linux remote-build package, never deploy it."""

from io import BytesIO
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


ROOT = Path(__file__).resolve().parent
FILES = (
    "function_app.py",
    "host.json",
    "requirements.txt",
    "schema.sql",
    "renegociacao/__init__.py",
    "renegociacao/bootstrap.py",
    "renegociacao/core.py",
    "renegociacao/dataverse.py",
    "renegociacao/directline_token.py",
    "renegociacao/odata.py",
    "renegociacao/security.py",
    "renegociacao/sql.py",
    "renegociacao/whatsapp_demo.py",
)


def content() -> bytes:
    stream = BytesIO()
    with ZipFile(stream, "w", compression=ZIP_DEFLATED) as archive:
        for name in FILES:
            entry = ZipInfo(name)
            entry.create_system = 3
            entry.external_attr = 0o100644 << 16
            entry.compress_type = ZIP_DEFLATED
            archive.writestr(entry, (ROOT / name).read_bytes())
    return stream.getvalue()


if __name__ == "__main__":
    output = ROOT / "renegociacao.zip"
    output.write_bytes(content())
    print(f"Created {output.name}; {len(FILES)} source files; no deployment performed.")
