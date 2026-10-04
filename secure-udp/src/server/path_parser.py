from pathlib import Path


def parse_request(payload: bytes, base_dir: str | Path) -> Path:
    try:
        requested = payload.decode("utf-8")
    except UnicodeDecodeError:
        raise ValueError("Invalid path encoding")

    if not requested.startswith("GET "):
        raise ValueError("Invalid request")

    filepath = requested[4:]

    if not filepath.startswith("/"):
        raise ValueError("Path must start with /")

    filename = filepath[1:]

    if not filename or "/" in filename or "\\" in filename:
        raise ValueError("Invalid filename")

    base_dir = Path(base_dir).resolve()
    path = (base_dir / filename).resolve()

    if path.parent != base_dir:
        raise ValueError("Invalid path")

    if not path.is_file():
        raise ValueError("File not exist")

    return path

