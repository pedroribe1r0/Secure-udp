from pathlib import Path

from ..shared.constants import CHUNK_SIZE


class FileManager:
    def __init__(self, base_dir: str | Path):
        self.base_dir = Path(base_dir).resolve()

    def resolve_path(self, filename: str | Path) -> Path:
        path = (self.base_dir / filename).resolve()
        if not path.is_relative_to(self.base_dir):
            raise ValueError("Invalid path")
        return path

    def read_chunk(self, filename: str | Path, position: int) -> bytes:
        with self.resolve_path(filename).open("rb") as file:
            file.seek(position)
            return file.read(CHUNK_SIZE)

    def write_chunk(self, filename: str | Path, data: bytes, position: int) -> None:
        mode = "wb" if position == 0 else "r+b"
        with self.resolve_path(filename).open(mode) as file:
            file.seek(position)
            file.write(data)

    def delete_file(self, filename: str | Path) -> None:
        self.resolve_path(filename).unlink(missing_ok=True)
