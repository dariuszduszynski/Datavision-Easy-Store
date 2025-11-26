from __future__ import annotations

# datavision_easystore/daily_sharded_store.py
import hashlib
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Optional, Tuple

from des.core import DesWriter
from des.utils.snowflake_name import SnowflakeNameGenerator, SnowflakeNameConfig


def shard_from_name(name: str, shard_bits: int) -> int:
    """
    Zwraca shard id w [0, 2**shard_bits - 1]
    na podstawie pierwszych shard_bits bitów SHA-256(name).
    """
    if shard_bits <= 0 or shard_bits > 32:
        raise ValueError("shard_bits must be in [1, 32]")

    h = hashlib.sha256(name.encode("utf-8")).digest()
    v = int.from_bytes(h[:4], "big")
    mask = (1 << shard_bits) - 1
    return (v >> (32 - shard_bits)) & mask


class DailyShardedDesStore:
    """
    Dzienny + sharded DES builder.

    - Dany dzień -> katalog: base_dir/YYYY-MM-DD/
    - Każdy shard -> plik: <shard_hex>.des

    add_file/add_file_from_path:
      - generuje nazwę SnowflakeName,
      - wylicza shard z nazwy,
      - wrzuca do odpowiedniego DesWriter-a.
    """

    def __init__(
        self,
        base_dir: str | Path,
        shard_bits: int = 8,
        day: Optional[date] = None,
        node_id: int = 0,
        prefix: str = "UserCustom",
        filename_ext: str = ".des",
    ):
        self.base_dir = Path(base_dir)
        self.shard_bits = shard_bits
        self.num_shards = 1 << shard_bits
        self.day = day or date.today()
        self.filename_ext = filename_ext

        self.day_str = self.day.isoformat()
        self.day_dir = self.base_dir / self.day_str
        self.day_dir.mkdir(parents=True, exist_ok=True)

        self._writers: dict[int, DesWriter] = {}

        self._name_gen = SnowflakeNameGenerator(
            SnowflakeNameConfig(
                node_id=node_id,
                prefix=prefix,
                wrap_bits=32,  # zostawmy zapas
            )
        )

    def _shard_hex(self, shard_id: int) -> str:
        hex_len = (self.shard_bits + 3) // 4
        return f"{shard_id:0{hex_len}x}"

    def _get_shard_writer(self, shard_id: int) -> DesWriter:
        if shard_id in self._writers:
            return self._writers[shard_id]

        shard_hex = self._shard_hex(shard_id)
        path = self.day_dir / f"{shard_hex}{self.filename_ext}"

        writer = DesWriter(str(path))
        self._writers[shard_id] = writer
        return writer

    def _generate_logical_name(self, ext: Optional[str] = None) -> str:
        base = self._name_gen.next_name(self.day)
        if ext:
            if not ext.startswith("."):
                ext = "." + ext
            return base + ext
        return base

    def add_file(
        self,
        data: bytes,
        meta: Optional[dict[str, Any]] = None,
        ext: Optional[str] = None,
    ) -> Tuple[str, Path]:
        """
        Dodaje plik do odpowiedniego sharda.
        Zwraca (logical_name, lokalna_ścieżka_do_DES).
        """
        logical_name = self._generate_logical_name(ext=ext)
        shard_id = shard_from_name(logical_name, self.shard_bits)
        writer = self._get_shard_writer(shard_id)

        writer.add_file(logical_name, data, meta=meta)

        shard_hex = self._shard_hex(shard_id)
        container_path = self.day_dir / f"{shard_hex}{self.filename_ext}"
        return logical_name, container_path

    def add_file_from_path(
        self,
        file_path: str | Path,
        meta: Optional[dict[str, Any]] = None,
        keep_ext: bool = True,
    ) -> Tuple[str, Path]:
        """
        Czyta plik z dysku, pakuje do odpowiedniego DES.
        """
        file_path = Path(file_path)
        data = file_path.read_bytes()

        ext = file_path.suffix if keep_ext else None
        logical_name, container_path = self.add_file(data, meta=meta, ext=ext)
        return logical_name, container_path

    def close(self) -> None:
        for w in self._writers.values():
            w.close()
        self._writers.clear()

    def __enter__(self) -> "DailyShardedDesStore":
        return self

    def __exit__(self, exc_type: Optional[type[BaseException]], exc: Optional[BaseException], tb: Any) -> None:
        self.close()


def iter_daily_des_files(base_dir: str | Path, day: date) -> Iterable[Path]:
    day_str = day.isoformat()
    day_dir = Path(base_dir) / day_str
    if not day_dir.exists():
        return []
    return day_dir.glob("*.des")
