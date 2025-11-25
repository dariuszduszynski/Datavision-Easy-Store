# datavision_easystore/buffer_to_des.py
"""
Prosty prototyp procesu:
- bierze pliki z katalogu "buffer_dir"
- traktuje nazwę pliku jako nasze "db_id" (dla demo)
- pakuje pliki do DailyShardedDesStore
- wypisuje mapowanie: db_id -> des_name, des_path

Docelowo:
- zamiast buffer_dir -> S3 (buffer)
- zamiast db_id z nazwy -> id z tabeli DB
- zamiast print -> UPDATE w DB po uploadzie DES do S3
"""

from datetime import date
from pathlib import Path

from des.packer.daily_sharded_store import DailyShardedDesStore


def pack_buffer_directory(
    buffer_dir: str | Path,
    des_base_dir: str | Path,
    shard_bits: int = 8,
    day: date | None = None,
    node_id: int = 0,
):
    day = day or date.today()
    buffer_dir = Path(buffer_dir)

    with DailyShardedDesStore(
        base_dir=des_base_dir,
        shard_bits=shard_bits,
        day=day,
        node_id=node_id,
        prefix="UserCustom",
    ) as store:
        for path in buffer_dir.glob("*"):
            if not path.is_file():
                continue

            db_id = path.stem  # DEMO: udajemy, że nazwa pliku to id z DB
            logical_name, container_path = store.add_file_from_path(
                path,
                meta={"db_id": db_id, "source_path": str(path)},
                keep_ext=True,
            )
            print(
                f"DB_ID={db_id} -> DES_NAME={logical_name} "
                f"IN={path} SHARD_FILE={container_path}"
            )

    print("Done building daily DES containers.")


if __name__ == "__main__":
    # demo
    pack_buffer_directory(
        buffer_dir="buffer",          # wrzuć tu pliki testowe
        des_base_dir="data/des",      # tu powstaną katalogi YYYY-MM-DD /
        shard_bits=8,
        node_id=1,
    )
