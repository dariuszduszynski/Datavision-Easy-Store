"""Catalog table model for DES marker worker."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from des.db.connector import Base


class CatalogEntry(Base):
    """Source catalog entry awaiting DES packing."""

    __tablename__ = "des_source_catalog"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    des_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    des_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    des_shard: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    des_status: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)

    source_bucket: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    source_key: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)

    def __repr__(self) -> str:
        return (
            f"CatalogEntry(id={self.id}, des_name={self.des_name}, "
            f"des_status={self.des_status})"
        )
