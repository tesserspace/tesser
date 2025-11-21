from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterator, List, Tuple, Union

DateLike = Union[str, date, datetime]
PathLike = Union[str, Path]

_PARTITION_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _normalize_date(value: DateLike | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(f"invalid date literal '{value}' (expected YYYY-MM-DD)") from exc
    raise TypeError("date filters must be date, datetime or YYYY-MM-DD strings")


@dataclass(frozen=True)
class Partition:
    date: date
    path: Path


class DataStore:
    """Scanner for the flight recorder directory structure."""

    SUPPORTED_KINDS: Tuple[str, ...] = ("ticks", "candles", "orders", "fills")

    def __init__(self, data_dir: PathLike):
        root = Path(data_dir).expanduser()
        if not root.exists():
            raise FileNotFoundError(f"flight recorder directory '{root}' does not exist")
        self.root = root

    def available_kinds(self) -> List[str]:
        return [kind for kind in self.SUPPORTED_KINDS if (self.root / kind).exists()]

    def files(
        self,
        kind: str,
        *,
        start_date: DateLike | None = None,
        end_date: DateLike | None = None,
    ) -> List[Path]:
        path = self._kind_path(kind)
        start = _normalize_date(start_date)
        end = _normalize_date(end_date)
        if start and end and end < start:
            raise ValueError("end_date must be on or after start_date")
        partitions = list(self._partitions(path))
        if start:
            partitions = [p for p in partitions if p.date >= start]
        if end:
            partitions = [p for p in partitions if p.date <= end]
        files: List[Path] = []
        for partition in partitions:
            files.extend(sorted(partition.path.glob("*.parquet")))
        return files

    def _kind_path(self, kind: str) -> Path:
        normalized = kind.strip().lower()
        if normalized not in self.SUPPORTED_KINDS:
            raise ValueError(
                f"unsupported dataset '{kind}'. Expected one of: {', '.join(self.SUPPORTED_KINDS)}"
            )
        path = self.root / normalized
        if not path.exists():
            raise FileNotFoundError(f"dataset directory '{path}' is missing")
        return path

    def _partitions(self, path: Path) -> Iterator[Partition]:
        if not path.exists():
            return iter(())
        partitions: List[Partition] = []
        for entry in path.iterdir():
            if not entry.is_dir():
                continue
            name = entry.name
            if not _PARTITION_PATTERN.match(name):
                continue
            partitions.append(Partition(date=datetime.strptime(name, "%Y-%m-%d").date(), path=entry))
        partitions.sort(key=lambda item: item.date)
        return iter(partitions)

    # Convenience helpers that defer imports to avoid cycles.
    def load_ticks(self, **kwargs):  # pragma: no cover - thin wrapper
        from .loaders import load_ticks

        return load_ticks(self, **kwargs)

    def load_candles(self, **kwargs):  # pragma: no cover - thin wrapper
        from .loaders import load_candles

        return load_candles(self, **kwargs)

    def load_orders(self, **kwargs):  # pragma: no cover - thin wrapper
        from .loaders import load_orders

        return load_orders(self, **kwargs)

    def load_fills(self, **kwargs):  # pragma: no cover - thin wrapper
        from .loaders import load_fills

        return load_fills(self, **kwargs)
