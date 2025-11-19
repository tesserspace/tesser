from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Iterable, List


def to_dataframe(items: Iterable[object]):  # pragma: no cover
    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "pandas is not installed. Install tesser[data] extra to enable DataFrame exports."
        ) from exc

    rows: List[dict[str, Any]] = []
    for item in items:
        if not is_dataclass(item) or isinstance(item, type):
            raise TypeError("to_dataframe expects dataclass instances")
        rows.append(asdict(item))  # type: ignore[arg-type]
    return pd.DataFrame(rows)
