"""High-level analytics helpers built on top of flight recorder data."""

from .execution import calculate_slippage, summarize_fills

__all__ = ["calculate_slippage", "summarize_fills"]
