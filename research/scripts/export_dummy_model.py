"""Export a lightweight LSTM model for Cortex benchmarks."""

from __future__ import annotations

import pathlib

import torch
import torch.nn as nn


class QuantLstm(nn.Module):
    def __init__(self, input_size: int = 5, hidden_size: int = 32, output_size: int = 3) -> None:
        super().__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, batch_first=True)
        self.fc = nn.Linear(hidden_size, output_size)
        self.softmax = nn.Softmax(dim=1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:  # type: ignore[override]
        out, _ = self.lstm(x)
        out = out[:, -1, :]
        out = self.fc(out)
        return self.softmax(out)


def export() -> None:
    model = QuantLstm()
    model.eval()

    dummy_input = torch.randn(1, 10, 5)

    output_path = pathlib.Path(__file__).resolve().parents[1] / "models" / "lstm_v1.onnx"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Exporting to {output_path}...")
    torch.onnx.export(
        model,
        dummy_input,
        output_path,
        input_names=["input"],
        output_names=["output"],
        dynamic_axes={"input": {0: "batch_size"}, "output": {0: "batch_size"}},
        opset_version=16,
        dynamo=False,
    )
    print("Done.")


if __name__ == "__main__":
    export()
