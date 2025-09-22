from datetime import datetime, timedelta
from typing import List

import numpy as np
import torch
import torch.nn as nn

from .schemas import IndicatorKey, TimeSeriesPoint


torch.set_num_threads(1)


class SmallLSTM(nn.Module):
    def __init__(self, input_size: int = 1, hidden_size: int = 16, num_layers: int = 1):
        super().__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_size, 1)

    def forward(self, x):
        out, _ = self.lstm(x)
        out = self.fc(out[:, -1, :])
        return out


def _to_supervised(values: np.ndarray, window: int = 12):
    X, y = [], []
    for i in range(len(values) - window):
        X.append(values[i:i+window])
        y.append(values[i+window])
    X = np.array(X).reshape(-1, window, 1)
    y = np.array(y).reshape(-1, 1)
    return X, y


def _next_month_str(last_iso: str) -> str:
    dt = datetime.fromisoformat(last_iso)
    year = dt.year + (1 if dt.month == 12 else 0)
    month = 1 if dt.month == 12 else dt.month + 1
    return f"{year:04d}-{month:02d}-01"


async def forecast_indicator(indicator: IndicatorKey, series: List[TimeSeriesPoint], horizon: int = 12) -> List[TimeSeriesPoint]:
    if len(series) < 24:
        # naive fallback
        last = series[-1]
        out = []
        t = last.t
        v = last.v
        for _ in range(horizon):
            t = _next_month_str(t)
            out.append(TimeSeriesPoint(t=t, v=v))
        return out

    values = np.array([p.v for p in series], dtype=np.float32)
    min_v, max_v = float(values.min()), float(values.max())
    scale = max(max_v - min_v, 1e-6)
    values_norm = (values - min_v) / scale

    window = 12
    X, y = _to_supervised(values_norm, window)
    X_t = torch.from_numpy(X)
    y_t = torch.from_numpy(y)

    model = SmallLSTM()
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

    model.train()
    epochs = 60
    for _ in range(epochs):
        optimizer.zero_grad()
        pred = model(X_t)
        loss = criterion(pred, y_t)
        loss.backward()
        optimizer.step()

    # autoregressive forecast
    model.eval()
    context = values_norm[-window:].reshape(1, window, 1)
    context_t = torch.from_numpy(context)
    preds = []
    with torch.no_grad():
        for _ in range(horizon):
            yhat = model(context_t).numpy().reshape(-1)[0]
            preds.append(float(yhat))
            # append and slide window
            new_context = np.concatenate([context.reshape(-1), [yhat]])[-window:]
            context_t = torch.from_numpy(new_context.reshape(1, window, 1))

    # invert scaling
    preds = [min_v + p * scale for p in preds]

    # produce dates
    t = series[-1].t
    out: List[TimeSeriesPoint] = []
    for v in preds:
        t = _next_month_str(t)
        out.append(TimeSeriesPoint(t=t, v=float(v)))
    return out
