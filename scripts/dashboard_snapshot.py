"""Render the CloudWatch dashboard to a PNG without the console (CloudWatch GetMetricWidgetImage).

    python scripts/dashboard_snapshot.py [--hours 3] [--out docs/images/dashboard.png]

Each graph widget of the `fraud-analyst` dashboard is rendered by CloudWatch and the images are
tiled in dashboard order. Text, single-value and alarm-status widgets are skipped.
"""

from __future__ import annotations

import argparse
import io
import json

import boto3
from PIL import Image

TILE_W, TILE_H, COLUMNS = 640, 300, 3


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dashboard", default="fraud-analyst")
    parser.add_argument("--hours", type=int, default=3)
    parser.add_argument("--out", default="docs/images/dashboard.png")
    args = parser.parse_args()

    cw = boto3.client("cloudwatch")
    body = json.loads(cw.get_dashboard(DashboardName=args.dashboard)["DashboardBody"])
    tiles = []
    for widget in body["widgets"]:
        if widget["type"] != "metric":
            continue
        props = dict(widget["properties"])
        # The image API cannot draw single-value tiles, and a bar would show one period, not the total.
        if props.get("view") == "singleValue":
            continue
        props.update({"width": TILE_W, "height": TILE_H, "start": f"-PT{args.hours}H", "end": "P0D"})
        png = cw.get_metric_widget_image(MetricWidget=json.dumps(props), OutputFormat="png")["MetricWidgetImage"]
        tiles.append(Image.open(io.BytesIO(png)).convert("RGB"))

    rows = (len(tiles) + COLUMNS - 1) // COLUMNS
    sheet = Image.new("RGB", (COLUMNS * TILE_W, rows * TILE_H), "white")
    for i, tile in enumerate(tiles):
        sheet.paste(tile, ((i % COLUMNS) * TILE_W, (i // COLUMNS) * TILE_H))
    sheet.save(args.out, optimize=True)
    print(f"wrote {args.out}: {len(tiles)} widgets, {sheet.size[0]}x{sheet.size[1]}")


if __name__ == "__main__":
    main()
