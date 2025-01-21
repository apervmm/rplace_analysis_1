import sys
import polars as pl
import datetime
import time

def parsearg():
    if len(sys.argv) != 6:
        print("Usage: python panda.py 2022_place_canvas_history.csv YYYY-MM-DD HH YYYY-MM-DD HH")
        sys.exit(1)

    start = datetime.datetime.strptime(f"{sys.argv[2]} {sys.argv[3]}", "%Y-%m-%d %H")
    end = datetime.datetime.strptime(f"{sys.argv[4]} {sys.argv[5]}", "%Y-%m-%d %H")
    # print(start)
    # print(end)
    if end <= start:
        print("start <= end")
        sys.exit(1)
    return  sys.argv[1], start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")

def main():
    t0 = time.perf_counter_ns()
    f, start, end = parsearg()

    data = pl.scan_csv(f)

    filtered = data.filter(
        (pl.col("timestamp") >= start) & (pl.col("timestamp") < end)
    )

    max_color = (
        filtered.group_by("pixel_color")
        .agg(pl.count("pixel_color").alias("count"))
        .sort("count", descending=True)
        .select("pixel_color")
        .first()
        .collect()
    )
    max_location = (
        filtered.group_by("coordinate")
        .agg(pl.count("coordinate").alias("count"))
        .sort("count", descending=True)
        .select("coordinate")
        .first()
        .collect()
    )

    t1 = time.perf_counter_ns()
    dff = (t1 - t0) / 1000000  

    print(f"- **Timeframe:**: {sys.argv[2]} {sys.argv[3]} - {sys.argv[4]} {sys.argv[5]}")
    print(f"- **Execution Time:** {dff:.2f} ms")
    print(f"- **Most Placed Color:** {max_color[0, 0]}")
    print(f"- **Most Placed Pixel Location:** {max_location[0, 0]}")


if __name__ == "__main__":
    main()
