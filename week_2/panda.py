import sys
import pandas as pd
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

    data = pd.read_csv(f, usecols=['timestamp', 'pixel_color', 'coordinate'])
    filt = data[(data['timestamp'] >= start) & (data['timestamp'] <= end)]

    max_color = filt['pixel_color'].value_counts().idxmax()
    max_location = filt['coordinate'].value_counts().idxmax()

    t1 = time.perf_counter_ns()
    dff = (t1 - t0) / 1000000

    print(f"- **Timeframe:**: {sys.argv[2]} {sys.argv[3]} - {sys.argv[4]} {sys.argv[5]}")
    print(f"- **Execution Time:** {dff:.2f} ms")
    print(f"- **Most Placed Color:** {max_color}")
    print(f"- **Most Placed Pixel Location:** {max_location}")


if __name__ == "__main__":
    main()
