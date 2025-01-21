# Almas Perneshev
# CSC369
# Asngment 1
# 1/10/2025


import sys
import gzip
import datetime
import time

def parsearg():
    if len(sys.argv) != 6:
        print("Usage: python3 asgn1.py 2022_place_canvas_history.csv.gzip YYYY-MM-DD HH YYYY-MM-DD HH")
        sys.exit(1)

    start = datetime.datetime.strptime(f"{sys.argv[2]} {sys.argv[3]}", "%Y-%m-%d %H")
    end = datetime.datetime.strptime(f"{sys.argv[4]} {sys.argv[5]}", "%Y-%m-%d %H")
    # print(start)
    # print(end)
    if end <= start:
        print("start <= end")
        sys.exit(1)

    return start, end


def parsetime(timestamp):
    fs = ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"]
    for i in fs:
        try:
            return datetime.datetime.strptime(timestamp, i)
        except ValueError:
            continue


def main():
    start, end = parsearg()
    color = {}
    location = {}
    t0 = time.perf_counter_ns()

    with gzip.open(sys.argv[1], "rt") as f:
        for line in f:
            # print(line)
            parts = line.strip().split(',', 3)
            t = parts[0].replace("UTC", "").strip()
            pcolor = parts[2].strip()
            rlocation = parts[3].strip().strip('"')

            ts = parsetime(t)

            if ts is not None and start <= ts < end:
                color[pcolor] = color.get(pcolor, 0) + 1
                location[rlocation] = location.get(rlocation, 0) + 1

    t1 = time.perf_counter_ns()
    # print(t1)
    dff = (t1 - t0) / 1000000
    # print(dff)

    if not color or not location:
        print("No data found")
        return

    max_color = max(color, key=color.get)
    max_location = max(location, key=location.get)
    # print(max_location)


    print(f"Timeframe: {sys.argv[2]} {sys.argv[3]} - {sys.argv[4]} {sys.argv[5]}")
    print(f"Execution Time: {dff:.2f} ms")
    print(f"Most Placed Color: {max_color}")
    print(f"Most Placed Pixel Location: ({max_location})")


if __name__ == "__main__":
    main()
