import sys
import duckdb
import datetime
import time

def parsearg():
    if len(sys.argv) != 6:
        print("Usage: python duckdb.py 2022_place_canvas_history.csv YYYY-MM-DD HH YYYY-MM-DD HH")
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

    conn = duckdb.connect()

    qcolor = f"""
        SELECT pixel_color, COUNT(*) AS color_count
        FROM read_csv_auto('{f}')
        WHERE timestamp >= '{start}' AND timestamp < '{end}'
        GROUP BY pixel_color
        ORDER BY color_count DESC
        LIMIT 1
    """

    qloc = f"""
        SELECT coordinate, COUNT(*) AS coord_count
        FROM read_csv_auto('{f}')
        WHERE timestamp >= '{start}' AND timestamp < '{end}'
        GROUP BY coordinate
        ORDER BY coord_count DESC
        LIMIT 1
    """

    max_color = conn.execute(qcolor).fetchall()
    max_location = conn.execute(qloc).fetchall()


    t1 = time.perf_counter_ns()
    dff = (t1 - t0) / 1000000

    print(f"Timeframe: {sys.argv[2]} {sys.argv[3]} - {sys.argv[4]} {sys.argv[5]}")
    print(f"Execution Time: {dff:.2f} ms")
    print(f"Most Placed Color: {max_color[0][0]}")
    print(f"Most Placed Pixel Location: {max_location[0][0]}")


if __name__ == "__main__":
    main()
