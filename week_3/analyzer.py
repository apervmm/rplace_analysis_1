import sys
import datetime
import time
import duckdb
import webcolors

con = duckdb.connect()




def parsearg():
    if len(sys.argv) != 6:
        print("Usage: python analyzer.py <parquet file> YYYY-MM-DD HH YYYY-MM-DD HH")
        sys.exit(1)

    start = datetime.datetime.strptime(f"{sys.argv[2]} {sys.argv[3]}", "%Y-%m-%d %H")
    end = datetime.datetime.strptime(f"{sys.argv[4]} {sys.argv[5]}", "%Y-%m-%d %H")

    if end <= start:
        print("start > end")
        sys.exit(1)
    return sys.argv[1], start, end



def hex_to_name(hex):
    try:
        return webcolors.hex_to_name(hex)
    except:
        cls = {}
        for name in webcolors.names("css3"):
            cls[name] = webcolors.name_to_rgb(name)
        
        target = webcolors.hex_to_rgb(hex)
        min_distance = float("inf")
        closest = None
        
        for name in cls:
            distance = 0
            for i in range(3):
                distance += (cls[name][i] - target[i]) ** 2
            
            if distance < min_distance:
                min_distance = distance
                closest = name
        return closest

def rank_colors(p, start, end):
    print(f"### Ranking of Colors by Distinct Users")
    query = f"""
        SELECT pixel_color, COUNT(DISTINCT user_id) AS dis_usr
        FROM parquet_scan('{p}')
        WHERE timestamp BETWEEN '{start}' AND '{end}'
        GROUP BY pixel_color
        ORDER BY dis_usr DESC
    """
    result = con.execute(query).fetchall()
    print("- **Top **")
    for i in range(len(result)):
        print(f" {i}. {hex_to_name(result[i][0])}: {result[i][1]} users")
    return None



def avg_session(p, start, end):
    query = f"""
        WITH usession AS (
            SELECT user_id, timestamp,
                timestamp - LAG(timestamp) OVER (PARTITION BY user_id ORDER BY timestamp) AS diff
            FROM parquet_scan('{p}')
            WHERE timestamp BETWEEN '{start}' AND '{end}'
        ),
        sessions AS (
            SELECT user_id, timestamp, 
                SUM(CASE WHEN diff > INTERVAL '15 minutes' OR diff IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY user_id ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
            FROM usession
        ),
        session_user AS (
            SELECT user_id, session_id, MAX(timestamp) - MIN(timestamp) AS session_length
            FROM sessions
            GROUP BY user_id, session_id
            HAVING COUNT(*) > 1
        )
        SELECT
            AVG(EXTRACT(EPOCH FROM session_length)) AS avg_session_length
        FROM session_user
    """
    result = con.execute(query).fetchone()
    print("\n### Average Session Length")
    print(f"- **Output:** {result[0]:.2f} seconds" if result[0] else "NA")
    return None


def pixel_perc(p, start, end):
    query = f"""
        WITH up AS (
            SELECT user_id, COUNT(*) AS p
            FROM parquet_scan('{p}')
            WHERE timestamp BETWEEN '{start}' AND '{end}'
            GROUP BY user_id
        )
        SELECT
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY p) AS p50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY p) AS p75,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY p) AS p90,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY p) AS p99
        FROM up
    """
    result = con.execute(query).fetchone()
    print("\n### Percentiles of Pixels Placed")
    print("- **Output:**")
    print(f"- 50th Percentile: {result[0]:.0f} pixels")
    print(f"- 75th Percentile: {result[1]:.0f} pixels")
    print(f"- 90th Percentile: {result[2]:.0f} pixels")
    print(f"- 99th Percentile: {result[3]:.0f} pixels")
    return None

def first_user(p, start, end):
    query = f"""
        WITH first_placements AS (
            SELECT user_id, MIN(timestamp) AS first_time
            FROM parquet_scan('{p}')
            GROUP BY user_id
        )
        SELECT COUNT(*)
        FROM first_placements
        WHERE first_time BETWEEN '{start}' AND '{end}'
    """
    result = con.execute(query).fetchone()
    print("\n### Count of First-Time Users")
    print(f"- **Output:** {result[0]} users")
    return None


def main():
    parq, start, end = parsearg()


    t0 = time.perf_counter_ns()
    rank_colors(parq, start, end)
    t1 = time.perf_counter_ns()
    print(f"\nRuntime tsk1: {(t1 - t0)/1000000} ms")


    avg_session(parq, start, end)
    t2 = time.perf_counter_ns()
    print(f"\nRuntime tsk2: {(t2 - t1)/1000000} ms")

    pixel_perc(parq, start, end)
    t3 = time.perf_counter_ns()
    print(f"\nRuntime tsk3: {(t3 - t2)/1000000} ms")

    first_user(parq, start, end)
    t4 = time.perf_counter_ns()
    print(f"\nRuntime tsk4: {(t4 - t3)/1000000} ms")

    print(f"\n### Total Runtime\n {(t4 - t0)/1000000} ms")


if __name__ == "__main__":
    main()
