from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, date_format, countDistinct
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import seaborn as sns


def initialize_spark():
    return SparkSession.builder.appName("rPlaceAnalysis").config("spark.sql.shuffle.partitions", "200").config("spark.driver.memory", "12g").getOrCreate()


def plot_quadrants(df):
    plt.figure(figsize=(14, 7))

    df["hour"] = pd.to_datetime(df["hour"])
    for quadrant in df["quadrant"].unique():
        subset = df[df["quadrant"] == quadrant]
        plt.plot(subset["hour"], subset["placements"], label=quadrant, marker="o", linestyle="-")
        
    plt.xlabel("Time (Hourly Bins)")
    plt.ylabel("# of User Color Placements")
    plt.title("User Color Placements Per Hour for Each Quadrant")
    plt.legend()
    plt.grid(True, linestyle="--", linewidth=0.5)
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=6))
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
    plt.xticks(rotation=45)

    plt.tight_layout()
    plt.show()

def plot_quadrant_activity(df):
    df = df.withColumn("hour", date_format(col("timestamp"), "yyyy-MM-dd HH:00"))
    quadrant_activity = df.groupBy(
        col("hour"),
        when((col("x") < 1000) & (col("y") < 1000), "Top-left")
        .when((col("x") >= 1000) & (col("y") < 1000), "Top-right")
        .when((col("x") < 1000) & (col("y") >= 1000), "Bottom-left")
        .otherwise("Bottom-right")
        .alias("quadrant")
    ).agg(count("*").alias("placements")).orderBy("hour").toPandas()
    plot_quadrants(quadrant_activity)


def compute_heatmap(df):
    heatmap_df = df.groupBy("x", "y").agg(count("*").alias("modifications"))

    heatmap_pandas = heatmap_df.toPandas().pivot(index="y", columns="x", values="modifications").fillna(0)

    plt.figure(figsize=(12, 8))
    sns.heatmap(heatmap_pandas, cmap="inferno", cbar=True, linewidths=0, robust=True)

    plt.title("r/Place 2022 Heatmap of Modifications")
    plt.xlabel("X Coordinate")
    plt.ylabel("Y Coordinate")
    plt.show()



def count_rect(df, expansion_time):
    return df.filter(
        (col("timestamp") >= expansion_time) &
        (col("x") >= 0) & (col("x") <= 250) &
        (col("y") >= 1350) & (col("y") <= 2000)
    ).agg(countDistinct("user_id").alias("unique_users_in_qiii_bottom_left")).collect()[0][0]


def count_exp(df, expansion_time):
    return df.filter(col("timestamp") < expansion_time)\
             .agg(countDistinct("user_id").alias("unique_users_before_expansion"))\
             .collect()[0][0]


def total_users(df):
    return df.agg(countDistinct("user_id").alias("total_users")).collect()[0][0]


def migrated(df, expansion_time):
    before_df = df.filter(col("timestamp") < expansion_time).select("user_id").distinct()
    
    after_df = df.filter(
        (col("timestamp") >= expansion_time) &
        (col("x") >= 0) & (col("x") <= 250) &
        (col("y") >= 1350) & (col("y") <= 2000)
    ).select("user_id").distinct()

    return before_df.join(after_df, "user_id", "inner")\
                    .agg(countDistinct("user_id").alias("migrating_users"))\
                    .collect()[0][0]

def plotUserAct(df, label):
    plt.figure(figsize=(14, 7))
    df["hour"] = pd.to_datetime(df["hour"])
    plt.plot(df["hour"], df["unique_users"], marker="o", linestyle="-", color="b", label=label)

    plt.xlabel("Time (Hourly Bins)")
    plt.ylabel("# of Users Placing Pixels")
    plt.title("User Activity")
    plt.legend()
    plt.grid(True, linestyle="--", linewidth=0.5)
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=1)) 
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
    plt.xticks(rotation=45, fontsize=8)  

    plt.tight_layout()
    plt.show()


def user_activity(df, expansion_time):
    return (
        df.filter(
            (col("timestamp") >= expansion_time) &
            (col("x") >= 0) & (col("x") <= 250) &
            (col("y") >= 1350) & (col("y") <= 2000)
        )
        .withColumn("hour", date_format(col("timestamp"), "yyyy-MM-dd HH:00"))
        .groupBy("hour")
        .agg(count("user_id").alias("unique_users"))
        .orderBy("hour")
    )

def unique_user_activity(df, expansion_time):
    return (
        df.filter(
            (col("timestamp") >= expansion_time) &
            (col("x") >= 0) & (col("x") <= 250) &
            (col("y") >= 1350) & (col("y") <= 2000)
        )
        .withColumn("hour", date_format(col("timestamp"), "yyyy-MM-dd HH:00"))
        .groupBy("hour")
        .agg(countDistinct("user_id").alias("unique_users"))
        .orderBy("hour")
    )

def main():
    parquet_file = "data.parquet"
    expansion_time = "2022-04-03 19:30:00"

    spark = initialize_spark()
    df = spark.read.parquet(parquet_file)

    
    plot_quadrant_activity(df)
    compute_heatmap(df)


    q3 = count_rect(df, expansion_time)
    bExp = count_exp(df, expansion_time)
    total_users_count = total_users(df)
    migrated_users = migrated(df, expansion_time)

    print(f"Number of unique users in QIII bottom-left area: {q3}")
    print(f"Number of unique users before the 3rd expansion: {bExp}")
    print(f"Total unique users: {total_users_count}")
    print(f"Number of users who migrated to QIII bottom-left: {migrated_users}")


    plotUserAct(user_activity(df, expansion_time).toPandas(), "Users in QIII Bottom Left")
    plotUserAct(unique_user_activity(df, expansion_time).toPandas(), "Unique Users in QIII Bottom Left")


if __name__ == "__main__":
    main()
