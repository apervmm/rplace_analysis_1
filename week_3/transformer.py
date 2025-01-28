import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa
import polars as pl

csv_file = "../2022_place_canvas_history.csv"
parquet_file = "data.parquet"

BLOCK_SIZE = 100_000_000
hash = {}
count = 0

read_options = pv.ReadOptions(block_size=BLOCK_SIZE)
csv_reader = pv.open_csv(csv_file, read_options=read_options)

parquet_writer = None

try:
    for record_batch in csv_reader:
        print(f"Processing batch with {record_batch.num_rows} rows...")

        df = pl.from_arrow(record_batch)

        # Convert timestamp and split coordinates
        df = (df
            .with_columns(
                pl.col("timestamp")
                .str.replace(r" UTC$", "")
                .str.strptime(
                    pl.Datetime,
                    format="%Y-%m-%d %H:%M:%S%.f", 
                    strict=False
                )
                .alias("timestamp")
            )
            .filter(
                pl.col("coordinate").str.count_matches(",") == 1
            )
            .with_columns(
                pl.col("coordinate")
                .str.split_exact(",", 1)
                .struct.field("field_0")
                .cast(pl.Int64)
                .alias("x"),
                pl.col("coordinate")
                .str.split_exact(",", 1)
                .struct.field("field_1")
                .cast(pl.Int64)
                .alias("y"),
            )
            .drop("coordinate")
        )

        ids = df["user_id"].to_list()
        users = []
        for id in ids:
            if id not in hash:
                hash[id] = count
                count += 1
            users.append(hash[id])
        df = df.with_columns(pl.Series("user_id", users))


        table = df.to_arrow()
        if parquet_writer is None:
            parquet_writer = pq.ParquetWriter(
                parquet_file, 
                schema=table.schema, 
                compression="zstd"
            )
        parquet_writer.write_table(table)

finally:
    if parquet_writer:
        parquet_writer.close()

print(f"Successfully processed and saved to {parquet_file}")
