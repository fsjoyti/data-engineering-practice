import os
import pathlib

import polars as pl


def main():
    current_directory = os.getcwd()
    final_directory = os.path.join(current_directory, r"data")
    path: pathlib.Path = final_directory + "\\" + "202306-divvy-tripdata.csv"
    print(path)
    schema_overrides = {"start_station_id": pl.String, "end_station_id": pl.String}

    trips = (
        pl.scan_csv(
            path,
            has_header=True,
            try_parse_dates=True,
            schema_overrides=schema_overrides,
        )
        .with_columns(duration=(pl.col("ended_at") - pl.col("started_at")))
        .select(
            datetime_start=pl.col("started_at"),
        )
        .with_columns(date=pl.col("datetime_start").dt.date())
    )

    trips_per_day = trips.group_by("date").agg(num_trips=pl.len()).sort("date")

    print(trips_per_day.collect())


if __name__ == "__main__":
    main()
