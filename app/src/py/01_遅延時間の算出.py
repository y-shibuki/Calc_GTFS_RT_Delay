import pandas as pd

from app.utils.db import get_db_adapter

db_adapter = get_db_adapter()


# 時刻表が更新される際に、StopSequenceがズレることがあるので、その一時的対応
def calc_delay(k):
    try:
        return (k["actual_arrival_time"] - k["arrival_time"]).total_seconds()
    except TypeError as e:
        print(k, e)
    return None


# テーブルのデータを全て消去
db_adapter.exec_query(
    """
        truncate delay
    """
)

with db_adapter.engine.connect() as con:
    stop_times_df = pd.read_sql(
        """
            select
                trip_id,
                stop_id,
                stop_sequence,
                arrival_time,
                agency
            from gtfs_stop_times
        """,
        con=con,
    )
    gtfs_rt_df = pd.read_sql(
        """
            select
                date,
                trip_id,
                stop_sequence,
                actual_arrival_time,
                agency
            from gtfs_rt
            where
                date >= '2023-08-28'
        """,
        con=con,
    )

    # GTFSデータとGTFS_RTデータを結合
    merged_stop_times = gtfs_rt_df.merge(
        stop_times_df, how="left", on=["trip_id", "stop_sequence", "agency"]
    )  # .merge(trip_df, how="left", on=["trip_id"])

    merged_stop_times["arrival_time"] = pd.to_datetime(
        merged_stop_times["date"].astype(str)
        + " "
        + merged_stop_times["arrival_time"].dt.strftime("%H:%M:%S")
    )

    merged_stop_times.dropna(axis="index", subset=["stop_id"], inplace=True)

    # 遅延時間を算出
    merged_stop_times["delay"] = merged_stop_times.apply(calc_delay, axis=1)

    merged_stop_times[
        [
            "date",
            "trip_id",
            "stop_id",
            "stop_sequence",
            "arrival_time",
            "actual_arrival_time",
            "delay",
            "agency",
        ]
    ].to_sql(name="delay", con=con, if_exists="append", index=False, method="multi")

    con.commit()

db_adapter.close()
