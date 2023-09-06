from datetime import datetime

import pandas as pd

from app.utils.db import get_db_adapter
from app.utils.logger import getLogger

logger = getLogger(__name__)
db_adapter = get_db_adapter()


# 時刻表が更新される際に、StopSequenceがズレることがあるので、その一時的対応
def calc_delay(k):
    try:
        return (k["actual_arrival_time"] - k["arrival_time"]).total_seconds()
    except TypeError as e:
        print(k, e)
    return None


# テーブルのデータを全て消去
db_adapter.exec_query("truncate delay")

for agency in ["関東自動車", "富山地鉄バス", "富山地鉄市内電車"]:
    start_date = sorted(
        [
            x[0]
            for x in db_adapter.query_data(
                """
            select distinct start_date
            from gtfs_stop_times
            where agency = '{agency}'
        """.format(
                    agency=agency
                )
            )
        ]
        + [datetime.now().date()]
    )

    for i in range(len(start_date) - 1):
        logger.info(f"{agency}: {start_date[i]} <= date < {start_date[i + 1]}")
        with db_adapter.engine.connect() as con:
            stop_times_df = pd.read_sql(
                """
                    select
                        trip_id,
                        stop_id,
                        stop_sequence,
                        arrival_time
                    from gtfs_stop_times
                    where
                        agency = '{agency}' and
                        start_date = '{start_date}'
                """.format(
                    agency=agency, start_date=start_date[i]
                ),
                con=con,
            )
            gtfs_rt_df = pd.read_sql(
                """
                    select
                        date,
                        trip_id,
                        stop_sequence,
                        actual_arrival_time
                    from gtfs_rt
                    where
                        agency = '{agency}' and
                        date >= '{start_date}' and
                        date < '{end_date}'
                """.format(
                    agency=agency, start_date=start_date[i], end_date=start_date[i + 1]
                ),
                con=con,
            )

            if len(gtfs_rt_df) == 0:
                continue

            # GTFSデータとGTFS_RTデータを結合
            merged_stop_times = gtfs_rt_df.merge(
                stop_times_df, how="left", on=["trip_id", "stop_sequence"]
            )  # .merge(trip_df, how="left", on=["trip_id"])

            merged_stop_times["arrival_time"] = pd.to_datetime(
                merged_stop_times["date"].astype(str)
                + " "
                + merged_stop_times["arrival_time"].dt.strftime("%H:%M:%S")
            )

            # merged_stop_times.dropna(axis="index", subset=["stop_id"], inplace=True)

            # 遅延時間を算出
            merged_stop_times["delay"] = merged_stop_times.apply(calc_delay, axis=1)

            # * 外れ値の除外 *
            # 最後の停留所を除外
            delay_df: pd.DataFrame = merged_stop_times.groupby(
                "trip_id", as_index=False
            ).apply(lambda x: x[x["stop_sequence"] < x["stop_sequence"].max()])
            # 最初の停留所を除外
            # delay_df.query("stop_sequence != 0", inplace=True)

            # 極端な値（1時間以上の遅延、早着）を除外
            # delay.query("delay >= -3600 & delay <= 3600", inplace=True)

            delay_df["agency"] = agency

            delay_df[
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
            ].to_sql(
                name="delay",
                con=con,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=5000,
            )

            con.commit()

db_adapter.close()
