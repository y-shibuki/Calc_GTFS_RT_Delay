import glob
from datetime import datetime

import pandas as pd

from app.utils.db import get_db_adapter
from app.utils.logger import getLogger

logger = getLogger(__name__)
db_adapter = get_db_adapter()

folder_path = "/Volumes/SSD/GTFS_DATA/"

# テーブルのデータを全て消去
db_adapter.exec_query("truncate gtfs_stop_times")
db_adapter.exec_query("truncate gtfs_stops")

for agency in ["関東自動車", "富山地鉄バス", "富山地鉄市内電車"]:
    for path in glob.glob(f"{folder_path}/gtfs/{agency}/*"):
        start_date = path.split("/")[-1]

        logger.info(f"{agency}: {start_date}")

        # stop_timesの読み込み
        stop_times_df = pd.read_csv(f"{path}/stop_times.txt")[
            [
                "trip_id",
                "arrival_time",
                "departure_time",
                "stop_id",
                "stop_sequence",
                "stop_headsign",
            ]
        ]

        stop_times_df["arrival_time"] = pd.to_datetime(
            stop_times_df["arrival_time"], format="%H:%M:%S"
        )
        stop_times_df["departure_time"] = pd.to_datetime(
            stop_times_df["departure_time"], format="%H:%M:%S"
        )
        stop_times_df["start_date"] = datetime.strptime(start_date, "%Y-%m-%d").date()
        stop_times_df["agency"] = agency

        with db_adapter.engine.connect() as con:
            stop_times_df.to_sql(
                name="gtfs_stop_times",
                con=con,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=100000
            )
            con.commit()

        # stopsの読み込み
        stops_df = pd.read_csv(f"{path}/stops.txt")[
            [
                "stop_id",
                "stop_name",
                "stop_lat",
                "stop_lon",
                "location_type",
                "parent_station"
            ]
        ]

        stops_df["agency"] = agency

        with db_adapter.engine.connect() as con:
            stops_df.to_sql(
                name="gtfs_stops",
                con=con,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=100000
            )
            con.commit()

db_adapter.close()
