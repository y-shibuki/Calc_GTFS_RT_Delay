import glob
from datetime import datetime

import pandas as pd

from app.utils.db import get_db_adapter

db_adapter = get_db_adapter()

folder_path = "/Volumes/SSD/GTFS_DATA/gtfs_auto_downloader"

# テーブルのデータを全て消去
db_adapter.exec_query(
    """
        truncate delay
    """
)

for agency in ["関東自動車", "富山地鉄バス", "富山地鉄市内電車"]:
    for path in glob.glob(f"{folder_path}/gtfs/{agency}/*"):
        print(path)
    break
    stop_times_df = pd.read_csv(f"./data/GTFS/{agency}/stop_times.txt")[
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
    stop_times_df["start_date"] = datetime.now()
    stop_times_df["agency"] = agency

    with db_adapter.engine.connect() as con:
        stop_times_df.to_sql(
            name="gtfs_stop_times",
            con=con,
            if_exists="append",
            index=False,
            method="multi",
        )
        con.commit()

db_adapter.close()
