import glob
import os
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

from app.utils.db import get_db_adapter
from app.utils.logger import getLogger

logger = getLogger(__name__)
db_adapter = get_db_adapter()

# 環境変数の読み込み
load_dotenv("./.env.local")

folder_path = os.getenv("FOLDER_PATH")

# テーブルのデータを全て消去
db_adapter.exec_query(
    """
    DROP TABLE IF EXISTS gtfs_stop_times, gtfs_stops, gtfs_section_time;
    """
)

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
                chunksize=100000,
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
                "parent_station",
            ]
        ]

        stops_df["start_date"] = datetime.strptime(start_date, "%Y-%m-%d").date()
        stops_df["agency"] = agency

        with db_adapter.engine.connect() as con:
            stops_df.to_sql(
                name="gtfs_stops",
                con=con,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=100000,
            )
            con.commit()

        # 区間所要時間を計算
        res = []
        for trip_id, g in stop_times_df.groupby("trip_id"):
            O_id = None
            departure_time = None
            for _, x in g.iterrows():
                if O_id is not None:
                    arrival_time = x["arrival_time"]
                    res.append(
                        [
                            trip_id,
                            O_id,
                            x["stop_id"],
                            (arrival_time - departure_time).total_seconds(),
                            agency,
                            start_date,
                        ]
                    )
                O_id = x["stop_id"]
                departure_time = x["departure_time"]

        with db_adapter.engine.connect() as con:
            pd.DataFrame(
                res, columns=["trip_id", "O_id", "D_id", "section_time", "agency", "start_date"]
            ).to_sql(
                name="gtfs_section_time",
                con=con,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=100000,
            )
            con.commit()

db_adapter.close()
