import glob
import itertools
import json
from datetime import datetime

import pandas as pd

from app.utils.db import get_db_adapter

db_adapter = get_db_adapter()

# IDが一意かどうか。
# visited_tripidには、TripIDがいつ読み込まれたかが格納されていく。
# 例外を防ぐためです。詳細はreadmeを読んでください。
visited_tripid = dict()
n = 0


# GTFS-RTデータの読み込み
def load_gtfs_data(file_path) -> list:
    with open(file_path, "r") as f:
        d = json.load(f)
        if "entity" in d:
            return d["entity"]
        else:
            return []


# JSON形式から表形式への変換
def convert_to_table(trip) -> list:
    global visited_tripid, n
    # 臨時便などは除き、時刻表に記載されているもののみを採用
    if trip["tripUpdate"]["trip"]["scheduleRelationship"] != "SCHEDULED":
        return []

    try:
        tripId = trip["tripUpdate"]["trip"]["tripId"]
    except KeyError:
        print("tripIdが存在しません。")
        return []

    # 前回tripIDが記録されたのが、直近(n-1）ではない→例外判定
    if tripId in visited_tripid and visited_tripid[tripId] != (n - 1):
        return []

    visited_tripid[tripId] = n

    res = []

    # 今後、停車予定の停留所が無い場合は採用しない
    if "stopTimeUpdate" not in trip["tripUpdate"]:
        return []

    for stop in trip["tripUpdate"]["stopTimeUpdate"]:
        date = datetime.fromtimestamp(int(stop["arrival"]["time"]))
        res.append([tripId, date, stop["stopSequence"]])
    return res


for agency in ["関東自動車", "富山地鉄バス", "富山地鉄市内電車"]:
    for date in [
        folder.split("/")[-2]
        for folder in glob.glob(f"./data/GTFS-RT/{agency}/TripUpdate/*/")
    ]:
        print(agency, date)

        temp_stop_times_list = []
        visited_tripid = dict()
        n = 0
        # GTFSデータを時刻順に読み込み、整形する
        for path in sorted(
            glob.glob(f"./data/GTFS-RT/{agency}/TripUpdate/{date}/*.json")
        ):
            # GTFSデータの読み込み
            d = load_gtfs_data(path)
            # 辞書型からリストへの変換
            temp_stop_times_list += list(
                itertools.chain.from_iterable(map(convert_to_table, d))
            )

            n += 1

        # DataFrame型への変換
        # 最後に記録された、「停留所への到着時刻」を実際の到着時刻とみなす
        stop_times_df = pd.DataFrame(
            temp_stop_times_list,
            columns=["trip_id", "actual_arrival_time", "stop_sequence"],
        ).groupby(
            by=["trip_id", "stop_sequence"], as_index=False
        ).last()

        stop_times_df["date"] = datetime.strptime(date, "%Y年%m月%d日")
        stop_times_df["agency"] = agency

        with db_adapter.engine.connect() as con:
            stop_times_df.to_sql(
                name="gtfs_rt",
                con=con,
                if_exists="append",
                index=False,
                method="multi"
            )
            con.commit()

db_adapter.close()
