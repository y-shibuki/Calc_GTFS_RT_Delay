import glob
import gzip
import itertools
import json
import os
import shutil
import tarfile
from datetime import datetime
from typing import Any, Dict, List, Set

import pandas as pd

from app.utils.db import get_db_adapter

db_adapter = get_db_adapter()

folder_path = "/Volumes/SSD/GTFS_DATA/gtfs_auto_downloader"

# IDが一意かどうか。
# visited_tripidには、TripIDがいつ読み込まれたかが格納されていく。
# 例外を防ぐためです。詳細はreadmeを読んでください。
visited_tripid = dict()
n = 0


def load_gtfs_data(file_path: str) -> list:
    """GTFS-RTデータの読み込み

    Args:
        file_path (str): GTFS-RTのファイルパス

    Returns:
        list: GTFS-RTデータ
    """
    with open(file_path, "r") as f:
        d = json.load(f)
        if "entity" in d:
            print(type(d))
            return d["entity"]
        else:
            return []


# JSON形式から表形式への変換
def convert_to_table(trip: Dict[str]) -> List[Any]:
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

    # 今後、停車予定の停留所が無い場合は採用しない
    if "stopTimeUpdate" not in trip["tripUpdate"]:
        return []

    return [
        [
            tripId,
            datetime.fromtimestamp(int(stop["arrival"]["time"])),
            stop["stopSequence"],
        ]
        for stop in trip["tripUpdate"]["stopTimeUpdate"]
    ]


def get_date(crawl_type: str = "append") -> Set[str]:
    """収集する日付を返す

    Args:
        crawl_type (str, optional): "all"なら全ての圧縮ファイルを収集する。"append"ならSQLデータベースに登録されていない日付のみを収集する。デフォルトは"append"。

    Raises:
        Exception: crawl_typeに不正値が代入された場合。

    Returns:
        Set[str]: 収集する日付
    """
    # サーバーで収集された日付
    crawled_date_set = {
        os.path.basename(x).split(".")[0] for x in glob.glob(f"{folder_path}/zip/*")
    }

    if crawl_type == "all":
        return crawled_date_set
    elif crawl_type == "append":
        # データベースに登録済の日付
        appended_date_set = {
            x[0].strftime("%Y年%m月%d日")
            for x in db_adapter.query_data("select distinct date from gtfs_rt")
        }
        return crawled_date_set - appended_date_set

    raise Exception


for date in get_date("append"):
    try:
        with tarfile.open(f"{folder_path}/zip/{date}.tar.gz", "r:gz") as tar:
            tar.extractall(path=folder_path)
    except gzip.BadGzipFile as e:
        print(e, "gzファイルが破損しています。")

    for agency in ["関東自動車", "富山地鉄バス", "富山地鉄市内電車"]:
        print(agency, date)

        temp_stop_times_list = []
        visited_tripid = dict()
        n = 0
        # GTFSデータを時刻順に読み込み、整形する
        for path in sorted(
            glob.glob(f"{folder_path}/data/{agency}/TripUpdate/{date}/*.json")
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
        stop_times_df = (
            pd.DataFrame(
                temp_stop_times_list,
                columns=["trip_id", "actual_arrival_time", "stop_sequence"],
            )
            .groupby(by=["trip_id", "stop_sequence"], as_index=False)
            .last()
        )

        stop_times_df["date"] = datetime.strptime(date, "%Y年%m月%d日")
        stop_times_df["agency"] = agency

        with db_adapter.engine.connect() as con:
            stop_times_df.to_sql(
                name="gtfs_rt", con=con, if_exists="append", index=False, method="multi"
            )
            con.commit()

    # 解凍したデータを一括削除
    # 解凍したままだと容量を圧迫するため
    shutil.rmtree(f"{folder_path}/date")

db_adapter.close()
