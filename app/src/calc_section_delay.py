from datetime import datetime

import pandas as pd

from app.utils.db import get_db_adapter
from app.utils.logger import getLogger

logger = getLogger(__name__)
db_adapter = get_db_adapter()

# テーブルのデータを全て消去
db_adapter.exec_query("truncate section_delay")

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
            section_time_df = pd.read_sql(
                """
                    select
                        trip_id,
                        O_id,
                        D_id,
                        section_time
                    from gtfs_section_time
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
                        rt.date,
                        rt.trip_id,
                        st.stop_id,
                        rt.stop_sequence,
                        rt.actual_arrival_time,
                        rt.agency
                    from gtfs_rt rt
                    inner join gtfs_stop_times st
                        on
                            rt.trip_id = st.trip_id and
                            rt.stop_sequence = st.stop_sequence and
                            st.agency = '{agency}' and
                            st.start_date = '{start_date}'
                    where
                        rt.agency = '{agency}' and
                        rt.date >= '{start_date}' and
                        rt.date < '{end_date}'
                    order by rt.stop_sequence ASC
                """.format(
                    agency=agency, start_date=start_date[i], end_date=start_date[i + 1]
                ),
                con=con,
            )

            if len(gtfs_rt_df) == 0:
                continue

        # 実区間所要時間を計算
        res = []
        for (trip_id, date), g in gtfs_rt_df.groupby(by=["trip_id", "date"]):
            O_id = None
            departure_time = None
            for _, x in g.iterrows():
                if O_id is not None:
                    arrival_time = x["actual_arrival_time"]
                    res.append(
                        [
                            trip_id,
                            O_id,
                            x["stop_id"],
                            (arrival_time - departure_time).total_seconds(),
                            agency,
                            date,
                        ]
                    )
                O_id = x["stop_id"]
                departure_time = x["actual_arrival_time"]

        actual_section_time_df = pd.DataFrame(
            res,
            columns=[
                "trip_id",
                "O_id",
                "D_id",
                "actual_section_time",
                "agency",
                "date",
            ],
        )

        # GTFSデータとGTFS_RTデータを結合
        merged_duration_time_df = actual_section_time_df.merge(
            section_time_df, how="left", on=["trip_id", "O_id", "D_id"]
        )

        merged_duration_time_df["delay"] = (
            merged_duration_time_df["actual_section_time"]
            - merged_duration_time_df["section_time"]
        )

        with db_adapter.engine.connect() as con:
            merged_duration_time_df[
                ["trip_id", "O_id", "D_id", "delay", "agency", "date"]
            ].to_sql(
                name="section_delay",
                con=con,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=100000,
            )
            con.commit()

db_adapter.close()
