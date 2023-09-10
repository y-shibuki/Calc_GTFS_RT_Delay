import pandas as pd

from app.utils.db import get_db_adapter
from app.utils.logger import getLogger

logger = getLogger(__name__)
db_adapter = get_db_adapter()

# テーブルのデータを全て消去
db_adapter.exec_query("truncate delay_utsunomiya_east")

with db_adapter.engine.connect() as con:
    delay_df = pd.read_sql("select * from delay where agency='関東自動車'", con=con)

# 宇都宮駅東口(S0904000148)を含むtrip_idのリスト
trip_id_list = [
    x[0]
    for x in db_adapter.query_data(
        """
            select distinct trip_id
            from gtfs_stop_times
            where agency='関東自動車' and stop_id like 'S0904000148%'
        """
    )
]
logger.info(trip_id_list)
with db_adapter.engine.connect() as con:
    delay_df.query("trip_id in @trip_id_list").to_sql(
        name="delay_utsunomiya_east",
        con=con,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=10000,
    )
