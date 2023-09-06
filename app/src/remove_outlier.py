"""
外れ値の除外
calc_delayの後に、Null値がないかどうかをSQLで確認してからにして下さい。
"""
import pandas as pd

from app.utils.db import get_db_adapter
from app.utils.logger import getLogger

logger = getLogger(__name__)
db_adapter = get_db_adapter()

with db_adapter.engine.connect() as con:
    delay_df = pd.read_sql("select * from delay", con=con)

if len(delay_df) == 0:
    exit()

logger.info(f"データ総数: {len(delay_df)}")

# Null値の除外
delay_df.dropna(axis="index", subset=["delay"], inplace=True)

# 最後の停留所を除外
delay_df: pd.DataFrame = delay_df.groupby(["trip_id", "agency"], as_index=False).apply(
    lambda x: x[x["stop_sequence"] < x["stop_sequence"].max()]
)
# 最初の停留所を除外
delay_df.query("stop_sequence != 0", inplace=True)

# 極端な値（1時間以上の遅延、早着）を除外
delay_df.query("delay >= -3600 & delay <= 3600", inplace=True)

logger.info(f"外れ値除外後のデータ総数: {len(delay_df)}")

# テーブルのデータを全て消去
db_adapter.exec_query("truncate delay")

with db_adapter.engine.connect() as con:
    delay_df.to_sql(
        name="delay",
        con=con,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=100000,
    )

    con.commit()

db_adapter.close()
