import pandas as pd

from app.utils.db import get_db_adapter
from app.utils.logger import getLogger

logger = getLogger(__name__)
db_adapter = get_db_adapter()


def f(x: pd.DataFrame):
    if len(x.drop_duplicates()) == 1:
        return
    else:
        logger.info(x)


with db_adapter.engine.connect() as con:
    df = pd.read_sql("select * from gtfs_stops", con=con)

del df["start_date"]
df.groupby(by=["stop_id", "agency"]).apply(f)
