import warnings
from collections import defaultdict

import pandas as pd

from app.utils.db import get_db_adapter
from app.utils.logger import getLogger

logger = getLogger(__name__)
db_adapter = get_db_adapter()

warnings.simplefilter("ignore")

agencies = ["関東自動車", "富山地鉄バス", "富山地鉄市内電車"]
delay_df = defaultdict(pd.DataFrame)

for agency in agencies:
    with db_adapter.engine.connect() as con:
        delay_df[agency] = pd.read_sql(
            """
            select *
            from delay
            where
                agency = '{agency}'
            """.format(agency=agency),
            con=con,
            chunksize=100000,
        )


res = []
for i in range(0, 2000, 100):
    temp = [f"{i} <= x < {i + 100}", i]
    for agency in agencies:
        temp.append(len(delay_df[agency].query(f"{i} <= delay < {i + 100}")))
    res.append(temp)

pd.DataFrame(res, columns=["階級", "下限"] + agencies).to_csv(
    "./data/LRT導入後の遅延時間.csv", index=False, encoding="utf-8-sig"
)

db_adapter.close()
