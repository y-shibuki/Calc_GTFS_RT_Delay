import warnings
from collections import defaultdict

import pandas as pd

from app.utils.db import get_db_adapter
from app.utils.logger import getLogger

logger = getLogger(__name__)
db_adapter = get_db_adapter()

warnings.simplefilter("ignore")

agencies = ["関東自動車", "富山地鉄バス", "富山地鉄市内電車"]
# agencies = ["関東自動車"]
# delay_utsunomiya_east _宇都宮駅東口を含むルート
delay_df = defaultdict(pd.DataFrame)

for agency in agencies:
    with db_adapter.engine.connect() as con:
        delay_df[agency] = pd.read_sql(
            """
            select *
            from delay_utsunomiya_east
            where
                agency = '{agency}'
                and
                date < '2023-08-26'
            """.format(
                agency=agency
            ),
            con=con,
        )

res = []
for i in range(0, 2000, 100):
    temp = [f"{i} <= x < {i + 100}", i]
    for agency in agencies:
        temp.append(len(delay_df[agency].query(f"{i} <= delay < {i + 100}")))
    res.append(temp)

df = pd.DataFrame(res, columns=["階級", "下限"] + agencies)

for agency in agencies:
    df[f"{agency}_標準化"] = df[agency] / df[agency].sum()

df.to_csv(
    "./data/度数分布表/LRT導入前の遅延時間_宇都宮駅東口を含むルート.csv", index=False, encoding="utf-8-sig"
)

db_adapter.close()
