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
    """)