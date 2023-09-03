import os
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, scoped_session, sessionmaker

Base: Any = declarative_base()

# 環境変数の読み込み
load_dotenv("./.env.local")


class DBAdapter:
    def __init__(self, url: str) -> None:
        self.__engine = create_engine(url)
        self.__session = scoped_session(
            sessionmaker(autocommit=False, autoflush=False, bind=self.__engine)
        )

    def close(self) -> None:
        self.__session.close()
        self.__engine.dispose()

    @property
    def session(self) -> Session:
        return self.__session

    @property
    def engine(self) -> Engine:
        return self.__engine

    def query_data(self, query: str) -> list[Any]:
        with self.engine.connect() as con:
            rs = con.execute(text(query))
        return [dt for dt in rs]

    def exec_query(self, query: str) -> None:
        with self.engine.connect() as con:
            con.execute(text(query))
            con.commit()


def get_db_adapter() -> DBAdapter:
    MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE")
    MYSQL_USER = os.environ.get("MYSQL_USER")
    MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD")
    MYSQL_PORT = os.environ.get("MYSQL_PORT")

    return DBAdapter(
        f"mysql://{MYSQL_USER}:{MYSQL_PASSWORD}@127.0.0.1:{MYSQL_PORT}/{MYSQL_DATABASE}"
    )
