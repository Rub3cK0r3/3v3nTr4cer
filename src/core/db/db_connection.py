import psycopg2
from psycopg2.extensions import connection, cursor
import os
import time
from urllib.parse import quote_plus
from loguru import logger

class DBConnection:
    conn: connection
    cur: cursor

    def __init__(self, channel: str = "event_channel") -> None:
        self.channel = channel
        self.conn, self.cur = self._connect_with_retry()

    def _create_connection(self) -> connection:
        conn = psycopg2.connect(
            host=os.environ.get("DB_HOST", "db"), # change to db for docker
            database=os.environ.get("POSTGRES_DB", "eventdb"),
            user=os.environ.get("POSTGRES_USER", "devuser"),
            password=quote_plus(os.environ.get("POSTGRES_PASSWORD", "devpass")),
        )
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        return conn

    def _listen(self, conn: connection) -> cursor:
        cur = conn.cursor()
        cur.execute(f"LISTEN {self.channel};")
        return cur

    def _connect_with_retry(self) -> tuple[connection, cursor]:
        while True:
            try:
                conn = self._create_connection()
                cur = self._listen(conn)
                return conn, cur
            except Exception as e:
                logger.bind(pipeline_stage="database").error("Database connection failed: {}", e)
                time.sleep(5)

    def reconnect(self) -> None:
        self.conn, self.cur = self._connect_with_retry()

    def close(self) -> None:
        try:
            if hasattr(self, 'cur') and self.cur is not None:
                self.cur.close()
            if hasattr(self, 'conn') and self.conn is not None:
                self.conn.close()
            logger.bind(pipeline_stage="database").info("Database connection closed")
        except Exception as e:
            logger.bind(pipeline_stage="database").error("Database connection close failed: {}", e)

if __name__ == "__main__":
    db = DBConnection()
    logger.bind(pipeline_stage="database").info("Database connection established")
    db.cur.execute("SELECT version();")
    version = db.cur.fetchone()
    logger.bind(pipeline_stage="database").info("PostgreSQL version: {}", version)

