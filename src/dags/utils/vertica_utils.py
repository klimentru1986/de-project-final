import vertica_python
from airflow.hooks.base import BaseHook


def try_execute(SQL):
    conn = BaseHook.get_connection("vertica")

    conn_info = {
        "host": conn.host,
        "port": conn.port,
        "user": conn.login,
        "password": conn.password,
        "database": conn.schema,
        "autocommit": True,
    }

    # И рекомендуем использовать соединение вот так
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(SQL)
        res = cur.fetchall()
        return res
