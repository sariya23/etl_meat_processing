from clickhouse_connect import get_client


cl = get_client(host="localhost", username="admin", password="qwerty")
cl.command("create table t (a String) ENGINE MergeTree order by a")