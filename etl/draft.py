import duckdb
import pandas as pd
import numpy as np
import requests
from datetime import datetime
import pytz

DB_PATH = "data.db"

def fetch_draft():
    url = "https://draft.premierleague.com/api/league/49439/details"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    df = pd.DataFrame(r.json()["standings"])

    # add metadata
    update = datetime.now(pytz.UTC).replace(microsecond=0).isoformat()
    df["update"] = update

    # add progress + best gw
    df["progress"] = np.where(df["rank_sort"] > df["last_rank"], "green",
                     np.where(df["rank_sort"] < df["last_rank"], "red", ""))
    max_value = df["event_total"].max()
    df["best_gw"] = np.where(df["event_total"] == max_value, "best", "")

    return df

def store_draft(df: pd.DataFrame):
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE TABLE IF NOT EXISTS draft_standings AS SELECT * FROM df LIMIT 0")
    con.register("df", df)
    con.execute("INSERT INTO draft_standings SELECT * FROM df")
    con.unregister("df")
    con.close()

def main():
    df = fetch_draft()
    store_draft(df)

if __name__ == "__main__":
    main()
