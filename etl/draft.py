import duckdb
import pandas as pd
import numpy as np
import requests
from datetime import datetime
import pytz

DB_PATH = "data.db"
LEAGUE_ID = 35582


def fetch_draft():
    """
    Function to fetch data from draft fpl api
    """
    url = f"https://draft.premierleague.com/api/league/{LEAGUE_ID}/details"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    league_data_json = r.json()
    df_league_standings = pd.DataFrame(league_data_json["standings"])

    # Extract league entries metadata
    df_entries = pd.DataFrame(league_data_json["league_entries"])

    # Keep only necessary columns and rename 'id' to 'league_entry'
    df_entries = df_entries[
        ["id", "entry_name", "player_first_name", "player_last_name"]
    ]
    df_entries = df_entries.rename(columns={"id": "league_entry"})

    # Combine first + last name into one column
    df_entries["player_name"] = (
        df_entries["player_first_name"].fillna("")
        + " "
        + df_entries["player_last_name"].fillna("")
    )
    df_entries = df_entries.drop(columns=["player_first_name", "player_last_name"])

    # Merge into standings
    df_league_standings = df_league_standings.merge(
        df_entries, on="league_entry", how="left"
    )

    # add metadata
    update = datetime.now(pytz.UTC).replace(microsecond=0).isoformat()
    df_league_standings["update"] = update

    # add progress + best gw
    max_value = df_league_standings["event_total"].max()
    df_league_standings["best_gw"] = np.where(
        df_league_standings["event_total"] == max_value, "best", ""
    )
    df_league_standings["progress"] = df_league_standings.apply(get_progress, axis=1)

    return df_league_standings


def store_draft(df: pd.DataFrame):
    """
    Function to store draft standings in database
    """
    con = duckdb.connect(DB_PATH)
    con.execute(
        "CREATE TABLE IF NOT EXISTS draft_standings AS SELECT * FROM df LIMIT 0"
    )
    con.register("df", df)
    con.execute("INSERT INTO draft_standings SELECT * FROM df")
    con.unregister("df")
    con.close()


def get_progress(row: pd.Series) -> str:
    """
    Function to generate progress data per gameweek
    """
    if pd.isna(row["last_rank"]):
        return ""
    elif row["rank"] < row["last_rank"]:
        return "green"
    elif row["rank"] > row["last_rank"]:
        return "red"
    else:
        return ""


def assign_gameweeks(df_updates: pd.DataFrame) -> pd.DataFrame:
    """
    Assigns gameweek labels to df_updates:
      - Each league_entry gets at most one GW per gameweek (the latest update).
      - Older updates in the same GW stay blank.
      - Previous GWs are retained.
    """
    # Ensure datetime
    df_updates["update"] = pd.to_datetime(df_updates["update"])

    # --- Get Gameweek Data ---
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    data = requests.get(url).json()

    events_df = pd.DataFrame(data["events"])

    events_df["deadline_time"] = pd.to_datetime(events_df["deadline_time"], utc=True)

    events_df["next_deadline_time"] = events_df["deadline_time"].shift(-1)

    events_df["end_time"] = events_df["next_deadline_time"] - pd.Timedelta(seconds=1)

    events_df["gameweek"] = "GW" + events_df["id"].astype(str)

    # --- Map update timestamp → gameweek ---
    def get_gw(ts):
        row = events_df[
            (events_df["deadline_time"] <= ts)
            & (events_df["end_time"].isna() | (events_df["end_time"] >= ts))
        ]
        return row.iloc[0]["gameweek"] if not row.empty else None

    df_updates["gw_temp"] = df_updates["update"].apply(get_gw)

    # --- Keep only latest update per (league_entry, gw_temp) ---
    # Find the latest update timestamp per league_entry+gameweek
    latest_per_gw = df_updates.groupby(["league_entry", "gw_temp"])["update"].transform(
        "max"
    )

    # Assign gameweek only if this row is the latest for that GW
    df_updates["gameweek"] = df_updates.apply(
        lambda row: row["gw_temp"] if row["update"] == latest_per_gw[row.name] else "",
        axis=1,
    )

    # Drop helper column
    df_updates = df_updates.drop(columns=["gw_temp"])

    return df_updates


def update_draft_standings_gw():
    """
    Incrementally updates the draft_standings_gw table in DuckDB
    with gameweek info, avoiding duplicates.
    """

    # Connect to DuckDB
    con = duckdb.connect("data.db")

    # 1️⃣ Ensure the enriched table exists
    con.execute(
        """
        DROP TABLE IF EXISTS draft_standings_gw;

        CREATE TABLE draft_standings_gw AS
        SELECT *, ''::VARCHAR AS gameweek
        FROM draft_standings
        LIMIT 0
        """
    )

    # 2️⃣ Find latest update already in GW table
    latest_gw_update = con.execute(
        "SELECT MAX(update) FROM draft_standings_gw"
    ).fetchone()[0]

    if latest_gw_update is None:
        latest_gw_update = "1970-01-01T00:00:00+00:00"  # no data yet

    # 3️⃣ Query only new rows
    df_new = con.execute(
        f"""
        SELECT * FROM draft_standings
        WHERE update > '{latest_gw_update}'
        ORDER BY update
    """
    ).df()

    if df_new.empty:
        print("No new rows to update.")
        con.close()
        return

    # 4️⃣ Assign gameweek
    df_new = assign_gameweeks(df_new)  # our previous function

    # 5️⃣ Keep only rows with gameweek assigned
    df_new = df_new[df_new["gameweek"] != ""]

    if df_new.empty:
        print("No new rows have a gameweek to assign.")
        con.close()
        return

    # 6️⃣ Deduplicate: keep only one row per league_entry + gameweek
    df_new = df_new.sort_values("update", ascending=False)
    df_new = df_new.drop_duplicates(subset=["league_entry", "gameweek"], keep="first")

    # 7️⃣ Insert into enriched table
    con.register("df_new_temp", df_new)
    con.execute("INSERT INTO draft_standings_gw SELECT * FROM df_new_temp")

    print(f"Inserted {len(df_new)} new rows with gameweek info.")
    con.close()


def main():
    # daily data update
    df = fetch_draft()
    store_draft(df)

    # assign gameweek
    update_draft_standings_gw()


if __name__ == "__main__":
    main()
