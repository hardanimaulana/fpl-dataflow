import duckdb
import pandas as pd
import numpy as np
import requests
from datetime import datetime
import pytz
from sqlalchemy import null

DB_PATH = "data.db"


def fetch_draft():
    """
    Function to fetch data from draft fpl api
    """
    url = "https://draft.premierleague.com/api/league/49439/details"
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


def add_initial_data():
    """
    Data to add manually until the daily crawl works properly
    """

    # Extract league entries metadata
    url = "https://draft.premierleague.com/api/league/49439/details"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    league_data_json = r.json()

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

    gw1_temp = {
        "standings": [
            {
                "event_total": 76,
                "last_rank": null,
                "league_entry": 253947,
                "rank": 1,
                "rank_sort": 1,
                "total": 76,
            },
            {
                "event_total": 51,
                "last_rank": null,
                "league_entry": 253943,
                "rank": 2,
                "rank_sort": 2,
                "total": 51,
            },
            {
                "event_total": 49,
                "last_rank": null,
                "league_entry": 253945,
                "rank": 3,
                "rank_sort": 3,
                "total": 49,
            },
            {
                "event_total": 48,
                "last_rank": null,
                "league_entry": 253940,
                "rank": 4,
                "rank_sort": 4,
                "total": 48,
            },
            {
                "event_total": 43,
                "last_rank": null,
                "league_entry": 253949,
                "rank": 5,
                "rank_sort": 5,
                "total": 43,
            },
            {
                "event_total": 40,
                "last_rank": null,
                "league_entry": 253948,
                "rank": 6,
                "rank_sort": 6,
                "total": 40,
            },
            {
                "event_total": 39,
                "last_rank": null,
                "league_entry": 253941,
                "rank": 7,
                "rank_sort": 7,
                "total": 39,
            },
            {
                "event_total": 38,
                "last_rank": null,
                "league_entry": 253944,
                "rank": 8,
                "rank_sort": 8,
                "total": 38,
            },
            {
                "event_total": 33,
                "last_rank": null,
                "league_entry": 253946,
                "rank": 9,
                "rank_sort": 9,
                "total": 33,
            },
            {
                "event_total": 29,
                "last_rank": null,
                "league_entry": 253939,
                "rank": 10,
                "rank_sort": 10,
                "total": 29,
            },
            {
                "event_total": 27,
                "last_rank": null,
                "league_entry": 253942,
                "rank": 11,
                "rank_sort": 11,
                "total": 27,
            },
        ]
    }
    gw2_temp = {
        "standings": [
            {
                "event_total": 66,
                "last_rank": 3,
                "league_entry": 253945,
                "rank": 1,
                "rank_sort": 1,
                "total": 115,
            },
            {
                "event_total": 38,
                "last_rank": 1,
                "league_entry": 253947,
                "rank": 2,
                "rank_sort": 2,
                "total": 114,
            },
            {
                "event_total": 70,
                "last_rank": 7,
                "league_entry": 253941,
                "rank": 3,
                "rank_sort": 3,
                "total": 109,
            },
            {
                "event_total": 43,
                "last_rank": 2,
                "league_entry": 253943,
                "rank": 4,
                "rank_sort": 4,
                "total": 94,
            },
            {
                "event_total": 53,
                "last_rank": 6,
                "league_entry": 253948,
                "rank": 5,
                "rank_sort": 5,
                "total": 93,
            },
            {
                "event_total": 29,
                "last_rank": 4,
                "league_entry": 253940,
                "rank": 6,
                "rank_sort": 6,
                "total": 77,
            },
            {
                "event_total": 36,
                "last_rank": 9,
                "league_entry": 253946,
                "rank": 7,
                "rank_sort": 7,
                "total": 69,
            },
            {
                "event_total": 27,
                "last_rank": 8,
                "league_entry": 253944,
                "rank": 8,
                "rank_sort": 8,
                "total": 65,
            },
            {
                "event_total": 15,
                "last_rank": 5,
                "league_entry": 253949,
                "rank": 9,
                "rank_sort": 9,
                "total": 58,
            },
            {
                "event_total": 28,
                "last_rank": 10,
                "league_entry": 253939,
                "rank": 10,
                "rank_sort": 10,
                "total": 57,
            },
            {
                "event_total": 19,
                "last_rank": 11,
                "league_entry": 253942,
                "rank": 11,
                "rank_sort": 11,
                "total": 46,
            },
        ]
    }

    df1 = pd.DataFrame.from_dict(gw1_temp["standings"])

    # Merge league entries into GW1 standings
    df1 = df1.merge(df_entries, on="league_entry", how="left")

    # add data in GW1 dataframe
    df1["update"] = (
        datetime(2025, 8, 22, tzinfo=pytz.UTC).replace(microsecond=0).isoformat()
    )
    # add best gameweek for GW1
    max_value = df1["event_total"].max()
    df1["best_gw"] = np.where(df1["event_total"] == max_value, "best", "")

    df2 = pd.DataFrame.from_dict(gw2_temp["standings"])

    # Merge league entries into GW2 standings
    df2 = df2.merge(df_entries, on="league_entry", how="left")

    # add data in GW2 dataframe
    df2["update"] = (
        datetime(2025, 8, 30, tzinfo=pytz.UTC).replace(microsecond=0).isoformat()
    )
    # add best gameweek for GW2
    max_value = df2["event_total"].max()
    df2["best_gw"] = np.where(df2["event_total"] == max_value, "best", "")

    df = pd.concat([df1, df2])

    # Replace sqlalchemy null with pandas NaN
    df["last_rank"] = df["last_rank"].replace({null: pd.NA})

    # add progress
    df["progress"] = df.apply(get_progress, axis=1)

    return df


def check_initial_data_available(cutoff_date: str) -> bool:
    """
    Check if the initial data exists before running daily updates
    """
    con = duckdb.connect("data.db")

    # Check if table exists
    tables = con.execute("SHOW TABLES").fetchall()
    if ("draft_standings",) not in tables:
        print("Table draft_standings does not exist yet.")
        return False  # no data available

    df_existing = con.execute("SELECT * FROM draft_standings ORDER BY update DESC").df()

    # Your existing cutoff_date logic
    data_available = False
    if not df_existing.empty:
        data_available = (df_existing["update"] <= cutoff_date).any()
    con.close()
    return data_available


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
    # initialize data
    cutoff_date = (
        datetime(2025, 8, 31, tzinfo=pytz.UTC).replace(microsecond=0).isoformat()
    )
    if check_initial_data_available(cutoff_date):
        print("initial data is already exist")
    else:
        print("initial data not exist")
        df_initial = add_initial_data()
        store_draft(df_initial)

    # daily data update
    df = fetch_draft()
    store_draft(df)

    # assign gameweek
    update_draft_standings_gw()


if __name__ == "__main__":
    main()
