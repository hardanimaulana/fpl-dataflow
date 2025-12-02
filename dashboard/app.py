import streamlit as st
import duckdb
import pandas as pd
from io import BytesIO
import os
from app_version import APP_VERSION

st.set_page_config(page_title="Gandaria Draft League", layout="wide")

st.sidebar.write(f"App version: {APP_VERSION}")

DB_PATH = "dashboard/data.db"


@st.cache_resource(hash_funcs={"builtins.function": id})
def get_connection(path, version: str):
    return duckdb.connect(path, read_only=True)


con = get_connection(DB_PATH, APP_VERSION)


# --- Helper function to clean DataFrame ---
def clean_df(df):
    df = df.reset_index(drop=True)
    df = df.loc[:, [c for c in df.columns if c not in ("", None)]]
    return df


# ========== 1. Last updated ==========
# Get the latest update timestamp across all standings
last_update = con.execute(
    "SELECT MAX(update) as last_update FROM draft_standings"
).fetchone()[0]
if last_update:
    st.markdown(f"**📅 Last data update:** {last_update}")
else:
    st.warning("No update timestamp found in the database.")

st.markdown("✅ Click on the column headers to sort the table interactively.")
st.markdown("---")

# ========== 2. Summary per league_entry ==========
st.subheader("📈 Summary per Team")

df_summary = con.execute(
    """
    SELECT 
        entry_name,
        player_name,
        SUM(CASE WHEN progress = 'green' THEN 1 ELSE 0 END) AS total_green,
        SUM(CASE WHEN progress = 'red' THEN 1 ELSE 0 END) AS total_red,
        SUM(CASE WHEN best_gw = 'best' THEN 1 ELSE 0 END) AS best_points_count
    FROM draft_standings_gw
    GROUP BY entry_name, player_name
    ORDER BY total_green DESC, total_red ASC, best_point_count DESC
"""
).df()

# Calculate progress points (2*green - 1*red) and best points (just count)
df_summary["progress_points"] = (
    2 * df_summary["total_green"] - 1 * df_summary["total_red"]
)
df_summary["best_points"] = df_summary["best_points_count"]

df_summary = df_summary[
    [
        "entry_name",
        "player_name",
        "total_green",
        "total_red",
        "progress_points",
        "best_points",
    ]
]

# Rename columns for readability
rename_map = {
    "entry_name": "Team",
    "player_name": "Manager",
    "total_green": "🟢⬆️",
    "total_red": "🔴⬇️",
    "progress_points": "Progress Points",
    "best_points_count": "Best Points",
}
df_summary = df_summary.rename(columns=rename_map)

st.dataframe(df_summary, use_container_width=True)

# ========== 3. Last Standings ==========
# Get the latest gameweek number dynamically
latest_gw = con.execute(
    """
    SELECT 'GW' || MAX(CAST(SUBSTRING(gameweek, 3) AS INT)) FROM draft_standings_gw
"""
).fetchone()[0]

st.subheader(f"🏆 Last Standings - {latest_gw}")

df_last = con.execute(
    f"""
    SELECT *
    FROM draft_standings_gw
    WHERE gameweek = '{latest_gw}'
    ORDER BY rank_sort
"""
).df()

# Drop unnecessary columns and clean index
columns_to_drop = ["update", "gameweek", "league_entry"]
df_last = df_last.drop(columns=[c for c in columns_to_drop if c in df_last.columns])
df_last = clean_df(df_last)

# Rename columns for readability
rename_map = {
    "event_total": "GW Points",
    "last_rank": "Last GW Rank",
    "rank": "Rank",
    "rank_sort": "Rank Sort",
    "total": "Total Points",
    "best_gw": "Best GW",
    "entry_name": "Team",
    "player_name": "Manager",
}
df_last = df_last.rename(columns=rename_map)

# Replace progress values with emojis
progress_map = {
    "green": "🟢⬆️",  # green up arrow
    "red": "🔴⬇️",  # red down arrow
    "": "➖",  # no change
}
if "progress" in df_last.columns:
    df_last["progress"] = df_last["progress"].map(progress_map)

# Show table
st.dataframe(df_last, use_container_width=True)

# ========== 4. Complete Gameweek ==========
st.subheader("📊 Complete Standings per Gameweek")
df_all = con.execute("SELECT * FROM draft_standings_gw ORDER BY update, rank_sort").df()

# Drop unnecessary columns and clean index
columns_to_drop = ["league_entry"]
df_all = df_all.drop(columns=[c for c in columns_to_drop if c in df_all.columns])
df_all = clean_df(df_all)
df_all.sort_values(["update", "rank"], ascending=[False, True])
# Rename columns for readability
rename_map = {
    "event_total": "GW Points",
    "last_rank": "Last GW Rank",
    "rank": "Rank",
    "rank_sort": "Rank Sort",
    "total": "Total Points",
    "best_gw": "Best GW",
    "entry_name": "Team",
    "player_name": "Manager",
    "last_update": "Last Update",
}
df_all = df_all.rename(columns=rename_map)
st.dataframe(
    df_all.sort_values(["update", "Rank"], ascending=[False, True]),
    use_container_width=True,
    height=600,
)
