import duckdb
import streamlit as st

con = duckdb.connect("data.db", read_only=True)

df_draft = con.execute("SELECT * FROM draft_standings").df()
df_draft_sorted = df_draft.sort_values(by=["update", "rank"], ascending=[False, True])

df_draft_gw = con.execute("SELECT * FROM draft_standings_gw").df()
df_draft_gw_sorted = df_draft_gw.sort_values(
    by=["update", "rank"], ascending=[False, True]
)
# df_classic = con.execute("SELECT * FROM classic_standings").df()
# df_players = con.execute("SELECT * FROM players_stats").df()

st.write("Draft standings per GW", df_draft_gw_sorted)
st.write("Draft standings", df_draft_sorted)
# st.write("Classic standings", df_classic.tail())
# st.write("Players", df_players.head())
