import duckdb
import streamlit as st

con = duckdb.connect("data.db", read_only=True)

df_draft = con.execute("SELECT * FROM draft_standings").df()
# df_classic = con.execute("SELECT * FROM classic_standings").df()
# df_players = con.execute("SELECT * FROM players_stats").df()

st.write("Draft standings", df_draft.tail())
# st.write("Classic standings", df_classic.tail())
# st.write("Players", df_players.head())
