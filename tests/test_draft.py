import unittest
from unittest.mock import Mock, patch

import pandas as pd

from etl import draft


class DraftCollectorTests(unittest.TestCase):
    def test_fetch_draft_uses_current_league_and_enriches_standings(self):
        league_payload = {
            "standings": [
                {
                    "event_total": 45,
                    "last_rank": None,
                    "league_entry": 185040,
                    "rank": 1,
                    "rank_sort": 1,
                    "total": 45,
                },
                {
                    "event_total": 30,
                    "last_rank": 1,
                    "league_entry": 185041,
                    "rank": 2,
                    "rank_sort": 2,
                    "total": 30,
                },
            ],
            "league_entries": [
                {
                    "id": 185040,
                    "entry_name": "Heavenly Food",
                    "player_first_name": "Hardani",
                    "player_last_name": "Maulana",
                },
                {
                    "id": 185041,
                    "entry_name": "Second Team",
                    "player_first_name": "Second",
                    "player_last_name": "Manager",
                },
            ],
        }
        response = Mock()
        response.json.return_value = league_payload

        with patch.object(draft.requests, "get", return_value=response) as mock_get:
            result = draft.fetch_draft()

        mock_get.assert_called_once_with(
            "https://draft.premierleague.com/api/league/35582/details", timeout=30
        )
        response.raise_for_status.assert_called_once()
        self.assertEqual(result.loc[0, "entry_name"], "Heavenly Food")
        self.assertEqual(result.loc[0, "player_name"], "Hardani Maulana")
        self.assertEqual(result.loc[0, "best_gw"], "best")
        self.assertEqual(result.loc[1, "progress"], "red")
        self.assertTrue(pd.notna(result.loc[0, "update"]))


if __name__ == "__main__":
    unittest.main()
