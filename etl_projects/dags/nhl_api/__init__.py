SOURCE = "nhl-api"
RESOURCE_MAPPING = {
    "teams": {
        "path": "/teams/{id}",
        "method": ["GET"],
        "modifiers": [
            "expand=team.schedule.next",
            "expand=team.schedule.previous",
            "expand=team.stats",
        ],
    },
    "team": {"path": "/teams/{id}", "method": ["GET"], "modifiers": []},
    "team_stats": {"path": "/teams/{id}/stats", "method": ["GET"], "modifiers": []},
    "team_roster": {"path": "/teams/{id}/roster", "method": ["GET"], "modifiers": []},
    "divisions": {"path": "/divisions/{id}", "method": ["GET"], "modifiers": []},
    "divisions": {"path": "/divisions/{id}", "method": ["GET"], "modifiers": []},
    "conferences": {"path": "/conferences/{id}", "method": ["GET"], "modifiers": []},
    "conferences": {"path": "/conferences/{id}", "method": ["GET"], "modifiers": []},
    "player": {"path": "/people/{id}", "method": ["GET"], "modifiers": []},
    "player_stats": {
        "path": "/people/{id}",
        "method": ["GET"],
        "modifiers": [
            "stats=onPaceRegularSeason",
            "stats=goalsByGameSituation=byMonth",
            "stats=byMonth",
            "stats=byDayOfWeek",
            "stats=vsDivision",
            "stats=vsConference",
            "stats=vsTeam",
            "stats=regularSeasonStatRankings",
            "stats=gameLog",
            "stats=winLoss",
            "stats=homeAndAway",
            "stats=statsSingleSeason",
            "season={yearyear}",
        ],
    },
    "prospects": {"path": "/draft/prospects", "method": ["GET"], "modifiers": []},
    "prospect": {"path": "/draft/prospects/{id}", "method": ["GET"], "modifiers": []},
    "awards": {"path": "/draft/awards", "method": ["GET"], "modifiers": []},
    "award": {"path": "/draft/awards/{id}", "method": ["GET"], "modifiers": []},
    "stats": {"path": "/stats", "method": ["GET"], "modifiers": []},
    "schedule": {
        "path": "/schedule",
        "method": ["GET"],
        "modifiers": [
            "startDate={startDate}",
            "endDate={endDate}",
            "date={yy-mm-dd}",
            "expand=schedule.broadcasts",
            "expand=schedule.linescore",
            "expand=schedule.ticket",
            "teamId={id}",
        ],
    },
    "standings": {
        "path": "/standings",
        "method": ["GET"],
        "modifiers": ["expand=standings.record", "date={date}", "season={season}"],
    },
    "stat_types": {"path": "/statTypes", "method": ["GET"], "modifiers": []},
    "standings_types": {"path": "/standingsTypes", "method": ["GET"], "modifiers": []},
    "game": {"path": "/people/{id}/feed/live", "method": ["GET"], "modifiers": []},
    "boxscore": {"path": "/people/{id}/boxscore", "method": ["GET"], "modifiers": []},
    "live_game": {
        # yyyymmdd_hhmmss
        "path": "/game/{id}/feed/live/diffPatch?startTimecode={date}",
        "method": ["GET"],
        "modifiers": [],
    },
    "draft": {"path": "/draft", "method": ["GET"], "modifiers": []},
    "specific_draft": {"path": "/draft/{year}", "method": ["GET"], "modifiers": []},
}
