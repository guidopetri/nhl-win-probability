#! /usr/bin/env python3

"""
Download NHL team data from the NHL API and load to a database. Works via a DAG
scheduler.
"""

from luigi import Task, LocalTarget
from luigi.format import Nop
from luigi.parameter import Parameter
from luigi.util import requires, inherits
from pipeline_import.postgres_templates import CopyWrapper, DimensionTable
from pipeline_import.postgres_templates import TransactionFactTable
from pipeline_import.postgres_templates import HashableDict


class GetTeams(Task):

    def output(self):
        import os

        file_location = (f'~/Temp/luigi/teams-json.pckl')
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        import requests
        import pickle

        self.output().makedirs()

        # get team IDs
        teams_url = 'https://statsapi.web.nhl.com/api/v1/teams'

        r = requests.get(teams_url)
        r.raise_for_status()
        data = r.json()['teams']

        with self.output().open('w') as f:
            pickle.dump(data, f)


@requires(GetTeams)
class GetRosters(Task):

    season = Parameter(default='20202021')

    # use NHL API endpoint:
    # https://statsapi.web.nhl.com/api/v1/teams/{{team_id}}?expand=team.roster
    # where `team_id` is a number representing the team, e.g. `20` for CGY

    def output(self):
        import os

        file_location = (f'~/Temp/luigi/rosters-json.pckl')
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        import requests
        import pickle

        self.output().makedirs()

        with self.input().open('r') as f:
            teams = pickle.load(f)

        team_ids = [team['id'] for team in teams]

        # get the roster for every team using the NHL API
        roster_url_template = 'https://statsapi.web.nhl.com/api/v1/teams/{}'
        params = {'expand': 'team.roster'}

        rosters = {}

        for team_id in team_ids:
            roster_url = roster_url_template.format(team_id)

            r = requests.get(roster_url, params=params)
            r.raise_for_status()
            data = r.json()
            roster = data['teams'][0]['roster']['roster']
            rosters[team_id] = roster

        with self.output().open('w') as f:
            pickle.dump(rosters, f)


@requires(GetRosters)
class GetGoalies(Task):

    def output(self):
        import os

        file_location = (f'~/Temp/luigi/goalies-json.pckl')
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        import pickle

        self.output().makedirs()

        with self.input().open('r') as f:
            rosters = pickle.load(f)

        goalies = [player
                   for roster in rosters.values()
                   for player in roster
                   if player['position']['code'] == 'G'
                   ]

        with self.output().open('w') as f:
            pickle.dump(goalies, f)


@requires(GetGoalies)
class GetGameLogs(Task):

    # use NHL API endpoint:
    # https://statsapi.web.nhl.com/api/v1/people/{{player_id}}/stats?stats=gameLog
    # where `player_id` is a number representing the player (in our case
    # goalies), e.g. `8474593` for Jacob Markstrom

    def output(self):
        import os

        file_location = (f'~/Temp/luigi/gamelogs-json.pckl')
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        import requests
        import pickle

        self.output().makedirs()

        with self.input().open('r') as f:
            goalies = pickle.load(f)

        goalie_ids = [goalie['person']['id'] for goalie in goalies]

        # get game logs per goalie through the NHL API
        player_url_template = 'https://statsapi.web.nhl.com/api/v1/people/{}/stats'  # noqa
        params = {'stats': 'gameLog'}

        goalie_stats = {}

        for goalie_id in goalie_ids:
            player_url = player_url_template.format(goalie_id)

            r = requests.get(player_url, params=params)
            r.raise_for_status()
            data = r.json()
            stats = data['stats'][0]['splits']
            goalie_stats[goalie_id] = stats

        with self.output().open('w') as f:
            pickle.dump(goalie_stats, f)


@requires(GetTeams)
class CleanTeams(Task):

    def output(self):
        import os

        file_location = (f'~/Temp/luigi/teams.pckl')
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        import pickle
        from pandas import DataFrame

        self.output().makedirs()

        with self.input().open('r') as f:
            teams = pickle.load(f)

        cleaned_teams = []

        for team in teams:
            # skip teams that aren't active anymore
            if not team['active']:
                continue

            team_id = team['id']
            team_name = team['name']
            team_shortname = team['abbreviation']
            cleaned_teams.append((team_id,
                                  team_name,
                                  team_shortname,
                                  ))

        cleaned_teams = DataFrame(cleaned_teams)
        cleaned_teams.columns = ['team_id', 'team_name', 'team_shortname']

        with self.output().temporary_path() as temp_output_path:
            cleaned_teams.to_pickle(temp_output_path, compression=None)


@requires(GetRosters)
class CleanRosters(Task):

    def output(self):
        import os

        file_location = (f'~/Temp/luigi/rosters.pckl')
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        import pickle
        from pandas import DataFrame

        self.output().makedirs()

        with self.input().open('r') as f:
            rosters = pickle.load(f)

        cleaned_rosters = []

        for team_id, roster in rosters.items():
            goalie_ids = [player['person']['id']
                          for player in roster
                          if player['position']['code'] == 'G'
                          ]

            cleaned_rosters.extend([(team_id, self.season, goalie_id)
                                    for goalie_id in goalie_ids])

        cleaned_rosters = DataFrame(cleaned_rosters)
        cleaned_rosters.columns = ['team_id', 'season', 'goalie_id']

        with self.output().temporary_path() as temp_output_path:
            cleaned_rosters.to_pickle(temp_output_path, compression=None)


@requires(GetGoalies)
class CleanGoalies(Task):

    def output(self):
        import os

        file_location = (f'~/Temp/luigi/goalies.pckl')
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        import pickle
        from pandas import DataFrame

        self.output().makedirs()

        with self.input().open('r') as f:
            goalies = pickle.load(f)

        cleaned_goalies = [(goalie['person']['id'],
                            goalie['person']['fullName'])
                           for goalie in goalies]

        cleaned_goalies = DataFrame(cleaned_goalies)
        cleaned_goalies.columns = ['goalie_id', 'goalie_name']

        with self.output().temporary_path() as temp_output_path:
            cleaned_goalies.to_pickle(temp_output_path, compression=None)


@requires(GetGameLogs)
class CleanGameLogs(Task):

    def output(self):
        import os

        file_location = (f'~/Temp/luigi/gamelogs.pckl')
        return LocalTarget(os.path.expanduser(file_location), format=Nop)

    def run(self):
        import pickle
        from pandas import DataFrame

        self.output().makedirs()

        with self.input().open('r') as f:
            gamelogs = pickle.load(f)

        cleaned_gamelogs = []

        for goalie_id, gamelog in gamelogs.items():
            game_id = gamelog['game']['gamePk']
            game_date = gamelog['date']
            season = gamelog['season']
            time_on_ice = gamelog['stat']['timeOnIce']
            goals_against = gamelog['stat']['goalsAgainst']
            goalie_team = gamelog['team']['id']
            opponent = gamelog['opponent']['id']
            even_strength_shots = gamelog['stat']['evenShots']
            even_strength_saves = gamelog['stat']['evenSaves']
            short_shots = gamelog['stat']['shortHandedShots']
            short_saves = gamelog['stat']['shortHandedSaves']
            power_shots = gamelog['stat']['powerPlayShots']
            power_saves = gamelog['stat']['powerPlaySaves']
            is_home_game = gamelog['isHome']
            is_overtime_game = gamelog['isOT']
            overtimes = gamelog['stat']['ot']
            is_won_game = gamelog['isWin']

            cleaned_gamelogs.append((game_id,
                                     game_date,
                                     season,
                                     goalie_id,
                                     time_on_ice,
                                     goals_against,
                                     goalie_team,
                                     opponent,
                                     even_strength_shots,
                                     even_strength_saves,
                                     short_shots,
                                     short_saves,
                                     power_shots,
                                     power_saves,
                                     is_home_game,
                                     is_overtime_game,
                                     overtimes,
                                     is_won_game,
                                     ))

        cleaned_gamelogs = DataFrame(cleaned_gamelogs)
        cleaned_gamelogs.columns = ['game_id',
                                    'game_date',
                                    'season',
                                    'goalie_id',
                                    'time_on_ice_in_seconds',
                                    'goals_against',
                                    'goalie_team',
                                    'opponent',
                                    'even_strength_shots',
                                    'even_strength_saves',
                                    'short_shots',
                                    'short_saves',
                                    'power_shots',
                                    'power_saves',
                                    'is_home_game',
                                    'is_overtime_game',
                                    'overtimes',
                                    'is_won_game',
                                    ],

        with self.output().temporary_path() as temp_output_path:
            cleaned_gamelogs.to_pickle(temp_output_path, compression=None)


@requires(CleanTeams)
class CopyTeams(DimensionTable):
    # placeholder, just inherits `DimensionTable`
    pass


@requires(CleanGoalies)
class CopyGoalies(DimensionTable):
    pass


@requires(CleanRosters)
class CopyRosters(TransactionFactTable):
    pass


@requires(CleanGameLogs)
class CopyGameLogs(TransactionFactTable):
    pass


@inherits(GetGameLogs)
class WriteToDB(CopyWrapper):
    jobs = [{'table_type': CopyGoalies,
             'fn':         CleanGoalies,
             'table':      'goalies',
             'columns':    ['goalie_id',
                            'goalie_name'],
             'id_cols':    ['goalie_id'],
             'date_cols':  [],
             'merge_cols': HashableDict()},
            {'table_type': CopyTeams,
             'fn':         CleanTeams,
             'table':      'teams',
             'columns':    ['team_id',
                            'team_name',
                            'team_short_name',
                            ],
             'id_cols':    ['team_id'],
             'date_cols':  [],
             'merge_cols': HashableDict()},
            {'table_type': CopyGameLogs,
             'fn':         CleanGameLogs,
             'table':      'gamelog',
             'columns':    ['game_id',
                            'game_date',
                            'season',
                            'goalie_id',
                            'time_on_ice_in_seconds',
                            'goals_against',
                            'goalie_team',
                            'opponent',
                            'even_strength_shots',
                            'even_strength_saves',
                            'short_shots',
                            'short_saves',
                            'power_shots',
                            'power_saves',
                            'is_home_game',
                            'is_overtime_game',
                            'overtimes',
                            'is_won_game',
                            ],
             'id_cols':    ['game_id',
                            'season',
                            'goalie_id',
                            ],
             'date_cols':  ['game_date'],
             'merge_cols': HashableDict({'goalie_team': ('teams', 'team_id'),
                                         'opponent': ('teams', 'team_id'),
                                         'goalie_id': ('goalies', 'goalie_id'),
                                         })},
            {'table_type': CopyRosters,
             'fn':         CleanRosters,
             'table':      'team_goalies',
             'columns':    ['team_id',
                            'season',
                            'goalie_id',
                            ],
             'id_cols':    ['team_id', 'season'],
             'date_cols':  [],
             'merge_cols': HashableDict({'team_id': ('teams', 'team_id'),
                                         'goalie_id': ('goalies', 'goalie_id'),
                                         })},
            ]
