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


class GetRosters(Task):

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

        # get team IDs
        teams_url = 'https://statsapi.web.nhl.com/api/v1/teams'

        r = requests.get(teams_url)
        r.raise_for_status()
        data = r.json()['teams']

        # get the roster for every team using the NHL API
        teams = [team['id'] for team in data]
        roster_url_template = 'https://statsapi.web.nhl.com/api/v1/teams/{}'
        params = {'expand': 'team.roster'}

        rosters = []

        for team in teams:
            roster_url = roster_url_template.format(team)

            r = requests.get(roster_url, params=params)
            r.raise_for_status()
            data = r.json()
            roster = data['teams'][0]['roster']['roster']
            rosters.append(roster)

        with self.output().temporary_path() as temp_output_path:
            pickle.dump(rosters, temp_output_path)


@requires(GetRosters)
class GetGameLogs(Task):

    # use NHL API endpoint:
    # https://statsapi.web.nhl.com/api/v1/people/{{player_id}}/stats?stats=gameLog
    # where `player_id` is a number representing the player (in our case
    # goalies), e.g. `8474593` for Jacob Markstrom

    def output(self):
        # write to file
        pass

    def run(self):
        # get game logs per goalie through the NHL API
        pass


@requires(GetGameLogs)
class CopyGameLogs(TransactionFactTable):
    # placeholder, just inherits `DimensionTable`
    pass


@requires(GetRosters)
class CopyRosters(DimensionTable):
    # placeholder, just inherits `DimensionTable`
    pass


@inherits(GetGameLogs)
class WriteToDB(CopyWrapper):
    # define jobs
    pass
