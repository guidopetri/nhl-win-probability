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
        # write to file
        pass

    def run(self):
        # get the roster for every team using the NHL API
        pass


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
