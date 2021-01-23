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

    def output(self):
        # write to file
        pass

    def run(self):
        # get the roster for every team using the NHL API
        pass


@requires(GetRosters)
class GetGameLogs(Task):

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
