# -*- coding: UTF-8 -*-
import time

from operator import attrgetter
from ccmlib.node import ToolError
from dse.query import SimpleStatement
from nose.tools import assert_true

from dtest import Tester, debug
from tools.data import create_ks, create_cf
from tools.decorators import since


def read_nodesync_status(session):
    """
    Reads in the entire nodesync status table, and returns the rows
    """
    return session.execute("SELECT * FROM system_distributed.nodesync_status")


def read_nodesync_status_for_table(session, keyspace, table):
    """
    Reads in the rows from the nodesync status system table that pertain to a specific data table
    """
    query = SimpleStatement("""SELECT * FROM system_distributed.nodesync_status WHERE keyspace_name='{keyspace}'
                AND table_name='{table}' ALLOW FILTERING""".format(keyspace=keyspace, table=table),
                            fetch_size=None)
    return session.execute(query)


def last_validation(row):
    """
    Takes in a single row from the nodesync status table, and returns the last time it was attempted to be validated,
    successfully or unsuccessfully. Returns None if validation has never been attempted.
    """
    if row.last_unsuccessful_validation is not None:
        return row.last_unsuccessful_validation.started_at
    elif row.last_successful_validation is not None:
        return row.last_successful_validation.started_at
    else:
        return None


def segment_has_been_repaired_recently(row, timestamp=None):
    """
    @param row: A row from the nodesync_status table
    @param timestamp: The time we want to know if the segment has been repaired since. If `None`, we check
    if the segment has ever been repaired.
    @return true if the segment has been repaired since timestamp
    """
    if timestamp is None:
        return not (row.last_unsuccessful_validation is None and row.last_successful_validation is None)
    else:
        if row.last_unsuccessful_validation is not None or row.last_successful_validation is not None:
            return last_validation(row)  # TODO handle the timestamp here
        else:
            return True


def wait_for_all_segments(session, timeout=30, timestamp=None):
    """
    Waits up to :timeout to see if every segment in the nodesync status table has been validated since :timestamp
    If :timestamp is None, just checks if they've ever been validated
    """
    start = time.time()
    while start + timeout > time.time():
        nodesync_status = read_nodesync_status(session)
        unvalidated_rows = [row for row in nodesync_status.current_rows
                            if not segment_has_been_repaired_recently(row, timestamp)]
        if len(unvalidated_rows) == 0:
            return True
        time.sleep(1)
    return False


def get_oldest_segments(session, keyspace, table, segment_count):
    """
    Returns the :segment_count oldest segments from the nodesync status table that pertain to :keyspace.:table
    """
    assert_true(wait_for_all_segments(session), "Not all segments repaired in time.")

    def sort_row(row):
        if row.last_unsuccessful_validation is not None:
            return row.last_unsuccessful_validation.started_at
        else:
            return row.last_successful_validation.started_at

    nodesync_status = read_nodesync_status_for_table(session, keyspace, table)
    nodesync_status.current_rows.sort(key=sort_row)
    return nodesync_status[0:segment_count]


def get_unsuccessful_validations(session, keyspace, table):
    """
    Returns the set of rows in the nodesync status table for :keyspace.:table where the latest validations
    were unsuccessful
    """
    nodesync_status = read_nodesync_status_for_table(session, keyspace, table)
    return [row for row in nodesync_status.current_rows if row.last_unsuccessful_validation is not None]


def equal_rows(row_a, row_b):
    """
    Compares two rows from the nodesync status table, and returns true if they have the same primary key
    """

    def compare_attributes(row_a, row_b, column_name):
        if not attrgetter(row_a, column_name) == attrgetter(row_b, column_name):
            return False

    primary_key_columns = ['keyspace_name', 'table_name', 'range_group', 'start_token', 'end_token']

    same_row = True
    for column in primary_key_columns:
        same_row = same_row and compare_attributes(row_a, row_b, column)

    return same_row


@since('4.0')
class TestNodeSync(Tester):

    def _prepare_cluster(self, nodes=1, byteman=False, jvm_arguments=[], options=None):
        cluster = self.cluster
        cluster.populate(nodes, install_byteman=byteman)
        if options:
            cluster.set_configuration_options(values=options)
        cluster.start(wait_for_binary_proto=True, jvm_args=jvm_arguments)
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self.session = session

        return self.session

    def test_decommission(self):
        session = self._prepare_cluster(nodes=4, jvm_arguments=["-Ddatastax.nodesync.min_validation_interval_ms={}".format(5000)])
        session.execute("""
                CREATE KEYSPACE IF NOT EXISTS ks
                WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' }
                """)
        session.execute('USE ks')
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 int) WITH nodesync = {'enabled': 'true'}")
        self.cluster.nodelist()[2].decommission()

    def test_no_replication(self):
        jvm_arguments = ["-Ddatastax.nodesync.min_validation_interval_ms={}".format(2000)]
        session = self._prepare_cluster(nodes=[2, 2], jvm_arguments=jvm_arguments)
        create_ks(session, 'ks', {'dc1': 3, 'dc2': 3})
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 int) WITH nodesync = {'enabled': 'true'}")

        # reset RF at dc1 as 0
        session.execute("ALTER KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 0, 'dc2': 3};")

        # wait 5s for error
        time.sleep(10)

    def cannot_run_repair_on_nodesync_enabled_table_test(self):
        """
        * Check that ordinary repair cannot be run on NodeSync enabled table
        @jira_ticket APOLLO-966
        """
        self.ignore_log_patterns = [
            'Cannot run both full and incremental repair, choose either --full or -inc option.']
        cluster = self.cluster
        debug("Starting cluster..")
        cluster.populate(2).start()
        node1, _ = cluster.nodelist()
        session = self.patient_cql_connection(node1)

        debug("Creating keyspace and table")
        create_ks(session, 'ks', 2)
        create_cf(session, 'table1', columns={'c1': 'text', 'c2': 'text'})

        # Check there is no problem executing repair on ks.table
        debug("Running repair on ks.table1")
        node1.nodetool('repair ks table1')

        # Now enable NodeSync - Check cannot run anti-entropy repair on NodeSync enabled table
        debug("Enabling nodesync on ks.table1 - cannot run repair")
        session.execute("ALTER TABLE ks.table1 WITH nodesync = {'enabled': 'true'}")
        with self.assertRaises(ToolError) as ctx:
            node1.nodetool('repair ks table1')
        self.assertIn('Cannot run anti-entropy repair on tables with NodeSync enabled', ctx.exception.stderr)

        # Now disable NodeSync on view_build_status table - no problem in running repair
        debug("Disabling nodesync on ks.table1 - can run repair again")
        session.execute("ALTER TABLE ks.table1 WITH nodesync = {'enabled': 'false'}")
        node1.nodetool('repair ks table1')
