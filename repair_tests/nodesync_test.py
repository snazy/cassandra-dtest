# -*- coding: UTF-8 -*-
import time, calendar

from operator import attrgetter
from ccmlib.node import ToolError
from dse.query import SimpleStatement
from nose.tools import assert_true

from dtest import Tester, debug
from tools.data import create_ks, create_cf
from tools.decorators import since


def toTimestamp(dt):
    return (calendar.timegm(dt.timetuple())) * 1000

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
        return toTimestamp(row.last_unsuccessful_validation.started_at)
    elif row.last_successful_validation is not None:
        return toTimestamp(row.last_successful_validation.started_at)
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
        validation_timestamp = last_validation(row)
        if validation_timestamp is None:
            return False
        return validation_timestamp >= timestamp


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
        # We shouldn't return success if nothing has been read.
        # TODO: we should actually do some token check magic to check that we do cover the whole range
        if len(unvalidated_rows) == 0 and len(nodesync_status.current_rows) != 0:
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

    def _prepare_cluster(self, nodes=1, byteman=False, jvm_arguments=[], options=None,
                         min_validation_interval_ms=1000, segment_lock_timeout_sec=10, segment_size_target_mb=10):
        cluster = self.cluster
        cluster.populate(nodes, install_byteman=byteman)
        if options:
            cluster.set_configuration_options(values=options)
        if min_validation_interval_ms:
            jvm_arguments += ["-Ddse.nodesync.min_validation_interval_ms={}".format(min_validation_interval_ms)]
        if segment_lock_timeout_sec:
            jvm_arguments += ["-Ddse.nodesync.segment_lock_timeout_sec={}".format(segment_lock_timeout_sec)]
        if segment_size_target_mb:
            jvm_arguments += ["-Ddse.nodesync.segment_size_target_bytes={}".format(segment_size_target_mb * 1024 * 1024)]
        debug("Starting cluster...")
        cluster.start(wait_for_binary_proto=True, jvm_args=jvm_arguments)
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self.session = session

        return self.session

    def test_decommission(self):
        session = self._prepare_cluster(nodes=4)
        create_ks(session, 'ks', 3)
        create_cf(session, 't', key_type='int', columns={'v': 'int', 'v2': 'int'}, nodesync=True)
        self.cluster.nodelist()[2].decommission()

    def test_no_replication(self):
        session = self._prepare_cluster(nodes=[2, 2])
        create_ks(session, 'ks', {'dc1': 3, 'dc2': 3})
        create_cf(session, 't', key_type='int', columns={'v': 'int', 'v2': 'int'}, nodesync=True)

        # reset RF at dc1 as 0
        session.execute("ALTER KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 0, 'dc2': 3};")

        # wait 5s for error
        time.sleep(10)

    def test_cannot_run_repair_on_nodesync_enabled_table(self):
        """
        * Check that ordinary repair cannot be run on NodeSync enabled table
        @jira_ticket APOLLO-966
        """
        self.ignore_log_patterns = [
            'Cannot run both full and incremental repair, choose either --full or -inc option.']
        session = self._prepare_cluster(nodes=2)

        create_ks(session, 'ks', 2)
        create_cf(session, 'table1', columns={'c1': 'text', 'c2': 'text'})

        node1 = self.cluster.nodelist()[0]
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

    def test_basic_nodesync_validation(self):
        """
        Validate basic behavior of NodeSync: that a table gets entirely validated and continuously so
        """
        session = self._prepare_cluster(nodes=2)
        create_ks(session, 'ks', 2)
        create_cf(session, 'table1', key_type='int', columns={'v' : 'text'})

        INSERTS = 1000
        debug("Inserting data...")
        for i in range(0, INSERTS):
            session.execute("INSERT INTO ks.table1(key, v) VALUES(%d, 'foobar')" % (i))

        # NodeSync is not yet running, so make sure there is no state
        self.assertEqual(0, len(read_nodesync_status_for_table(session, 'ks', 'table').current_rows))

        # Enable NodeSync and make sure everything gets validated
        debug("Enabling NodeSync and waiting on initial validations...")
        session.execute("ALTER TABLE ks.table1 WITH nodesync = {'enabled': 'true'}")
        wait_for_all_segments(session)

        # Now, test that segments gets continuously validated by repeatedly grabbing a timestamp and make sure
        # that everything gets validated past this timestamp.
        RUNS = 5
        for _ in range(0, RUNS):
            timestamp = time.time() * 1000
            debug("Waiting on validations being older than %d" % (timestamp))
            wait_for_all_segments(session, timestamp=timestamp)
