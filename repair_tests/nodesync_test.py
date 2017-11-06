# -*- coding: UTF-8 -*-
import calendar
import time
import heapq

from functools import total_ordering
from operator import attrgetter

from ccmlib.node import ToolError
from dse.query import SimpleStatement
from dse.metadata import Murmur3Token
from nose.tools import assert_true

from dtest import Tester, debug
from tools.assertions import assert_length_equal
from tools.data import create_cf, create_ks
from tools.decorators import since


# TODO: this only work with murmur3, we should make sure the tests force that
MIN_TOKEN = -(2 ** 63)


def toTimestamp(dt):
    return (calendar.timegm(dt.timetuple())) * 1000


@total_ordering
class NodeSyncRecord(object):
    """ Represents a record from the status table """
    def __init__(self, keyspace, table, start, end, last_time=None, last_successful_time=None):
        self.keyspace = keyspace
        self.table = table
        self.start = start
        self.end = end
        self.last_time = last_time
        self.last_successful_time = last_successful_time
        self.last_was_success = last_time is not None and last_successful_time is not None and last_time == last_successful_time

    def __lt__(self, other):
        if self.start == other.start:
            return self.end < other.end
        return self.start < other.start

    def __str__(self):
        def tk(token):
            return '<min>' if token == MIN_TOKEN else token

        if self.last_time is None and self.last_successful_time is None:
            validation = '<none>'
        else:
            validation = 'last={} ({}ms ago)'.format(self.last_time, (time.time() * 1000) - self.last_time)
            if not self.last_was_success:
                validation += ', last_success={} ({}ms ago)'.format(self.last_successful_time, (time.time() * 1000) - self.last_successful_time)
        return "{ks}.{tbl}-({start}, {end}]@({val})".format(ks=self.keyspace, tbl=self.table,
                                                            start=tk(self.start), end=tk(self.end),
                                                            val=validation)

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return self.__dict_ == other.__dict__

    def from_new_start(sefl, new_start):
        return NodeSyncRecord(self.keyspace, self.table, new_start, self.end, self.last_time, self.last_successful_time)

    def to_new_end(sefl, new_end):
        return NodeSyncRecord(self.keyspace, self.table, self.start, new_end, self.last_time, self.last_successful_time)


def parse_record(row):
    last_time = None
    last_successful_time = None

    if row.last_successful_validation is None:
        if row.last_unsuccessful_validation is not None:
            last_time = toTimestamp(row.last_unsuccessful_validation.started_at)
    else:
        last_successful_time = toTimestamp(row.last_successful_validation.started_at)
        if row.last_unsuccessful_validation is None or row.last_successful_validation.started_at > row.last_unsuccessful_validation.started_at:
            last_time = last_successful_time
        else:
            last_time = toTimestamp(row.last_unsuccessful_validation.started_at)
    return NodeSyncRecord(row.keyspace_name, row.table_name, row.start_token, row.end_token, last_time, last_successful_time)


def compareStartEnd(start, end):
    if end == MIN_TOKEN:
        return -1
    return -1 if start < end else (0 if start == end else 1)


def compareEndEnd(end1, end2):
    if end1 == MIN_TOKEN and end2 == MIN_TOKEN:
        return 0
    if end1 == MIN_TOKEN:
        return 1
    if end2 == MIN_TOKEN:
        return -1

    return -1 if end1 < end2 else (0 if end1 == end2 else 1)


def consolidate(keyspace, table, records):
    if len(records) == 0:
        return [NodeSyncRecord(keyspace, table, MIN_TOKEN, MIN_TOKEN)]

    heapq.heapify(records)

    result = []
    curr = heapq.heappop(records)

    # If the first doesn't start at the very beginning of the ring, add what is missing
    if curr.start != MIN_TOKEN:
        result.append(NodeSyncRecord(keyspace, table, MIN_TOKEN, curr.start))

    while len(records) > 0:
        next = heapq.heappop(records)

        startEndCmp = compareStartEnd(next.start, curr.end)
        if startEndCmp >= 0:
            # next record starts after the current one. Add current one (and, if there is a gap between curr and next,
            # add that as well) and move to next.
            result.append(curr)
            if startEndCmp > 0:
                result.append(NodeSyncRecord(keyspace, table, curr.end, next.start))
            curr = next
        else:
            # next record intersects with curr on some part. First add part that comes _before_ the intesection if any
            if curr < next:
                result.append(curr.to_new_end(next.start))

            # then, we'll deal with the intersection of curr and next. We'll deal with the part following that intersection
            # later, so push that first back to the heap
            endEndCmp = compareEndEnd(next.end, curr.end)
            if endEndCmp < 0:
                # next ends before curr, push the rest of curr to be dealt later
                heapq.heappush(records, curr.from_new_start(next.end))
            elif endEndCmp > 0:
                # curr ends before next, push the rest of next to be dealt later
                heapq.heappush(records, next.from_new_start(curr.end))

            # and then update curr to be the intersection
            curr = NodeSyncRecord(keyspace, table, next.start, next.end if endEndCmp < 0 else curr.end,
                                  max(curr.last_time, next.last_time), max(curr.last_successful_time, next.last_successful_time))

    # add the last curr, and if it doesn't cover the end of the ring, adds what's missing
    result.append(curr)
    if curr.end != MIN_TOKEN:
        result.append(NodeSyncRecord(keyspace, table, curr.end, MIN_TOKEN))

    return result


def read_nodesync_status(session, keyspace, table):
    """
    Reads in the rows from the nodesync status system table that pertain to a specific data table,
    returning a list of NodeSyncRecord.
    """
    query = SimpleStatement("""SELECT * FROM system_distributed.nodesync_status WHERE keyspace_name='{keyspace}'
                AND table_name='{table}' ALLOW FILTERING""".format(keyspace=keyspace, table=table),
                            fetch_size=None)
    raw_records = [ parse_record(row) for row in session.execute(query).current_rows ]
    records = consolidate(keyspace, table, raw_records)
    return records


def wait_for_all_segments(session, keyspace, table, timeout=30, predicate=lambda r: r.last_time is not None):
    """
    Waits up to :timeout to see if every segment of :keyspace.:table pass the provided :predicate
    If :predicate is not set, it simply checks if they've ever been validated
    """
    start = time.time()
    while start + timeout > time.time():
        nodesync_status = read_nodesync_status(session, keyspace, table)
        if all(predicate(record) for record in nodesync_status):
            return True
        time.sleep(1)
    return False

def validated_since(timestamp):
    return lambda r: r.last_time > timestamp

def not_validated():
    return lambda r: r.last_time is None and r.last_successful_time is None

def get_oldest_segments(session, keyspace, table, segment_count, only_successful=False):
    """
    Returns the :segment_count oldest segments from the nodesync status table that pertain to :keyspace.:table
    """
    assert_true(wait_for_all_segments(session, keyspace, table), "Not all segments repaired in time.")

    def sort_record(record):
        time = record.last_successful_time if only_successful else record.last_time
        return time if time is not None else -1

    nodesync_status = read_nodesync_status(session, keyspace, table)
    nodesync_status.sort(key=sort_record)
    return nodesync_status[0:segment_count]


def get_unsuccessful_validations(session, keyspace, table):
    """
    Returns the set of rows in the nodesync status table for :keyspace.:table where the latest validations
    were unsuccessful
    """
    nodesync_status = read_nodesync_status(session, keyspace, table)
    return [record for record in nodesync_status if not record.last_was_success]


@since('4.0')
class TestNodeSync(Tester):

    def _prepare_cluster(self, nodes=1, byteman=False, jvm_arguments=None, options=None,
                         min_validation_interval_ms=1000, segment_lock_timeout_sec=10, segment_size_target_mb=10):

        if jvm_arguments is None:
            jvm_arguments = []

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
        create_cf(session, 'table1', key_type='int', columns={'v': 'text'})

        INSERTS = 1000
        debug("Inserting data...")
        for i in range(0, INSERTS):
            session.execute("INSERT INTO ks.table1(key, v) VALUES({}, 'foobar')".format(i))

        # NodeSync is not yet running, so make sure there is no state
        wait_for_all_segments(session, 'ks', 'table1', predicate=not_validated())

        # Enable NodeSync and make sure everything gets validated
        debug("Enabling NodeSync and waiting on initial validations...")
        session.execute("ALTER TABLE ks.table1 WITH nodesync = {'enabled': 'true'}")
        wait_for_all_segments(session, 'ks', 'table1')

        # Now, test that segments gets continuously validated by repeatedly grabbing a timestamp and make sure
        # that everything gets validated past this timestamp.
        RUNS = 5
        for _ in range(0, RUNS):
            timestamp = time.time() * 1000
            debug("Waiting on validations being older than {}".format(timestamp))
            wait_for_all_segments(session, 'ks', 'table1', predicate=validated_since(timestamp))
