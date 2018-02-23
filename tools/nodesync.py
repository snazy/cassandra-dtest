# -*- coding: UTF-8 -*-
"""
Functions and tools aimed at testing NodeSync, and more particularly with retrieving and
testing the content of the NodeSync status table.

The main methods exposed are :wait_for_all_segments, :get_oldest_segments and :get_unsuccessful_validations
"""
import time
import calendar
import heapq
import re
import subprocess

from ccmlib import common
from dtest import debug
from dse.query import SimpleStatement
from tools.assertions import assert_true


def enable_nodesync(session, keyspace, table):
    """ Enable NodeSync on a particular table """
    session.execute("ALTER TABLE {}.{} WITH nodesync = {{'enabled': 'true'}}".format(keyspace, table))


def disable_nodesync(session, keyspace, table):
    """ Disable NodeSync on a particular table """
    session.execute("ALTER TABLE {}.{} WITH nodesync = {{'enabled': 'false'}}".format(keyspace, table))


def nodesync_tool(cluster, args=list(), expected_stdout=None, expected_stderr=None,
                  ignore_stdout_order=False, ignore_stderr_order=False, print_debug=True):
    """
    Runs the nodesync command line tool with the specified arguments, ensuring the specified expected command output.

    @param cluster the cluster on which to run the tool
    @param args The arguments to be passed to nodesync
    @param expected_stdout A list of text lines that should be contained by the command stdout
    @param expected_stderr A list of text lines that should be contained by the command stderr
    @param ignore_stdout_order If stdout is not expected to be ordered
    @param ignore_stderr_order If stderr is not expected to be ordered
    @return a 2-tuple composed by the stdout and the stderr of the command execution
    """

    # prepare the command
    node = cluster.nodelist()[0]
    env = common.make_cassandra_env(node.get_install_cassandra_root(), node.get_node_cassandra_root())
    nodesync_bin = node.get_tool('nodesync')
    full_args = [nodesync_bin] + args
    if print_debug:
        debug('COMMAND: ' + ' '.join(str(arg) for arg in full_args))

    # run the command
    p = subprocess.Popen(full_args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if print_debug:
        debug('STDERR: ' + stderr)
        debug('STDOUT: ' + stdout)

    def non_blank_lines(text):
        return filter(lambda l: l.strip(), text.splitlines()) if text else []

    def check_lines(expected, actual, ignore_order=False):
        if expected:
            if ignore_order:
                expected = sorted(expected)
                actual = sorted(actual)
            for i in range(len(actual)):
                if expected[0] in actual[i]:
                    return check_lines(expected[1:], actual[1 + i:])
            return False
        return True

    # validate stderr
    stderr_lines = non_blank_lines(stderr)
    if expected_stderr:
        assert_true(check_lines(expected_stderr, stderr_lines, ignore_stderr_order),
                    'Expected lines in stderr %s but found %s (order matters: %s)'
                    % (expected_stderr, stderr_lines, not ignore_stderr_order))
    else:
        assert_true(len(stderr_lines) == 0, 'Found unexpected errors: ' + stderr)

    # validate stdout
    if expected_stdout:
        lines = non_blank_lines(stdout)
        assert_true(check_lines(expected_stdout, lines, ignore_stdout_order),
                    'Expected lines in stdout %s but found %s (order matters: %s)'
                    % (expected_stdout, lines, not ignore_stdout_order))

    # return the command results
    return stdout, stderr


# A convenience primarily meant to help debugging test failures. If you don't understand why
# a part of a test doesn't do what it should, you can enclose that part with:
#   with tracing() as trace:
#     ... some part of the test ...
# to get the NodeSync trace for that piece of code.
def tracing(cluster):
    return Tracer(cluster)


class Tracer():
    def __init__(self, cluster):
        self.cluster = cluster

    def __enter__(self):
        stdout, _ = nodesync_tool(self.cluster, ['tracing', 'enable'],
                                  expected_stderr=["Warning: Do not forget to stop tracing with 'nodesync tracing disable'."],
                                  print_debug=False)
        match = re.compile('Session id is ([a-z0-9-]+)').search(stdout)
        self.id = match.group(1)

    def __exit__(self, type, value, traceback):
        nodesync_tool(self.cluster, ['tracing', 'disable'], print_debug=False)
        stdout, _ = nodesync_tool(self.cluster, ['tracing', 'show', '-i ' + self.id], print_debug=False)
        print("-- Tracing output -----------------")
        print(stdout)
        print("-----------------------------------")


def nodesync_opts(min_validation_interval_ms=1000, segment_lock_timeout_sec=20, segment_size_target_kb=100,
                  size_checker_interval_sec=5, controller_update_interval_sec=300):
    """ Creates a list of JVM arguments that sets settings for NodeSync more suitable to testing

    This is generally intended to be used for building the nodesync_options argument of the prepare
    method in tools/preparation.py.

    Note that each parameter can be tweaked, but even without any argument passed, this will provide
    settings that are more suitable to dtest than the production default.
    """
    args = []
    if min_validation_interval_ms:
        args.append("-Ddse.nodesync.min_validation_interval_ms={}".format(min_validation_interval_ms))
    if segment_lock_timeout_sec:
        args.append("-Ddse.nodesync.segment_lock_timeout_sec={}".format(segment_lock_timeout_sec))
    if segment_size_target_kb:
        args.append("-Ddse.nodesync.segment_size_target_bytes={}".format(segment_size_target_kb * 1024))
    if size_checker_interval_sec:
        args.append("-Ddse.nodesync.size_checker_interval_sec={}".format(size_checker_interval_sec))
    if controller_update_interval_sec:
        args.append("-Ddse.nodesync.controller_update_interval_sec={}".format(controller_update_interval_sec))

    return args


# Note: this only work with murmur3, which is the default for test. It would be bad to generalize this a bit
# to other partitioners, but probably not urgent given murmur3 should definitively be the most used by and large
_MIN_TOKEN = -(2 ** 63)


class NodeSyncRecord(object):
    """ Represents a record from the status table

    Attributes:
        - keyspace and table: the keyspace and table this is a segment record for.
        - start and end: the start and end token of the segment it represents
        - last_time: the time of the last validation on this segment, or None if we have no such record
        - last_successful_time: the time of the last fully successful validation on this segment, or
          None if we have no such record.
        - last_was_success: if the last validation done was successful.
    """

    def __init__(self, keyspace, table, start, end, last_time=None, last_successful_time=None, missing_nodes=None):
        self.keyspace = keyspace
        self.table = table
        self.start = start
        self.end = end
        self.last_time = last_time
        self.last_successful_time = last_successful_time
        self.last_was_success = last_time is not None and last_successful_time is not None and last_time == last_successful_time
        self.missing_nodes = None if self.last_was_success else missing_nodes

    def __str__(self):
        def tk(token):
            return '<min>' if token == _MIN_TOKEN else token

        if self.last_time is None and self.last_successful_time is None:
            validation = '<none>'
        else:
            validation = 'last={} ({}s ago)'.format(self.last_time, ((time.time() * 1000) - self.last_time) / 1000)
            if self.last_successful_time is not None and not self.last_was_success:
                validation += ', last_success={} ({}s ago)'.format(self.last_successful_time, ((time.time() * 1000) - self.last_successful_time) / 1000)
        missing = ''
        if self.missing_nodes:
            missing = ', missing={}'.format(str(self.missing_nodes))
        return "{ks}.{tbl}-({start}, {end}]@({val}{missing})".format(ks=self.keyspace, tbl=self.table,
                                                                     start=tk(self.start), end=tk(self.end),
                                                                     val=validation, missing=missing)

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def from_new_start(self, new_start):
        """ Creates a new record, equivalent to this one except for the start token that will be :new_start.  """
        return NodeSyncRecord(self.keyspace, self.table, new_start, self.end, self.last_time, self.last_successful_time)

    def to_new_end(self, new_end):
        """ Creates a new record, equivalent to this one except for the end token that will be :new_end."""
        return NodeSyncRecord(self.keyspace, self.table, self.start, new_end, self.last_time, self.last_successful_time)

    @classmethod
    def _from_row(cls, row):
        """ Parse a row from the status table to an equivalent NodeSyncRecord. """

        def toTimestamp(dt):
            """ Convert a datetime to a milliseconds epoch timestamp """
            return (calendar.timegm(dt.timetuple())) * 1000

        last_time = None
        last_successful_time = None
        missing_nodes = None

        if row.last_successful_validation is None:
            if row.last_unsuccessful_validation is not None:
                last_time = toTimestamp(row.last_unsuccessful_validation.started_at)
                missing_nodes = row.last_unsuccessful_validation.missing_nodes
        else:
            last_successful_time = toTimestamp(row.last_successful_validation.started_at)
            if row.last_unsuccessful_validation is None or row.last_successful_validation.started_at > row.last_unsuccessful_validation.started_at:
                last_time = last_successful_time
            else:
                last_time = toTimestamp(row.last_unsuccessful_validation.started_at)
                missing_nodes = row.last_unsuccessful_validation.missing_nodes
        return cls(row.keyspace_name, row.table_name, row.start_token, row.end_token, last_time, last_successful_time, missing_nodes)


def __consolidate(keyspace, table, records):
    """ Consolidate raw records coming from the status table to make them more usable for testing

    Raw records from the status table aren't ideal to use directly for 2 main reasons:
    - There can be overlapping records. This will typically happen after topology changes, but not handling those
    may give a broken view of the state of validation of the ring.
    - There may be holes. There is only records for parts that have been validated, but for testing we generally
    want to make sure there isn't part of the ring that hasn't been validated, so we need to know about those "holes".

    What this method does is transform the list of records to a new more regular record list that
    - will always cover the full ring. Any "hole" will be filled with a record whose last_time and last_successful_time
      are None.
      - won't have overlaps. It detects parts where 2 segments overlap and only keep the most recent validation in the
      final result.

    The exact algorithm uses is basically similar to the `NodeSyncRecord#consolidateValidations` method server side
    which does the same job.
    """
    if len(records) == 0:
        return [NodeSyncRecord(keyspace, table, _MIN_TOKEN, _MIN_TOKEN)]

    # Auxiliary function comparing 2 tokens, assuming the 1st one is a range start and 2nd one a range end
    # The reason we have this and the next one is that the MIN_TOKEN on the right of a range breaks proper
    # comparisons and this abstract this issue somewhat
    def compare_start_end(start, end):
        if end == _MIN_TOKEN:
            return -1
        return -1 if start < end else (0 if start == end else 1)

    # Same as above, but where the 2 tokens are assumed to be range ends
    def compare_end_end(end1, end2):
        if end1 == _MIN_TOKEN and end2 == _MIN_TOKEN:
            return 0
        if end1 == _MIN_TOKEN:
            return 1
        if end2 == _MIN_TOKEN:
            return -1
        return -1 if end1 < end2 else (0 if end1 == end2 else 1)

    def as_sortable(record):
        return (record.start, record.end, record)

    # We use a priority queue that keeps record sorted by their start. We will re-add during the loop below to make
    # things easier, which is why we don't simply sort and call it a day

    records = [as_sortable(record) for record in records]
    heapq.heapify(records)

    result = []
    _, _, curr = heapq.heappop(records)

    # If the first doesn't start at the very beginning of the ring, add what is missing
    if curr.start != _MIN_TOKEN:
        result.append(NodeSyncRecord(keyspace, table, _MIN_TOKEN, curr.start))

    while len(records) > 0:
        _, _, next = heapq.heappop(records)

        startEndCmp = compare_start_end(next.start, curr.end)
        if startEndCmp >= 0:
            # next record starts after the current one. Add current one (and, if there is a gap between curr and next,
            # add that as well) and move to next.
            result.append(curr)
            if startEndCmp > 0:
                result.append(NodeSyncRecord(keyspace, table, curr.end, next.start))
            curr = next
        else:
            # next record intersects with curr on some part. First add part that comes _before_ the intersection if any
            if curr.start < next.start:
                result.append(curr.to_new_end(next.start))

            # then, we'll deal with the intersection of curr and next. We'll deal with the part following that intersection
            # later, so push that first back to the heap
            endEndCmp = compare_end_end(next.end, curr.end)
            if endEndCmp < 0:
                # next ends before curr, push the rest of curr to be dealt later
                heapq.heappush(records, as_sortable(curr.from_new_start(next.end)))
            elif endEndCmp > 0:
                # curr ends before next, push the rest of next to be dealt later
                heapq.heappush(records, as_sortable(next.from_new_start(curr.end)))

            # and then update curr to be the intersection
            missing = (set(curr.missing_nodes) if curr.missing_nodes else set()) | (set(next.missing_nodes) if next.missing_nodes else set())
            curr = NodeSyncRecord(keyspace, table, next.start, next.end if endEndCmp < 0 else curr.end,
                                  last_time=max(curr.last_time, next.last_time),
                                  last_successful_time=max(curr.last_successful_time, next.last_successful_time),
                                  missing_nodes=missing)

    # add the last curr, and if it doesn't cover the end of the ring, adds what's missing
    result.append(curr)
    if curr.end != _MIN_TOKEN:
        result.append(NodeSyncRecord(keyspace, table, curr.end, _MIN_TOKEN))

    return result


def read_nodesync_status(session, keyspace, table, print_debug=False):
    """ Reads in the rows from the nodesync status system table that pertain to a specific data table,
    returning a list of NodeSyncRecord.
    """
    query = SimpleStatement("""SELECT * FROM system_distributed.nodesync_status WHERE keyspace_name='{keyspace}'
                AND table_name='{table}' ALLOW FILTERING""".format(keyspace=keyspace, table=table),
                            fetch_size=None)
    rows = session.execute(query).current_rows
    raw_records = [NodeSyncRecord._from_row(row) for row in rows]
    if print_debug:
        debug("Records = {}".format(raw_records))
    records = __consolidate(keyspace, table, raw_records)
    return records


def validated_since(timestamp, only_success=True):
    """ Predicate for use with wait_for_all_segments that checks all segments have been validated since
    the provided :timestamp.

    If :only_success is set (the default), it will check for successful validation only. Otherwise, it'll check
    any validation.
    """
    return lambda r: (r.last_successful_time if only_success else r.last_time) > timestamp


def not_validated():
    """ Predicate for use with wait_for_all_segments that checks that segments have _not_ been validated.

    Mostly useful to check NodeSync is not running (or at least not on a specific table).
    """
    return lambda r: r.last_time is None and r.last_successful_time is None


def assert_all_segments(session, keyspace, table, timeout=60, predicate=None):
    """ Waits up to :timeout to see if every segment of :keyspace.:table pass the provided :predicate, and fail
    if that is not the case.

    If :predicate is not set, it defaults to one that checks that all segments are validated successfully
    from the point of this method call.
    """
    start = time.time()
    if not predicate:
        predicate = validated_since(start * 1000)
    while start + timeout > time.time():
        nodesync_status = read_nodesync_status(session, keyspace, table)
        if all(predicate(record) for record in nodesync_status):
            debug("All segments matched predicate in {} seconds".format(time.time() - start))
            return
        time.sleep(1)

    failed_records = [record for record in nodesync_status if not predicate(record)]
    assert False, """Was not able to validate all segments within the {timeout}s timeout:
                     {count} failed the predicate (for instance: {ex_failed})""".format(timeout=timeout,
                                                                                        count=len(failed_records),
                                                                                        ex_failed=failed_records[0])


def get_oldest_segments(session, keyspace, table, segment_count, only_success=True):
    """ Returns the :segment_count oldest segments from the nodesync status table that pertain to :keyspace.:table

    If :only_success is set (the default), then it will only rely on the last_successful_time (and so segments
    that have never been successfully validated will be returned first, as being oldest), otherwise last_time
    will be used.
    """
    def sort_record(record):
        time = record.last_successful_time if only_success else record.last_time
        return time if time is not None else -1

    nodesync_status = read_nodesync_status(session, keyspace, table)
    nodesync_status.sort(key=sort_record)
    return nodesync_status[0:segment_count]


def get_unsuccessful_validations(session, keyspace, table):
    """ Returns the set of rows in the nodesync status table for :keyspace.:table where the latest validations
    were unsuccessful.
    """
    nodesync_status = read_nodesync_status(session, keyspace, table)
    return [record for record in nodesync_status if not record.last_was_success]
