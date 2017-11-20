import datetime
import subprocess
import time
from uuid import UUID

from cassandra.util import sortedset
from ccmlib import common

from dtest import Tester, debug, DtestTimeoutError
from tools.assertions import assert_one, assert_all, assert_true
from tools.data import create_ks, rows_to_list
from tools.decorators import no_vnodes, since
from tools.jmxutils import (JolokiaAgent, make_mbean)

NODESYNC_SERVICE_MBEAN = make_mbean('nodesync', 'NodeSyncService', domain='com.datastax')


def _start_nodesync_service(node):
    debug('Starting NodeSync service in node %s' % node)
    with JolokiaAgent(node) as jmx:
        jmx.execute_method(NODESYNC_SERVICE_MBEAN, 'enable')


def _stop_nodesync_service(node):
    debug('Stopping NodeSync service in node %s' % node)
    with JolokiaAgent(node) as jmx:
        jmx.execute_method(NODESYNC_SERVICE_MBEAN, 'disable()')


def _parse_validation_id(stdout):
    lines = filter(lambda l: l.strip(), stdout.splitlines())
    return UUID(lines[-1])


@since('4.0')
class TestNodeSyncTool(Tester):
    """
    Test NodeSync command line tool.
    @jira_ticket APOLLO-948
    """

    def _prepare_cluster(self, nodes=1, rf=1, byteman=False, keyspaces=0, tables_per_keyspace=0, nodesync_enabled=True):
        cluster = self.cluster
        cluster.populate(nodes, install_byteman=byteman).start(wait_for_binary_proto=True)
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self.session = session

        for k in xrange(keyspaces):
            keyspace_name = 'k%d' % (k + 1)
            create_ks(session, keyspace_name, rf)
            for t in xrange(tables_per_keyspace):
                table_name = 't%d' % (t + 1)
                session.execute("CREATE TABLE %s.%s (k int PRIMARY KEY, v int) WITH nodesync = {'enabled':'%s'}"
                                % (keyspace_name, table_name, nodesync_enabled))

        return self.session

    def _start_nodesync_service(self):
        for node in self.cluster.nodelist():
            _start_nodesync_service(node)

    def _stop_nodesync_service(self):
        for node in self.cluster.nodelist():
            _stop_nodesync_service(node)

    def _nodesync(self, args=list(), expected_stdout=None, expected_stderr=None,
                  ignore_stdout_order=False, ignore_stderr_order=False):
        """
        Runs nodesync command line tool with the specified arguments, ensuring the specified expected command output.

        @param args The arguments to be passed to nodesync
        @param expected_stdout A list of text lines that should be contained by the command stdout
        @param expected_stderr A list of text lines that should be contained by the command stderr
        @param ignore_stdout_order If stdout is not expected to be ordered
        @param ignore_stderr_order If stderr is not expected to be ordered
        @return a 2-tuple composed by the stdout and the stderr of the command execution
        """

        # prepare the command
        node = self.cluster.nodelist()[0]
        env = common.make_cassandra_env(node.get_install_cassandra_root(), node.get_node_cassandra_root())
        nodesync_bin = node.get_tool('nodesync')
        full_args = [nodesync_bin] + args
        debug('COMMAND: ' + ' '.join(str(arg) for arg in full_args))

        # run the command
        p = subprocess.Popen(full_args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
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

    def _wait_for_validation(self, uuid, count=1):
        """ Wait until the validation identified by the specified UUID appears on the system table """
        query = "SELECT COUNT(*) FROM system_distributed.nodesync_user_validations WHERE id='{}'".format(uuid)
        start = time.time()
        while time.time() < start + 20:
            if rows_to_list(self.session.execute(query))[0][0] >= count:
                break
            time.sleep(1)
        else:
            raise DtestTimeoutError('Timeout waiting for the validation to be written to the system table')

    def _test_toggle(self, enable=True):
        """
        Test either 'nodesync enable' or 'nodesync disable' complementary commands.

        @param enable If true 'nodesync enable' is tested, else 'nodesync disable' is tested
        """

        def nodesync(args=list(), stdout=None, stderr=None):
            args = ['enable' if enable else 'disable'] + args
            if stdout:
                stdout = map(lambda t: 'Nodesync %s for %s' % ('enabled' if enable else 'disabled', t), stdout)
            self._nodesync(args, stdout, stderr, ignore_stdout_order=True)

        def assert_enabled(keyspace, table, expected):
            query = "SELECT nodesync['enabled'] FROM system_schema.tables WHERE keyspace_name='{}' AND table_name='{}'"
            assert_one(self.session, query.format(keyspace, table), [str(expected).lower()])

        self._prepare_cluster(keyspaces=2, tables_per_keyspace=4, nodesync_enabled=not enable)

        # input errors
        nodesync(['t1'], stderr=['error: Keyspace required for unqualified table name: t1'])
        nodesync(['k0.t1'], stderr=['error: Keyspace [k0] does not exist.'])
        nodesync(['k1.t0'], stderr=['error: Table [k1.t0] does not exist.'])

        # qualified table name
        nodesync(['-v', 'k1.t1'], ['k1.t1'])
        assert_enabled('k1', 't1', enable)
        assert_enabled('k1', 't2', not enable)
        assert_enabled('k1', 't3', not enable)
        assert_enabled('k1', 't4', not enable)
        assert_enabled('k2', 't1', not enable)
        assert_enabled('k2', 't2', not enable)
        assert_enabled('k2', 't3', not enable)
        assert_enabled('k2', 't4', not enable)

        # qualified table name with unused default keyspace
        nodesync(['-v', '-k', 'k2', 'k1.t2'], ['k1.t2'])
        assert_enabled('k1', 't1', enable)
        assert_enabled('k1', 't2', enable)
        assert_enabled('k1', 't3', not enable)
        assert_enabled('k1', 't4', not enable)
        assert_enabled('k2', 't1', not enable)
        assert_enabled('k2', 't2', not enable)
        assert_enabled('k2', 't3', not enable)
        assert_enabled('k2', 't4', not enable)

        # unqualified table name with default keyspace
        nodesync(['-v', '-k', 'k2', 't1'], ['k2.t1'])
        assert_enabled('k1', 't1', enable)
        assert_enabled('k1', 't2', enable)
        assert_enabled('k1', 't3', not enable)
        assert_enabled('k1', 't4', not enable)
        assert_enabled('k2', 't1', enable)
        assert_enabled('k2', 't2', not enable)
        assert_enabled('k2', 't3', not enable)
        assert_enabled('k2', 't4', not enable)

        # qualified and unqualified table names with default keyspace
        nodesync(['-v', '-k', 'k1', 't3', 'k2.t2'], ['k1.t3', 'k2.t2'])
        assert_enabled('k1', 't1', enable)
        assert_enabled('k1', 't2', enable)
        assert_enabled('k1', 't3', enable)
        assert_enabled('k1', 't4', not enable)
        assert_enabled('k2', 't1', enable)
        assert_enabled('k2', 't2', enable)
        assert_enabled('k2', 't3', not enable)
        assert_enabled('k2', 't4', not enable)

        # wildcard with default keyspace
        nodesync(['-v', '-k', 'k1', '*'], ['k1.t1', 'k1.t2', 'k1.t3', 'k1.t4'])
        assert_enabled('k1', 't1', enable)
        assert_enabled('k1', 't2', enable)
        assert_enabled('k1', 't3', enable)
        assert_enabled('k1', 't4', enable)
        assert_enabled('k2', 't1', enable)
        assert_enabled('k2', 't2', enable)
        assert_enabled('k2', 't3', not enable)
        assert_enabled('k2', 't4', not enable)

        # wildcard without default keyspace
        nodesync(['-v', '*'],
                 ['k1.t1', 'k1.t2', 'k1.t3', 'k1.t4', 'k2.t1', 'k2.t2', 'k2.t3', 'k2.t4',
                  'system_distributed.repair_history',
                  'system_distributed.parent_repair_history',
                  'system_distributed.view_build_status',
                  'system_distributed.nodesync_status',
                  'system_distributed.nodesync_user_validations'])
        assert_enabled('k1', 't1', enable)
        assert_enabled('k1', 't2', enable)
        assert_enabled('k1', 't3', enable)
        assert_enabled('k1', 't4', enable)
        assert_enabled('k2', 't1', enable)
        assert_enabled('k2', 't2', enable)
        assert_enabled('k2', 't3', enable)
        assert_enabled('k2', 't4', enable)
        assert_enabled('system_distributed', 'repair_history', enable)
        assert_enabled('system_distributed', 'parent_repair_history', enable)
        assert_enabled('system_distributed', 'view_build_status', enable)
        assert_enabled('system_distributed', 'nodesync_status', enable)
        assert_enabled('system_distributed', 'nodesync_user_validations', enable)

    def test_enable(self):
        """
        Test 'nodesync enable' command.
        """
        self._test_toggle(True)

    def test_disable(self):
        """
        Test 'nodesync disable' command.
        """
        self._test_toggle(False)

    def test_submit(self):
        """
        Test 'nodesync validation submit' command.
        """

        def test(args=list(), expected_stdout=None, expected_stderr=None, should_succeed=False):
            """
            Invoke the command validating the output and, if it should succeed, check that the validation has been
            written in the system table with the generated id.
            """
            stdout, stderr = self._nodesync(['validation', 'submit'] + args, expected_stdout, expected_stderr)
            if should_succeed:
                uuid = _parse_validation_id(stdout)
                self._wait_for_validation(uuid)

        self._prepare_cluster(nodes=3, rf=2, keyspaces=1, tables_per_keyspace=1)

        # test basic input validation
        test(expected_stderr=['error: A qualified table name should be specified'])
        test(['t'], expected_stderr=['error: Keyspace required for unqualified table name: t'])
        test(['k0.t1'], expected_stderr=['error: Keyspace [k0] does not exist.'])
        test(['k1.t0'], expected_stderr=['error: Table [k1.t0] does not exist.'])
        test(['k1.t1', 'a'], expected_stderr=['error: Cannot parse range: a'])
        test(['k1.t1', '(0]'], expected_stderr=['error: Cannot parse range: (0]'])
        test(['k1.t1', '(0,]'], expected_stderr=['error: Cannot parse range: (0,]'])
        test(['k1.t1', '(,0]'], expected_stderr=['error: Cannot parse range: (,0]'])
        test(['k1.t1', '(a,0]'], expected_stderr=['error: Cannot parse token: a'])
        test(['k1.t1', '(0,b]'], expected_stderr=['error: Cannot parse token: b'])
        test(['k1.t1', '(1,2)'], expected_stderr=['error: Invalid input range: (1,2): '
                                                  'only ranges with an open start and closed end are allowed. '
                                                  'Did you meant (1, 2]?'])
        test(['k1.t1', '[1,2]'], expected_stderr=['error: Invalid input range: [1,2]: '
                                                  'only ranges with an open start and closed end are allowed. '
                                                  'Did you meant (1, 2]?'])
        test(['k1.t1', '[1,2)'], expected_stderr=['error: Invalid input range: [1,2): '
                                                  'only ranges with an open start and closed end are allowed. '
                                                  'Did you meant (1, 2]?'])

        # test error message when nodesync is not active
        self._stop_nodesync_service()
        test(['k1.t1', '(1,2]'], expected_stderr=["there are no more replicas to try: "
                                                  "Cannot start user validation, NodeSync is not currently running.",
                                                  'error: Submission failed'])
        self._start_nodesync_service()

        # successful invocations without the verbose flag should produce an empty output
        test(['k1.t1'], should_succeed=True, expected_stdout=[])
        test(['k1.t1', '(1,100]'], should_succeed=True, expected_stdout=[])
        test(['k1.t1', '(1,100]', '(200,300]'], should_succeed=True, expected_stdout=[])

        # successful invocations with the verbose flag should produce an non-empty output
        test(['-v', 'k1.t1'], should_succeed=True, expected_stdout=['submitted for ranges'])
        test(['-v', 'k1.t1', '(1,100]'], should_succeed=True, expected_stdout=['submitted for ranges'])
        test(['-v', 'k1.t1', '(1,100]', '(200,300]'], should_succeed=True, expected_stdout=['submitted for ranges'])

        # stop the second node, we should still be able to submit validations to the whole ring due to replication
        self.cluster.nodelist()[1].stop(wait_other_notice=True)
        test(['-v', 'k1.t1'], should_succeed=True, expected_stdout=['submitted for ranges',
                                                                    'trying next replicas',
                                                                    'submitted for ranges'])

        # stop the third node, validations should be sent to the first node and cancelled after detecting the down nodes
        self.cluster.nodelist()[2].stop(wait_other_notice=True)
        test(['-v', 'k1.t1'],
             expected_stdout=['submitted for ranges',
                              'trying next replicas'],
             expected_stderr=['there are no more replicas to try',
                              'Cancelling validation in those nodes where it was already submitted: [/127.0.0.1]',
                              '/127.0.0.1:',
                              'has been successfully cancelled',
                              'error: Submission failed'])

    @no_vnodes()
    def test_submit_no_vnodes(self):
        """
        Extends `test_submit` taking advantage of the determinism of token range allocation when vnodes are disabled
        to do additional checks on the command output and it's side effects.
        """
        session = self._prepare_cluster(nodes=3, rf=2, keyspaces=1, tables_per_keyspace=1)
        self._start_nodesync_service()

        def test(args=list(), expected_stdout=list(), expected_stderr=list(), expected_rows=None):
            """
            Invoke the command validating the output and check that expected rows have been written in the system table
            with the generated id.
            """
            stdout, stderr = self._nodesync(['validation', 'submit'] + args, expected_stdout, expected_stderr)
            if expected_rows is not None:
                uuid = _parse_validation_id(stdout)
                self._wait_for_validation(uuid, len(expected_rows))
                query = "SELECT node, keyspace_name, table_name, validated_ranges " \
                        "FROM system_distributed.nodesync_user_validations WHERE id='{}'".format(uuid)
                assert_all(session, query, expected_rows)

        # if no range is specified then the whole ring should be validated
        test(args=['k1.t1'],
             expected_rows=[
                 ['127.0.0.1', 'k1', 't1', sortedset(['(-3074457345618258603,-9223372036854775808]'])],
                 ['127.0.0.2', 'k1', 't1', sortedset(['(-9223372036854775808,-3074457345618258603]'])]])

        # validate a single range
        test(args=['k1.t1', '(1,2]'],
             expected_rows=[['127.0.0.1', 'k1', 't1', sortedset(['(1,2]'])]])

        # validate a single range with verbose flag
        test(args=['-v', 'k1.t1', '(1,2]'],
             expected_rows=[['127.0.0.1', 'k1', 't1', sortedset(['(1,2]'])]],
             expected_stdout=['/127.0.0.1: submitted for ranges [(1,2]]'])

        # validate two ranges in the same node with verbose flag
        test(args=['-v', 'k1.t1', '(1,2]', '(3,4]'],
             expected_rows=[['127.0.0.1', 'k1', 't1', sortedset(['(1,2]', '(3,4]'])]],
             expected_stdout=['/127.0.0.1: submitted for ranges [(1,2], (3,4]]'])

        # validate two ranges in different nodes with verbose flag
        test(args=['-v', 'k1.t1', '(1,2]', '(-9000000000000000001,-9000000000000000000]'],
             expected_rows=[['127.0.0.1', 'k1', 't1', sortedset(['(1,2]'])],
                            ['127.0.0.2', 'k1', 't1', sortedset(['(-9000000000000000001,-9000000000000000000]'])]],
             expected_stdout=['/127.0.0.1: submitted for ranges [(1,2]]',
                              '/127.0.0.2: submitted for ranges [(-9000000000000000001,-9000000000000000000]]'])

        # stop the second node
        self.cluster.nodelist()[1].stop(wait_other_notice=True)

        # a single validation for that node should be submitted to the next replica
        test(args=['-v', 'k1.t1', '(-9000000000000000001,-9000000000000000000]'],
             expected_rows=[['127.0.0.3', 'k1', 't1', sortedset(['(-9000000000000000001,-9000000000000000000]'])]],
             expected_stdout=['/127.0.0.2: failed for ranges [(-9000000000000000001,-9000000000000000000]], '
                              'trying next replicas: JMX connection to 127.0.0.2/127.0.0.2:7200 failed',
                              '/127.0.0.3: submitted for ranges [(-9000000000000000001,-9000000000000000000]]'])

        # stop the third node
        self.cluster.nodelist()[2].stop(wait_other_notice=True)

        # a single validation of a range with all its replicas down should fail
        test(args=['-v', 'k1.t1', '(-9000000000000000001,-9000000000000000000]'],
             expected_stdout=['/127.0.0.2: failed for ranges [(-9000000000000000001,-9000000000000000000]], '
                              'trying next replicas: JMX connection to 127.0.0.2/127.0.0.2:7200 failed'],
             expected_stderr=['/127.0.0.3: failed for ranges [(-9000000000000000001,-9000000000000000000]], '
                              'there are no more replicas to try: JMX connection to 127.0.0.3/127.0.0.3:7300',
                              'error: Submission failed'])

        # a validation of two ranges where only one of them fails should be cancelled in the successful node
        test(args=['-v', 'k1.t1', '(1,2]', '(-9000000000000000001,-9000000000000000000]'],
             expected_stdout=['/127.0.0.1: submitted for ranges [(1,2]]',
                              '/127.0.0.2: failed for ranges [(-9000000000000000001,-9000000000000000000]], '
                              'trying next replicas: JMX connection to 127.0.0.2/127.0.0.2:7200 failed'],
             expected_stderr=['/127.0.0.3: failed for ranges [(-9000000000000000001,-9000000000000000000]], '
                              'there are no more replicas to try: JMX connection to 127.0.0.3/127.0.0.3:7300',
                              'Cancelling validation in those nodes where it was already submitted: [/127.0.0.1]',
                              '/127.0.0.1:',
                              'has been successfully cancelled',
                              'error: Submission failed'])

    def test_list(self):
        """
        Test 'nodesync validation list' command.
        """
        session = self._prepare_cluster(nodes=2, rf=2, keyspaces=1, tables_per_keyspace=1)

        class Metrics(object):
            """Validation metrics UDT"""

            def __init__(self, data_validated=0, data_repaired=0):
                self.data_validated = data_validated
                self.data_repaired = data_repaired
                self.objects_validated = 0
                self.objects_repaired = 0
                self.repair_data_sent = 0
                self.repair_objects_sent = 0
                self.pages_outcomes = {}

        session.cluster.register_user_type('system_distributed', 'nodesync_metrics', Metrics)

        query = session.prepare("""INSERT INTO system_distributed.nodesync_user_validations (
                                   id,
                                   node,
                                   keyspace_name,
                                   table_name,
                                   status,
                                   outcomes,
                                   started_at,
                                   ended_at,
                                   segments_to_validate,
                                   segments_validated,
                                   metrics
                                   ) VALUES (?, ?, 'k1', 't1', ?, ?, ?, ?, ?, ?, ?)""")

        def test(list_all=False, records=list(), expected_stdout=list()):

            # insert the validation records
            for record in records:
                session.execute(query, record)

            # run nodesync list
            args = ['validation', 'list']
            if list_all:
                args += ['-a']
            self._nodesync(args, expected_stdout=expected_stdout)

        start = datetime.datetime(2017, 1, 1)
        end = start + datetime.timedelta(hours=1)

        # running but not started
        test(list_all=False,
             records=[
                 ('1', '127.0.0.1', 'running', {}, None, None, 10, 0, Metrics())],
             expected_stdout=['Identifier  Table  Status   Outcome  Duration  ETA  Progress  Validated  Repaired',
                              '1           k1.t1  running  success       0ms    ?        0%         0B        0B'])

        # running with progress (we can't know the duration nor the ETA because they depend on system time)
        test(list_all=False,
             records=[('1', '127.0.0.1', 'running', {'full_in_sync': 1}, start, None, 2, 1, Metrics(2048, 2))],
             expected_stdout=['Identifier  Table  Status   Outcome  ',
                              '1           k1.t1  running  success  '])

        # running without progress (we can't know the duration because it depends on system time)
        test(list_all=False,
             records=[('1', '127.0.0.1', 'running', {'failed': 1}, start, None, 2, 0, Metrics(2048, 2))],
             expected_stdout=['  ETA  Progress  Validated  Repaired',
                              '    ?        0%        2kB        2B'])

        # successful
        test(list_all=True,
             records=[('1', '127.0.0.1', 'successful', {'full_in_sync': 1}, start, end, 2, 2, Metrics(1024, 2))],
             expected_stdout=['Identifier  Table  Status      Outcome  Duration  ETA  Progress  Validated  Repaired',
                              '1           k1.t1  successful  success        1h    -      100%        1kB        2B'])

        # cancelled
        test(list_all=True,
             records=[('1', '127.0.0.1', 'cancelled', {'full_in_sync': 1}, start, end, 4, 1, Metrics(10, 2))],
             expected_stdout=['Identifier  Table  Status     Outcome  Duration  ETA  Progress  Validated  Repaired',
                              '1           k1.t1  cancelled  success        1h    -       25%        10B        2B'])

        # failed
        test(list_all=True,
             records=[('1', '127.0.0.1', 'failed', {'partial_repaired': 1}, start, end, 5, 1, Metrics(10, 2))],
             expected_stdout=['Identifier  Table  Status  Outcome  Duration  ETA  Progress  Validated  Repaired',
                              '1           k1.t1  failed  partial        1h    -       20%        10B        2B'])

        # combine multiple validations
        test(list_all=True,
             records=[('1', '127.0.0.1', 'successful', {'full_in_sync': 1}, start, end, 1, 1, Metrics(10, 2)),
                      ('1', '127.0.0.2', 'successful', {'partial_in_sync': 1}, start, end, 1, 1, Metrics(30, 4)),
                      ('2', '127.0.0.1', 'successful', {'partial_in_sync': 1}, start, end, 1, 1, Metrics(10, 2)),
                      ('2', '127.0.0.2', 'successful', {'failed': 1}, start, end, 1, 1, Metrics(10, 2)),
                      ('3', '127.0.0.1', 'successful', {'full_in_sync': 1}, start, end, 15, 10, Metrics(10, 2)),
                      ('3', '127.0.0.2', 'failed', {'failed': 1}, start, end, 5, 0, Metrics(1024, 2))],
             expected_stdout=['Identifier  Table  Status      Outcome  Duration  ETA  Progress  Validated  Repaired',
                              '1           k1.t1  successful  partial        1h    -      100%        40B        6B',
                              '2           k1.t1  successful  failed         1h    -      100%        20B        4B',
                              '3           k1.t1  failed      failed         1h    -       50%        1kB        4B'])

    def test_cancel(self):
        """
        Test 'nodesync validation cancel' command.
        """
        self._prepare_cluster(nodes=2, rf=2, byteman=True, keyspaces=1, tables_per_keyspace=1)
        node1, node2 = self.cluster.nodelist()
        self._start_nodesync_service()

        def submit_validation():
            stdout, stderr = self._nodesync(['validation', 'submit', 'k1.t1'])
            _uuid = _parse_validation_id(stdout)
            self._wait_for_validation(_uuid)
            return _uuid

        # use byteman to keep the validations running for a while
        for node in self.cluster.nodelist():
            node.byteman_submit(['./byteman/user_validation_sleep.btm'])

        # try to cancel non existing validation
        self._nodesync(args=['validation', 'cancel', '-v', 'not_existent_id'],
                       expected_stderr=["error: The validation to be cancelled hasn't been found in any node"])

        # successfully cancel a validation
        uuid = submit_validation()
        self._nodesync(args=['validation', 'cancel', '-v', str(uuid)],
                       expected_stdout=['/127.0.0.1: Cancelled',
                                        'The validation has been cancelled in nodes [/127.0.0.1]'])

        # try to cancel an already cancelled validation
        self._nodesync(args=['validation', 'cancel', '-v', str(uuid)],
                       expected_stderr=["error: The validation to be cancelled hasn't been found in any node"])

        # try to cancel a validation residing on a down node
        uuid = submit_validation()
        node1.stop(wait_other_notice=True)  # will remove its proposers
        self._nodesync(args=['-h', '127.0.0.2', 'validation', 'cancel', '-v', str(uuid)],
                       expected_stderr=["error: The validation to be cancelled hasn't been found in any node"])

    def test_quoted_arg_with_spaces(self):
        """
        @jira_ticket APOLLO-1183

        Fixes bug in handling of quoted parameters.
        """

        cluster = self.cluster
        cluster.populate([1]).start()
        node = cluster.nodelist()[0]

        # Execute some nodesync command with spaces in an argument.
        # The command itself doesn't make sense, but that's not the point of this test.
        self._nodesync(args=['validation', 'cancel', '-v', 'this id does not exist'],
                       expected_stderr=["error: The validation to be cancelled hasn't been found in any node"])
