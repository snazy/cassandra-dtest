import datetime
import os
import re
import time
from uuid import UUID

from cassandra.util import sortedset
from ccmlib.node import ToolError

from dtest import DtestTimeoutError, Tester, debug, keystore_type_desc, trusttore_type_desc
from tools.assertions import assert_all, assert_one
from tools.data import create_ks, rows_to_list
from tools.decorators import no_vnodes, since
from tools.nodesync import nodesync_tool
from tools.jmxutils import enable_jmx_ssl
from tools.sslkeygen import generate_ssl_stores


def _parse_validation_id(stdout):
    lines = filter(lambda l: l.strip(), stdout.splitlines())
    return UUID(lines[-1])


@since('4.0')
class TestNodeSyncTool(Tester):
    """
    Test NodeSync command line tool.
    @jira_ticket APOLLO-948
    """

    nodetool_ssl_args = list()
    nodesync_ssl_args = list()

    def _prepare_cluster(self, nodes=1, rf=1, byteman=False, keyspaces=0, tables_per_keyspace=0, nodesync_enabled=True,
                         cql_ssl=False, jmx_ssl=True, require_client_auth=False):

        cluster = self.cluster
        cluster.populate(nodes, install_byteman=byteman)

        ssl_opts = None
        if cql_ssl or jmx_ssl:

            # Generate SSL stores
            generate_ssl_stores(self.test_path)
            password = 'cassandra'
            keystore = os.path.join(self.test_path, keystore_type_desc().getFileName())
            truststore = os.path.join(self.test_path, trusttore_type_desc().getFileName())

            # Prepare JVM properties
            jvm_args = ['-Djavax.net.ssl.trustStore=%s' % truststore,
                        '-Djavax.net.ssl.trustStorePassword=%s' % password]
            if require_client_auth:
                jvm_args += ['-Djavax.net.ssl.keyStore=%s' % keystore,
                             '-Djavax.net.ssl.keyStorePassword=%s' % password]

            # Prepare node connection arguments
            self.nodetool_ssl_args = (['--ssl'] + jvm_args) if jmx_ssl else list()
            self.nodesync_ssl_args = (['--cql-ssl'] if cql_ssl else list()) + \
                                     (['--jmx-ssl'] if jmx_ssl else list()) + jvm_args

            # Prepare Python driver SSL options
            if cql_ssl:
                certificate = os.path.join(self.test_path, 'ccm_node.cer')
                ssl_opts = {'ca_certs': certificate}
                if require_client_auth:
                    key = os.path.join(self.test_path, 'ccm_node.key')
                    ssl_opts.update({'keyfile': key, 'certfile': certificate})

            # Enable server-side encryption
            for node in cluster.nodelist():
                if cql_ssl:
                    node.set_configuration_options({
                        'client_encryption_options': {
                            'enabled': 'true',
                            'keystore': keystore,
                            'keystore_password': password,
                            'require_client_auth': require_client_auth,
                            'truststore': truststore,
                            'truststore_password': password
                        }
                    })
                if jmx_ssl:
                    enable_jmx_ssl(node,
                                   keystore=keystore,
                                   keystore_password=password,
                                   require_client_auth=require_client_auth,
                                   truststore=truststore,
                                   truststore_password=password)

        cluster.start(wait_for_binary_proto=True)
        node1 = self.cluster.nodelist()[0]
        timeout = 180 if require_client_auth else 30  # client authentication can produce timeouts
        session = self.patient_cql_connection(node1, ssl_opts=ssl_opts, timeout=timeout)
        self.session = session

        for k in xrange(keyspaces):
            keyspace_name = 'k%d' % (k + 1)
            create_ks(session, keyspace_name, rf)
            for t in xrange(tables_per_keyspace):
                table_name = 't%d' % (t + 1)
                session.execute("CREATE TABLE %s.%s (k int PRIMARY KEY, v int) WITH nodesync = {'enabled':'%s'}"
                                % (keyspace_name, table_name, nodesync_enabled))

        return self.session

    def _restart_cluster(self, gently=True):
        self.cluster.stop(gently=gently)
        self.cluster.start(wait_for_binary_proto=True)
        node1 = self.cluster.nodelist()[0]
        self.session = self.patient_cql_connection(node1)
        return self.session

    def _nodesync(self, args=list(), expected_stdout=None, expected_stderr=None,
                  ignore_stdout_order=False, ignore_stderr_order=False, print_debug=True):
        """
        Runs NodeSync tool with the connection arguments prepared by a previous call to `_prepare_cluster`
        """
        return nodesync_tool(cluster=self.cluster,
                             args=self.nodesync_ssl_args + args,
                             expected_stdout=expected_stdout,
                             expected_stderr=expected_stderr,
                             ignore_stdout_order=ignore_stdout_order,
                             ignore_stderr_order=ignore_stderr_order,
                             print_debug=print_debug)

    def _nodetool(self, node, cmd):
        """
        Runs nodetool with the connection arguments prepared by a previous call to `_prepare_cluster`
        """
        return node.nodetool(' '.join([cmd] + self.nodetool_ssl_args))

    def _start_nodesync_service(self):
        for node in self.cluster.nodelist():
            self._nodetool(node, 'nodesyncservice enable')

    def _stop_nodesync_service(self):
        for node in self.cluster.nodelist():
            self._nodetool(node, 'nodesyncservice disable')

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

    def _wait_for_validation_status(self, uuid, node, status, timeout=30):
        """
        Wait until the validation identified by the specified UUID and node appears on the system table with the
        specified status.
        """
        query = "SELECT status FROM system_distributed.nodesync_user_validations " \
                "WHERE id='{}' AND node='{}'".format(uuid, node.address())
        start = time.time()
        while time.time() < start + timeout:
            try:
                self.assertEqual(rows_to_list(self.session.execute(query)), [[status]])
                break
            except AssertionError:
                time.sleep(1)
        else:
            raise DtestTimeoutError('Timeout waiting for the validation status to be written to the system table')

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
        nodesync(['t1'], stderr=['Error: Keyspace required for unqualified table name: t1'])
        nodesync(['k0.t1'], stderr=['Error: Keyspace \[k0\] does not exist\.'])
        nodesync(['k1.t0'], stderr=['Error: Table \[k1\.t0\] does not exist\.'])

        # qualified table name
        nodesync(['-v', 'k1.t1'], ['k1\.t1'])
        assert_enabled('k1', 't1', enable)
        assert_enabled('k1', 't2', not enable)
        assert_enabled('k1', 't3', not enable)
        assert_enabled('k1', 't4', not enable)
        assert_enabled('k2', 't1', not enable)
        assert_enabled('k2', 't2', not enable)
        assert_enabled('k2', 't3', not enable)
        assert_enabled('k2', 't4', not enable)

        # qualified table name with unused default keyspace
        nodesync(['-v', '-k', 'k2', 'k1.t2'], ['k1\.t2'])
        assert_enabled('k1', 't1', enable)
        assert_enabled('k1', 't2', enable)
        assert_enabled('k1', 't3', not enable)
        assert_enabled('k1', 't4', not enable)
        assert_enabled('k2', 't1', not enable)
        assert_enabled('k2', 't2', not enable)
        assert_enabled('k2', 't3', not enable)
        assert_enabled('k2', 't4', not enable)

        # unqualified table name with default keyspace
        nodesync(['-v', '-k', 'k2', 't1'], ['k2\.t1'])
        assert_enabled('k1', 't1', enable)
        assert_enabled('k1', 't2', enable)
        assert_enabled('k1', 't3', not enable)
        assert_enabled('k1', 't4', not enable)
        assert_enabled('k2', 't1', enable)
        assert_enabled('k2', 't2', not enable)
        assert_enabled('k2', 't3', not enable)
        assert_enabled('k2', 't4', not enable)

        # qualified and unqualified table names with default keyspace
        nodesync(['-v', '-k', 'k1', 't3', 'k2.t2'], ['k1\.t3', 'k2\.t2'])
        assert_enabled('k1', 't1', enable)
        assert_enabled('k1', 't2', enable)
        assert_enabled('k1', 't3', enable)
        assert_enabled('k1', 't4', not enable)
        assert_enabled('k2', 't1', enable)
        assert_enabled('k2', 't2', enable)
        assert_enabled('k2', 't3', not enable)
        assert_enabled('k2', 't4', not enable)

        # wildcard with default keyspace
        nodesync(['-v', '-k', 'k1', '*'], ['k1\.t1', 'k1\.t2', 'k1\.t3', 'k1\.t4'])
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
                 ['k1\.t1', 'k1\.t2', 'k1\.t3', 'k1\.t4', 'k2\.t1', 'k2\.t2', 'k2\.t3', 'k2\.t4',
                  'system_distributed\.repair_history',
                  'system_distributed\.parent_repair_history',
                  'system_distributed\.view_build_status',
                  'system_distributed\.nodesync_status',
                  'system_distributed\.nodesync_user_validations'])
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

    def test_enable_nodesync(self):
        """
        Test 'nodesync enable' command.
        """
        self._test_toggle(True)

    def test_disable_nodesync(self):
        """
        Test 'nodesync disable' command.
        """
        self._test_toggle(False)

    def test_submit_user_validation(self):
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
        test(expected_stderr=['Error: A qualified table name should be specified'])
        test(['t'], expected_stderr=['Error: Keyspace required for unqualified table name: t'])
        test(['k0.t1'], expected_stderr=['Error: Keyspace \[k0\] does not exist\.'])
        test(['k1.t0'], expected_stderr=['Error: Table \[k1\.t0\] does not exist\.'])
        test(['k1.t1', 'a'], expected_stderr=['Error: Cannot parse range: a'])
        test(['k1.t1', '(0]'], expected_stderr=['Error: Cannot parse range: \(0\]'])
        test(['k1.t1', '(0,]'], expected_stderr=['Error: Cannot parse range: \(0,\]'])
        test(['k1.t1', '(,0]'], expected_stderr=['Error: Cannot parse range: \(,0\]'])
        test(['k1.t1', '(a,0]'], expected_stderr=['Error: Cannot parse token: a'])
        test(['k1.t1', '(0,b]'], expected_stderr=['Error: Cannot parse token: b'])
        test(['k1.t1', '(1,2)'], expected_stderr=['Error: Invalid input range: \(1,2\): '
                                                  'only ranges with an open start and closed end are allowed\. '
                                                  'Did you meant \(1, 2\]?'])
        test(['k1.t1', '[1,2]'], expected_stderr=['Error: Invalid input range: \[1,2\]: '
                                                  'only ranges with an open start and closed end are allowed\. '
                                                  'Did you meant \(1, 2\]?'])
        test(['k1.t1', '[1,2)'], expected_stderr=['Error: Invalid input range: \[1,2\): '
                                                  'only ranges with an open start and closed end are allowed\. '
                                                  'Did you meant \(1, 2\]?'])

        # test error message when nodesync is not active
        self._stop_nodesync_service()
        test(['k1.t1', '(1,2]'], expected_stderr=["there are no more replicas to try: "
                                                  "Cannot start user validation, NodeSync is not currently running\.",
                                                  'Error: Submission failed'])
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
                              'Cancelling validation in those nodes where it was already submitted: [\[\{]/127\.0\.0\.1[\]\}]',
                              '/127\.0\.0\.1:',
                              'has been successfully cancelled',
                              'Error: Submission failed'])

    @no_vnodes()
    def test_submit_user_validation_no_vnodes(self):
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
             expected_stdout=['/127.0.0.1: submitted for ranges \[\(1,2\]\]'])

        # validate two ranges in the same node with verbose flag
        test(args=['-v', 'k1.t1', '(1,2]', '(3,4]'],
             expected_rows=[['127.0.0.1', 'k1', 't1', sortedset(['(1,2]', '(3,4]'])]],
             expected_stdout=['/127.0.0.1: submitted for ranges \[\(1,2\], \(3,4\]\]'])

        # validate two ranges in different nodes with verbose flag
        test(args=['-v', 'k1.t1', '(1,2]', '(-9000000000000000001,-9000000000000000000]'],
             expected_rows=[['127.0.0.1', 'k1', 't1', sortedset(['(1,2]'])],
                            ['127.0.0.2', 'k1', 't1', sortedset(['(-9000000000000000001,-9000000000000000000]'])]],
             expected_stdout=['/127\.0\.0\.1: submitted for ranges \[\(1,2\]\]',
                              '/127\.0\.0\.2: submitted for ranges \[\(-9000000000000000001,-9000000000000000000\]\]'])

        # stop the second node
        self.cluster.nodelist()[1].stop(wait_other_notice=True)

        # a single validation for that node should be submitted to the next replica
        test(args=['-v', 'k1.t1', '(-9000000000000000001,-9000000000000000000]'],
             expected_rows=[['127.0.0.3', 'k1', 't1', sortedset(['(-9000000000000000001,-9000000000000000000]'])]],
             expected_stdout=['/127\.0\.0\.2: failed for ranges \[\(-9000000000000000001,-9000000000000000000\]\], '
                              'trying next replicas: JMX connection to 127\.0\.0\.2:7200 failed',
                              '/127\.0\.0\.3: submitted for ranges \[\(-9000000000000000001,-9000000000000000000\]\]'])

        # stop the third node
        self.cluster.nodelist()[2].stop(wait_other_notice=True)

        # a single validation of a range with all its replicas down should fail
        test(args=['-v', 'k1.t1', '(-9000000000000000001,-9000000000000000000]'],
             expected_stdout=['/127\.0\.0\.2: failed for ranges \[\(-9000000000000000001,-9000000000000000000\]\], '
                              'trying next replicas: JMX connection to 127\.0\.0\.2:7200 failed'],
             expected_stderr=['/127\.0\.0\.3: failed for ranges \[\(-9000000000000000001,-9000000000000000000\]\], '
                              'there are no more replicas to try: JMX connection to 127\.0\.0\.3:7300',
                              'Error: Submission failed'])

        # a validation of two ranges where only one of them fails should be cancelled in the successful node
        test(args=['-v', 'k1.t1', '(1,2]', '(-9000000000000000001,-9000000000000000000]'],
             expected_stdout=['/127\.0\.0\.1: submitted for ranges \[\(1,2\]\]',
                              '/127\.0\.0\.2: failed for ranges \[\(-9000000000000000001,-9000000000000000000\]\], '
                              'trying next replicas: JMX connection to 127\.0\.0\.2:7200 failed'],
             expected_stderr=['/127\.0\.0\.3: failed for ranges \[\(-9000000000000000001,-9000000000000000000\]\], '
                              'there are no more replicas to try: JMX connection to 127\.0\.0\.3:7300',
                              'Cancelling validation in those nodes where it was already submitted: [\{\[]/127.0.0.1[\}\]]',
                              '/127\.0\.0\.1:',
                              'has been successfully cancelled',
                              'Error: Submission failed'])

    def test_submit_user_validation_with_rate(self):
        """
        Test 'nodesync validation submit' command with '-r/--rate' option.
        @jira_ticket APOLLO-1271
        """
        self._prepare_cluster(nodes=2, rf=2, byteman=True, keyspaces=1, tables_per_keyspace=1)
        node = self.cluster.nodelist()[0]

        def submit(rate=None, status_to_wait_for=None):
            """
            param rate The validation rate in KB/s
            param status_to_wait_for The user validation status to wait for, possible values are `running`,
            `successful`, `cancelled` and `failed`
            """
            args = ['validation', 'submit']
            if rate:
                args.extend(['-r', str(rate)])
            args.append('k1.t1')
            stdout, stderr = self._nodesync(args)
            uuid = _parse_validation_id(stdout)
            if status_to_wait_for:
                self._wait_for_validation_status(uuid, node, status_to_wait_for)
            return uuid

        def cancel(uuid):
            self._nodesync(args=['validation', 'cancel', str(uuid)])
            self._wait_for_validation_status(uuid, node, 'cancelled')

        def get_rate(expected):
            self.assertIn('{} KB/s'.format(expected), self._nodetool(node, 'nodesyncservice getrate').stdout)

        def set_rate(rate, conflicting_validation=None):
            command = 'nodesyncservice setrate {}'.format(rate)
            if conflicting_validation:
                try:
                    self._nodetool(node, command)
                except ToolError as e:
                    msg = 'Cannot set NodeSync rate because a user triggered validation with custom rate is running: {}'
                    self.assertIn(msg.format(conflicting_validation), e.message)
            else:
                self._nodetool(node, command)
                get_rate(expected=rate)

        set_rate(rate=1000)

        # submit with rate and verify that, at termination, the original rate is restored and we can set a new rate
        submit(rate=1, status_to_wait_for='successful')
        get_rate(expected=1000)
        set_rate(rate=2000)

        # use byteman to keep the validations running for a while
        node.byteman_submit(['./byteman/user_validation_sleep.btm'])

        # submit without rate and verify that, while running, the original rate is kept and we can set a new rate
        submit(rate=None, status_to_wait_for='running')
        get_rate(expected=2000)
        set_rate(rate=3000)

        # submit with rate and verify that the new rate is not applied and we can still set the rate
        # because the previous validation hasn't finished yet
        submit(rate=2, status_to_wait_for='running')
        get_rate(expected=3000)
        set_rate(rate=4000)

        # restart the cluster to get rid of byteman and the slowed down validations
        self._restart_cluster(gently=False)

        # submit a bunch of validations and check that the rate is restored when all of them have finished
        set_rate(rate=5000)
        for uuid in map(lambda x: submit(rate=x), xrange(10)):
            self._wait_for_validation_status(uuid, node, 'successful', timeout=180)
        get_rate(expected=5000)
        set_rate(rate=6000)

        # use byteman to keep the validations running for a while
        node.byteman_submit(['./byteman/user_validation_sleep.btm'])

        # submit with rate and verify that, while running, the validation rate is applied and we can't set a new rate
        uuid4 = submit(rate=3, status_to_wait_for='running')
        get_rate(expected=3)
        set_rate(rate=7000, conflicting_validation=uuid4)

        # cancel the running validation and verify that the rate is restored
        cancel(uuid4)
        get_rate(expected=6000)

    def test_list_user_validations(self):
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
                              '1           k1\.t1  running  success       0ms    \?        0%         0B        0B'])

        # running with progress (we can't know the duration nor the ETA because they depend on system time)
        test(list_all=False,
             records=[('1', '127.0.0.1', 'running', {'full_in_sync': 1}, start, None, 2, 1, Metrics(2048, 2))],
             expected_stdout=['Identifier  Table  Status   Outcome  ',
                              '1           k1\.t1  running  success  '])

        # running without progress (we can't know the duration because it depends on system time)
        test(list_all=False,
             records=[('1', '127.0.0.1', 'running', {'failed': 1}, start, None, 2, 0, Metrics(2048, 2))],
             expected_stdout=['  ETA  Progress  Validated  Repaired',
                              '    \?        0%        2kB        2B'])

        # successful
        test(list_all=True,
             records=[('1', '127.0.0.1', 'successful', {'full_in_sync': 1}, start, end, 2, 2, Metrics(1024, 2))],
             expected_stdout=['Identifier  Table  Status      Outcome  Duration  ETA  Progress  Validated  Repaired',
                              '1           k1\.t1  successful  success        1h    -      100%        1kB        2B'])

        # cancelled
        test(list_all=True,
             records=[('1', '127.0.0.1', 'cancelled', {'full_in_sync': 1}, start, end, 4, 1, Metrics(10, 2))],
             expected_stdout=['Identifier  Table  Status     Outcome  Duration  ETA  Progress  Validated  Repaired',
                              '1           k1\.t1  cancelled  success        1h    -       25%        10B        2B'])

        # failed
        test(list_all=True,
             records=[('1', '127.0.0.1', 'failed', {'partial_repaired': 1}, start, end, 5, 1, Metrics(10, 2))],
             expected_stdout=['Identifier  Table  Status  Outcome  Duration  ETA  Progress  Validated  Repaired',
                              '1           k1\.t1  failed  partial        1h    -       20%        10B        2B'])

        # combine multiple validations
        test(list_all=True,
             records=[('1', '127.0.0.1', 'successful', {'full_in_sync': 1}, start, end, 1, 1, Metrics(10, 2)),
                      ('1', '127.0.0.2', 'successful', {'partial_in_sync': 1}, start, end, 1, 1, Metrics(30, 4)),
                      ('2', '127.0.0.1', 'successful', {'partial_in_sync': 1}, start, end, 1, 1, Metrics(10, 2)),
                      ('2', '127.0.0.2', 'successful', {'failed': 1}, start, end, 1, 1, Metrics(10, 2)),
                      ('3', '127.0.0.1', 'successful', {'full_in_sync': 1}, start, end, 15, 10, Metrics(10, 2)),
                      ('3', '127.0.0.2', 'failed', {'failed': 1}, start, end, 5, 0, Metrics(1024, 2))],
             expected_stdout=['Identifier  Table  Status      Outcome  Duration  ETA  Progress  Validated  Repaired',
                              '1           k1\.t1  successful  partial        1h    -      100%        40B        6B',
                              '2           k1\.t1  successful  failed         1h    -      100%        20B        4B',
                              '3           k1\.t1  failed      failed         1h    -       50%        1kB        4B'])

    def test_cancel_user_validation(self):
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
                       expected_stderr=["Error: The validation to be cancelled hasn't been found in any node"])

        # successfully cancel a validation
        uuid = submit_validation()
        self._nodesync(args=['validation', 'cancel', '-v', str(uuid)],
                       expected_stdout=['/127.0.0.1: Cancelled',
                                        'The validation has been cancelled in nodes [\[\{]/127\.0\.0\.1[\]\}]'])

        # try to cancel an already cancelled validation
        self._nodesync(args=['validation', 'cancel', '-v', str(uuid)],
                       expected_stderr=["Error: The validation to be cancelled hasn't been found in any node"])

        # try to cancel a validation residing on a down node that gets unnoticed by the driver
        uuid = submit_validation()
        node1.stop(wait_other_notice=True, gently=False)  # will remove its proposers
        self._nodesync(args=['-h', '127.0.0.2', 'validation', 'cancel', '-v', str(uuid)],
                       expected_stderr=['Error: The cancellation has failed in nodes: [\[\{]/127\.0\.0\.1[\]\}]'])

    def test_quoted_arg_with_spaces(self):
        """
        @jira_ticket APOLLO-1183

        Fixes bug in handling of quoted parameters.
        """
        self._prepare_cluster()

        # Execute some nodesync command with spaces in an argument.
        # The command itself doesn't make sense, but that's not the point of this test.
        self._nodesync(args=['validation', 'cancel', '-v', 'this id does not exist'],
                       expected_stderr=["Error: The validation to be cancelled hasn't been found in any node"])

    def test_tracing_nofollow(self):
        """
        Test enabling tracing, disabling it and checking the result.
        """
        self._prepare_cluster(nodes=2, rf=2, keyspaces=1, tables_per_keyspace=1)
        self._start_nodesync_service()

        stdout, _ = self._nodesync(['tracing', 'enable'],
                                   expected_stderr=[
                                       "Warning: Do not forget to stop tracing with 'nodesync tracing disable'\."])

        match = re.compile('Session id is ([a-z0-9-]+)').search(stdout)
        self.assertIsNotNone(match)
        id = match.group(1)

        self._nodesync(['tracing', 'status'], expected_stdout=["Tracing is enabled on all nodes"])
        self._nodesync(['tracing', 'disable'])
        self._nodesync(['tracing', 'status'], expected_stdout=["Tracing is disabled on all nodes"])

        stdout, _ = self._nodesync(['tracing', 'show', '-i ' + id])
        for node in self.cluster.nodelist():
            self.assertIn("Starting NodeSync tracing on /{}".format(node.address()), stdout)
        for node in self.cluster.nodelist():
            self.assertIn("Stopped NodeSync tracing on /{}".format(node.address()), stdout)

    def test_tracing_follow(self):
        """
        Test enabling tracing with the follow option and a timeout.
        """
        self._prepare_cluster(nodes=2, rf=2, keyspaces=1, tables_per_keyspace=1)
        self._start_nodesync_service()

        start = time.time()
        stdout, _ = self._nodesync(['tracing', 'enable', '--follow', '-t 3'])
        # Note that we print the duration for fun, but I don't think we rely on that number
        # in any meaningful way because it account for JMX initialization that the tool
        # does and that is very very slow (so the duration will be a lot longer than 3 seconds)
        debug("duration: {}".format(time.time() - start))

        for node in self.cluster.nodelist():
            self.assertIn("Starting NodeSync tracing on /{}".format(node.address()), stdout)
        for node in self.cluster.nodelist():
            self.assertIn("Stopped NodeSync tracing on /{}".format(node.address()), stdout)

        self._nodesync(['tracing', 'status'], expected_stdout=["Tracing is disabled on all nodes"])

    def test_tracing_node_subset(self):
        """
        Test enabling tracing, disabling it and checking the result on only a subset of nodes.
        """
        self._prepare_cluster(nodes=2, rf=2, keyspaces=1, tables_per_keyspace=1)
        self._start_nodesync_service()

        [node1, node2] = self.cluster.nodelist()
        stdout, _ = self._nodesync(['tracing', 'enable', '-n ' + node1.address()],
                                   expected_stderr=[
                                       "Warning: Do not forget to stop tracing with 'nodesync tracing disable'\."])

        match = re.compile('Session id is ([a-z0-9-]+)').search(stdout)
        self.assertIsNotNone(match)
        id = match.group(1)

        self._nodesync(['tracing', 'status'],
                       expected_stdout=["Tracing is only enabled on \[/{}\]".format(node1.address())])
        self._nodesync(['tracing', 'status', '-n ' + node1.address()], expected_stdout=["Tracing is enabled"])
        self._nodesync(['tracing', 'disable'])
        self._nodesync(['tracing', 'status'], expected_stdout=["Tracing is disabled on all nodes"])

        stdout, _ = self._nodesync(['tracing', 'show', '-i ' + id])

        self.assertIn("Starting NodeSync tracing on /{}".format(node1.address()), stdout)
        self.assertIn("Stopped NodeSync tracing on /{}".format(node1.address()), stdout)

        self.assertNotIn("Starting NodeSync tracing on /{}".format(node2.address()), stdout)
        self.assertNotIn("Stopped NodeSync tracing on /{}".format(node2.address()), stdout)

    def test_disabled_jmx(self):
        """
        Tests that commands requiring JMX show a meaningful error if JMX is not enabled in a target node
        @jira_ticket DB-1710
        :return:
        """
        self._prepare_cluster(keyspaces=1, tables_per_keyspace=1, nodesync_enabled=True)
        self.session.execute("DELETE jmx_port FROM system.local WHERE key='local'")
        self._nodesync(['validation', 'submit', 'k1.t1', '(0,1]'],
                       expected_stderr=['/127\.0\.0\.1: failed for ranges \[\(0,1\]\], '
                                        'there are no more replicas to try: '
                                        'Unable to read the JMX port of node 127\.0\.0\.1, '
                                        'this could be because JMX is not enabled in that node '
                                        'or it\'s running a version without NodeSync support'])

    def test_jmx_ssl_without_client_auth(self):
        """
        Test JMX connectivity with SSL encryption without client authentication required
        """
        self._test_jmx_ssl(require_client_auth=False)

    def test_jmx_ssl_with_client_auth(self):
        """
        Test JMX connectivity with SSL encryption with client authentication required
        """
        self._test_jmx_ssl(require_client_auth=True)

    def _test_jmx_ssl(self, require_client_auth=False):
        """
        Test JMX connectivity with SSL encryption
        :param require_client_auth: if client authentication is required by the server
        """
        self._prepare_cluster(nodes=1, rf=2, keyspaces=1, tables_per_keyspace=1,
                              cql_ssl=False, jmx_ssl=True, require_client_auth=require_client_auth)

        cluster = self.cluster
        keystore = os.path.join(self.test_path, keystore_type_desc().getFileName())
        truststore = os.path.join(self.test_path, trusttore_type_desc().getFileName())
        ssl_args = list()
        args = ['validation', 'submit', 'k1.t1', '(1,2]']

        # without any encryption arguments
        nodesync_tool(cluster, ssl_args + args,
                      expected_stderr=['non-JRMP server at remote endpoint', 'Error: Submission failed'])

        # encryption flag
        ssl_args += ['--jmx-ssl']
        nodesync_tool(cluster, ssl_args + args,
                      expected_stderr=['unable to find valid certification', 'Error: Submission failed'])

        # encryption flag and trust store
        ssl_args += ['-Djavax.net.ssl.trustStore=%s' % truststore, '-Djavax.net.ssl.trustStorePassword=cassandra']
        nodesync_tool(cluster, ssl_args + args,
                      expected_stderr=['bad_certificate', 'Error: Submission failed'] if require_client_auth else None)

        #  encryption flag, trust store and key store
        ssl_args += ['-Djavax.net.ssl.keyStore=%s' % keystore, '-Djavax.net.ssl.keyStorePassword=cassandra']
        nodesync_tool(cluster, ssl_args + args)

    def test_cql_ssl_without_client_auth(self):
        """
        Test CQL connectivity with SSL encryption without client authentication required
        """
        self._test_cql_ssl(require_client_auth=False)

    def test_cql_ssl_with_client_auth(self):
        """
        Test CQL connectivity with SSL encryption with client authentication required
        """
        self._test_cql_ssl(require_client_auth=True)

    def _test_cql_ssl(self, require_client_auth=False):
        """
        Test CQL connectivity with SSL encryption
        :param require_client_auth: if client authentication is required by the server
        """
        self._prepare_cluster(nodes=1, cql_ssl=True, jmx_ssl=False, require_client_auth=require_client_auth)

        cluster = self.cluster
        keystore = os.path.join(self.test_path, keystore_type_desc().getFileName())
        truststore = os.path.join(self.test_path, keystore_type_desc().getFileName())
        ssl_args = list()
        args = ['validation', 'list']

        # without any encryption arguments
        nodesync_tool(cluster, ssl_args + args, expected_stderr=['Connection has been closed'])

        # encryption flag
        ssl_args += ['--cql-ssl']
        nodesync_tool(cluster, ssl_args + args, expected_stderr=['Error writing'])

        # encryption flag and trust store
        ssl_args += ['-Djavax.net.ssl.trustStore=%s' % truststore, '-Djavax.net.ssl.trustStorePassword=cassandra']
        nodesync_tool(cluster, ssl_args + args,
                      expected_stderr=['TransportException'] if require_client_auth else None)

        #  encryption flag, trust store and key store
        ssl_args += ['-Djavax.net.ssl.keyStore=%s' % keystore, '-Djavax.net.ssl.keyStorePassword=cassandra']
        nodesync_tool(cluster, ssl_args + args)
