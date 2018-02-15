import time
import uuid
from distutils.version import LooseVersion

from dse import ConsistencyLevel, WriteFailure, WriteTimeout
from dse.policies import FallthroughRetryPolicy
from dse.query import SimpleStatement
from nose.plugins.attrib import attr

from dtest import Tester, debug, supports_v5_protocol
from thrift_bindings.v22 import ttypes as thrift_types
from thrift_tests import get_thrift_client
from tools.data import create_ks
from tools.decorators import since
from tools.jmxutils import (JolokiaAgent, make_mbean,
                            remove_perf_disable_shared_mem)

KEYSPACE = "foo"


# These tests use the cassandra.test.fail_writes_ks option, which was only
# implemented in 2.2, so we skip it before then.
@since('2.2')
class TestWriteFailures(Tester):
    """
    Tests for write failures in the replicas,
    @jira_ticket CASSANDRA-8592.

    They require CURRENT_VERSION = VERSION_4 in CassandraDaemon.Server
    otherwise these tests will fail.
    """

    def setUp(self):
        super(TestWriteFailures, self).setUp()

        self.ignore_log_patterns = [
            "Testing write failures",  # The error to simulate a write failure
            "ERROR WRITE_FAILURE",     # Logged in DEBUG mode for write failures
            "MigrationStage"           # This occurs sometimes due to node down (because of restart)
        ]

        self.supports_v5_protocol = supports_v5_protocol(self.cluster.version())
        self.expected_expt = WriteFailure
        self.protocol_version = 5 if self.supports_v5_protocol else 4
        self.replication_factor = 3
        self.consistency_level = ConsistencyLevel.ALL
        self.failing_nodes = [1, 2]

    def tearDown(self):
        super(TestWriteFailures, self).tearDown()

    def _prepare_cluster(self, start_rpc=False):
        self.cluster.populate(3)

        if start_rpc:
            self.cluster.set_configuration_options(values={'start_rpc': True})

        self.cluster.start()
        self.nodes = self.cluster.nodes.values()

        session = self.patient_exclusive_cql_connection(self.nodes[0], protocol_version=self.protocol_version)

        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS %s
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '%s' }
            """ % (KEYSPACE, self.replication_factor))
        session.set_keyspace(KEYSPACE)

        session.execute("CREATE TABLE IF NOT EXISTS mytable (key text PRIMARY KEY, value text)")
        session.execute("CREATE TABLE IF NOT EXISTS countertable (key uuid PRIMARY KEY, value counter)")

        for idx in self.failing_nodes:
            node = self.nodes[idx]
            node.stop()
            node.start(wait_for_binary_proto=True, jvm_args=["-Dcassandra.test.fail_writes_ks=" + KEYSPACE])

            if idx == 0:
                session = self.patient_exclusive_cql_connection(node, protocol_version=self.protocol_version)
                session.set_keyspace(KEYSPACE)

        return session

    def _perform_cql_statement(self, text):
        session = self._prepare_cluster()

        statement = session.prepare(text)
        statement.consistency_level = self.consistency_level

        if self.expected_expt is None:
            session.execute(statement)
        else:
            with self.assertRaises(self.expected_expt) as cm:
                session.execute(statement)
            return cm.exception

    def _assert_error_code_map_exists_with_code(self, exception, expected_code):
        """
        Asserts that the given exception contains an error code map
        where at least one node responded with some expected code.
        This is meant for testing failure exceptions on protocol v5.
        """
        self.assertIsNotNone(exception)
        self.assertIsNotNone(exception.error_code_map)
        expected_code_found = False
        for error_code in exception.error_code_map.values():
            if error_code == expected_code:
                expected_code_found = True
                break
        self.assertTrue(expected_code_found,
                        "The error code map did not contain {}, got instead: {}"
                        .format(expected_code, exception.error_code_map.values()))

    @attr("smoke-test")
    @since('2.2', max_version='2.2.x')
    def test_mutation_v2(self):
        """
        A failed mutation at v2 receives a WriteTimeout
        """
        self.expected_expt = WriteTimeout
        self.protocol_version = 2
        self._perform_cql_statement("INSERT INTO mytable (key, value) VALUES ('key1', 'Value 1')")

    @attr("smoke-test")
    def test_mutation_v3(self):
        """
        A failed mutation at v3 receives a WriteTimeout
        """
        self.expected_expt = WriteTimeout
        self.protocol_version = 3
        self._perform_cql_statement("INSERT INTO mytable (key, value) VALUES ('key1', 'Value 1')")

    @attr("smoke-test")
    def test_mutation_v4(self):
        """
        A failed mutation at v4 receives a WriteFailure
        """
        self.expected_expt = WriteFailure
        self.protocol_version = 4
        self._perform_cql_statement("INSERT INTO mytable (key, value) VALUES ('key1', 'Value 1')")

    @attr("smoke-test")
    @since('3.10')
    def test_mutation_v5(self):
        """
        A failed mutation at v5 receives a WriteFailure with an error code map containing error code 0x0000
        """
        self.expected_expt = WriteFailure
        self.protocol_version = 5
        exc = self._perform_cql_statement("INSERT INTO mytable (key, value) VALUES ('key1', 'Value 1')")
        self._assert_error_code_map_exists_with_code(exc, 0x0000)

    @attr("smoke-test")
    def test_mutation_any(self):
        """
        A WriteFailure is not received at consistency level ANY
        even if all nodes fail because of hinting
        """
        self.consistency_level = ConsistencyLevel.ANY
        self.expected_expt = None
        self.failing_nodes = [0, 1, 2]
        self._perform_cql_statement("INSERT INTO mytable (key, value) VALUES ('key1', 'Value 1')")

    @attr("smoke-test")
    def test_mutation_one(self):
        """
            A WriteFailure is received at consistency level ONE
            if all nodes fail
        """
        self.consistency_level = ConsistencyLevel.ONE
        self.failing_nodes = [0, 1, 2]
        exc = self._perform_cql_statement("INSERT INTO mytable (key, value) VALUES ('key1', 'Value 1')")
        if self.supports_v5_protocol:
            self._assert_error_code_map_exists_with_code(exc, 0x0000)

    @attr("smoke-test")
    def test_mutation_quorum(self):
        """
        A WriteFailure is not received at consistency level
        QUORUM if quorum succeeds
        """
        self.consistency_level = ConsistencyLevel.QUORUM
        self.expected_expt = None
        self.failing_nodes = [2]
        self._perform_cql_statement("INSERT INTO mytable (key, value) VALUES ('key1', 'Value 1')")

    @attr("smoke-test")
    def test_batch(self):
        """
        A failed batch receives a WriteFailure
        """
        exc = self._perform_cql_statement("""
            BEGIN BATCH
            INSERT INTO mytable (key, value) VALUES ('key2', 'Value 2') USING TIMESTAMP 1111111111111111
            INSERT INTO mytable (key, value) VALUES ('key3', 'Value 3') USING TIMESTAMP 1111111111111112
            APPLY BATCH
        """)
        if self.supports_v5_protocol:
            self._assert_error_code_map_exists_with_code(exc, 0x0000)

    @attr("smoke-test")
    def test_counter(self):
        """
        A failed counter mutation receives a WriteFailure
        """
        _id = str(uuid.uuid4())
        exc = self._perform_cql_statement("""
            UPDATE countertable
                SET value = value + 1
                where key = {uuid}
        """.format(uuid=_id))
        if self.supports_v5_protocol:
            if self.cluster.version() >= LooseVersion('4.0'):
                self._assert_error_code_map_exists_with_code(exc, 0x0004)
            else:
                self._assert_error_code_map_exists_with_code(exc, 0x0000)

    @attr("smoke-test")
    def test_paxos(self):
        """
        A light transaction receives a WriteFailure
        """
        exc = self._perform_cql_statement("INSERT INTO mytable (key, value) VALUES ('key1', 'Value 1') IF NOT EXISTS")
        if self.supports_v5_protocol:
            self._assert_error_code_map_exists_with_code(exc, 0x0000)

    @attr("smoke-test")
    def test_paxos_any(self):
        """
        A light transaction at consistency level ANY does not receive a WriteFailure
        """
        self.consistency_level = ConsistencyLevel.ANY
        self.expected_expt = None
        self._perform_cql_statement("INSERT INTO mytable (key, value) VALUES ('key1', 'Value 1') IF NOT EXISTS")

    @since('2.0', max_version='4')
    def test_thrift(self):
        """
        A thrift client receives a TimedOutException
        """
        self._prepare_cluster(start_rpc=True)
        self.expected_expt = thrift_types.TimedOutException

        client = get_thrift_client()
        client.transport.open()
        client.set_keyspace(KEYSPACE)

        with self.assertRaises(self.expected_expt):
            client.insert('key1',
                          thrift_types.ColumnParent('mytable'),
                          thrift_types.Column('value', 'Value 1', 0),
                          thrift_types.ConsistencyLevel.ALL)

        client.transport.close()

    @since('3.0')
    def test_cross_dc_rtt(self):
        """
        Verify cross_dc_rtt_in_ms is applied and reduces hints
        @jira_ticket APOLLO-854
        """
        debug("prepare cluster")
        self.cluster.populate(nodes=[2, 2], install_byteman=True)
        self.cluster.set_configuration_options(
            values={'hinted_handoff_enabled': True,
                    'write_request_timeout_in_ms': 1000,
                    'cross_dc_rtt_in_ms': 6000}
        )
        node1, _, node3, _ = self.cluster.nodelist()
        for node in self.cluster.nodelist():
            remove_perf_disable_shared_mem(node)  # necessary for jmx
        self.cluster.start()
        session = self.patient_exclusive_cql_connection(node1)  # node1 is coordinator with whitelist policy

        debug("prepare schema")
        create_ks(session, 'ks', {'dc1': 2, 'dc2': 2})
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)  WITH speculative_retry = 'NONE'")

        debug("install byteman to dc2 - node3 to delay write request for 5 seconds")
        script_version = '4x' if self.cluster.version() >= '4' else '3x'
        node3.byteman_submit(['./byteman/delay_write_request_{}.btm'.format(script_version)])

        # no hints generated
        hint_size = self._get_hint_size(node1)
        debug("hint size {}".format(hint_size))
        self.assertEquals(0, hint_size)

        debug("test with CL ALL with cross_dc_rtt_in_ms=6000")
        t_request, hint_size = self._exec_insert(session, node1, cl=ConsistencyLevel.ALL)
        self.assertTrue(1 <= t_request <= 2)
        self.assertEquals(0, hint_size)

        debug("test with CL LOCAL_QUORUM with cross_dc_rtt_in_ms=6000")
        t_request, hint_size = self._exec_insert(session, node1)
        self.assertTrue(t_request < 1)
        self.assertEquals(0, hint_size)

        debug("test with CL LOCAL_QUORUM with cross_dc_rtt_in_ms=1000")
        for node in self.cluster.nodelist():
            self._set_cross_dc_rtt_in_ms(node, 1000)
        t_request, hint_size = self._exec_insert(session, node1)
        self.assertTrue(t_request < 1)
        self.assertNotEquals(0, hint_size)

        debug("test with CL LOCAL_QUORUM with cross_dc_rtt_in_ms=0")
        for node in self.cluster.nodelist():
            self._set_cross_dc_rtt_in_ms(node, 0)
        t_request, hint_size_2 = self._exec_insert(session, node1)
        self.assertTrue(t_request < 1)
        self.assertEquals(hint_size * 2, hint_size_2)

    def _exec_insert(self, session, node, sleep=8, cl=ConsistencyLevel.LOCAL_QUORUM):
        t_start = time.time()
        try:
            session.execute(SimpleStatement("INSERT INTO ks.t(id) VALUES(1)",
                                            consistency_level=cl,
                                            retry_policy=FallthroughRetryPolicy()))
            if cl is ConsistencyLevel.ALL:
                self.fail("Expect WriteTimeout")
            t_request = time.time() - t_start
            debug("duration:{}s".format(t_request))
        except WriteTimeout as exc:
            t_request = time.time() - t_start
            debug("duration:{}s {}".format(t_request, str(exc)))

        # wait for cross dc rtt
        time.sleep(sleep)

        hint_size = self._get_hint_size(node)
        debug("hint size {}".format(hint_size))

        return t_request, hint_size

    def _set_cross_dc_rtt_in_ms(self, node, cross_dc_rtt_in_ms):
        mbean = make_mbean('db', type='StorageProxy', )
        with JolokiaAgent(node) as jmx:
            jmx.write_attribute(mbean, attribute='CrossDCRttLatency', value=cross_dc_rtt_in_ms)

    def _get_hint_size(self, node):
        mbean = make_mbean('metrics', type='Storage', name='TotalHints')
        with JolokiaAgent(node) as jmx:
            return jmx.read_attribute(mbean, 'Count')

    @since('3.11')
    def test_success_with_failing_node(self):
        """
        Verify that a single node consistently writing to fail will not result in a failed write if the other replicas
        write successfully.
        @jira_ticket DB-1717
        """
        num_nodes = 3
        cluster = self.cluster
        debug("Creating and starting a {}-node cluster".format(num_nodes))
        cluster.populate(nodes=num_nodes).start(wait_for_binary_proto=True)

        ks_name = "ks"
        node = cluster.nodelist()[0]
        debug("Restarting node 0 to set it up so that it always fails writes")
        node.stop()
        node.start(wait_for_binary_proto=True, jvm_args=["-Dcassandra.test.fail_writes_ks=" + ks_name])

        cf_name = "t1"
        debug("Creating a keyspace and a table (replicated on all nodes)")
        setupCql = """
            CREATE KEYSPACE {ks_name} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor': {num_nodes} }};
            CREATE TABLE {ks_name}.{cf_name} (tid varchar PRIMARY KEY, data varchar);
        """.format(ks_name=ks_name, num_nodes=num_nodes, cf_name=cf_name)
        node.run_cqlsh(setupCql)
        time.sleep(1)

        debug("Executing some QUORUM writes that are expected to succeed (only node 0 should fail to write)")
        session = self.patient_cql_connection(node, consistency_level=ConsistencyLevel.QUORUM)
        for i in range(1, 2 * num_nodes):
            writeCql = "UPDATE {ks_name}.{cf_name} SET data='Some data' WHERE tid='key{index}'" \
                .format(ks_name=ks_name, cf_name=cf_name, index=i)
            session.execute(writeCql)
