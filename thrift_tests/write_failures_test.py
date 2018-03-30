from dse import ConsistencyLevel, WriteFailure

from dtests.dtest import Tester, supports_v5_protocol
from thrift_bindings.v22 import ttypes as thrift_types
from tools.decorators import since

from .thrift_tests import get_thrift_client

KEYSPACE = "foo"


class TestWriteFailures(Tester):

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

    def _prepare_cluster(self, start_rpc=False, with_compact_storage=False):
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

        compact_storage = " WITH COMPACT STORAGE" if with_compact_storage else ""

        session.execute("CREATE TABLE IF NOT EXISTS mytable (key text PRIMARY KEY, value text){}".format(compact_storage))
        session.execute("CREATE TABLE IF NOT EXISTS countertable (key uuid PRIMARY KEY, value counter)")

        for idx in self.failing_nodes:
            node = self.nodes[idx]
            node.stop()
            node.start(wait_for_binary_proto=True, jvm_args=["-Dcassandra.test.fail_writes_ks=" + KEYSPACE])

            if idx == 0:
                session = self.patient_exclusive_cql_connection(node, protocol_version=self.protocol_version)
                session.set_keyspace(KEYSPACE)

        return session

    @since('2.0', max_version='4')
    def test_thrift(self):
        """
        A thrift client receives a TimedOutException
        """
        self._prepare_cluster(start_rpc=True, with_compact_storage=True)
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
