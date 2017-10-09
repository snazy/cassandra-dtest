import time

from dtest import Tester
from tools.decorators import since
from tools.data import create_ks


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
