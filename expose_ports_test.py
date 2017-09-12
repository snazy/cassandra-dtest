from dtest import Tester
from tools.assertions import assert_one, assert_all
from tools.decorators import since


@since('4.0')
class TestExposePorts(Tester):
    """
    Test the exposition of both local and remote communication ports through system tables.
    @jira_ticket APOLLO-1040
    """

    def test_expose_local_ports(self):
        """
        Test the exposition of local connection ports through `system.local` table.
        """
        cluster = self.cluster
        cluster.populate(3).start()
        query = """
           SELECT
              native_transport_port,
              native_transport_port_ssl,
              storage_port,
              storage_port_ssl,
              jmx_port
           FROM system.local
        """
        for node in cluster.nodelist():
            session = self.patient_exclusive_cql_connection(node)
            assert_one(session, query, [9042, 9042, 7000, 7001, int(node.jmx_port)])

    def test_expose_peers_ports(self):
        """
        Test the exposition of peers connection ports through `system.peers` table.
        """
        cluster = self.cluster
        cluster.populate(3).start()
        nodes = cluster.nodelist()
        query = """
           SELECT
              native_transport_port,
              native_transport_port_ssl,
              storage_port,
              storage_port_ssl,
              jmx_port
           FROM system.peers
        """
        for node in nodes:
            session = self.patient_exclusive_cql_connection(node)
            peers = filter(lambda n: n != node, nodes)
            expected = map(lambda peer: [9042, 9042, 7000, 7001, int(peer.jmx_port)], peers)
            assert_all(session, query, expected, ignore_order=True)
