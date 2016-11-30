from dtest import Tester, debug
from tools.data import putget, create_cf, create_ks
from tools.sslkeygen import generate_ssl_stores


class TestInternodeSSL(Tester):

    def putget_with_internode_ssl_test(self):
        """
        Simple putget test with internode ssl enabled
        with default 'all' internode compression
        @jira_ticket CASSANDRA-9884
        """
        self.__putget_with_internode_ssl_test('all')

    def putget_with_internode_ssl_without_compression_test(self):
        """
        Simple putget test with internode ssl enabled
        without internode compression
        @jira_ticket CASSANDRA-9884
        """
        self.__putget_with_internode_ssl_test('none')

    def __putget_with_internode_ssl_test(self, internode_compression):
        cluster = self.cluster

        debug("***using internode ssl***")
        generate_ssl_stores(self.test_path)
        cluster.set_configuration_options({'internode_compression': internode_compression})
        cluster.enable_internode_ssl(self.test_path)

        cluster.populate(3).start()

        session = self.patient_cql_connection(cluster.nodelist()[0])
        create_ks(session, 'ks', 3)
        create_cf(session, 'cf', compression=None)
        putget(cluster, session)
