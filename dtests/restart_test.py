import time

from dtest import Tester
from tools.data import create_ks
from tools.decorators import since


@since('3.0')
class TestRestart(Tester):
    def schema_change_while_node_initializing_test(self):
        """
        Tests whether a slow initialization of a node's StorageService/Gossiper after restart can lead to problems
        if there are schema changes in the meantime.
        @jira_ticket DB-1487
        """
        cluster = self.cluster
        cluster.populate(2, debug=True, install_byteman=True).start()

        node2 = cluster.nodelist()[1]
        # Stop a node without waiting for the others to notice (in order for them to still consider the node alive
        # during its initialization, where is the vulnerability window we're testing)
        node2.stop(wait_other_notice=False, gently=False)
        node2.update_startup_byteman_script('./byteman/delay_storage_service_init.btm')
        node2.start(no_wait=True, wait_other_notice=False, wait_for_binary_proto=False)

        # This sleep interval should be as short as possible, but long enough to have the tested node restart.
        # Its purpose is to allow the schema change load that follows it to happen as early as possible, before
        # the healthy node is able to pick up that the tested node has restarted, and may not be in working condition
        time.sleep(2)

        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        for i in range(1, 10):
            # Trigger a schema change (that needs to reach the tested node)
            create_ks(session, 'test_keyspace' + str(i), 2)
            time.sleep(1)

        # Wait enough to have the test complete after the delayed StorageService initialization has completed
        time.sleep(10)
