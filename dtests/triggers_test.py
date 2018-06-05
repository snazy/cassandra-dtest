from ccmlib.node import TimeoutError
from dse import ConsistencyLevel

from dtest import Tester, debug
from tools.data import create_cf, create_ks, insert_c1c2
from tools.decorators import since_dse


class TestTriggers(Tester):

    @since_dse('6.0')
    def triggers_test(self):
        """
        * Loads a test trigger
        * Check that is being triggered
        * Check that node cannot startup if trigger cannot be loaded

        If changing the org.apache.cassandra.triggers.ITrigger interface,
        the lib/test-trigger.jar must be regenerated via the ant target
        "ant test-trigger-jar" on the apollo project.

        @jira_ticket APOLLO-1181
        """
        debug("Starting cluster...")

        ERROR_MSG = "Could not load class 'org.apache.cassandra.triggers.TestTrigger' from trigger 'testtrigger' from ks.cf."

        self.ignore_log_patterns = [ERROR_MSG]

        cluster = self.cluster
        cluster.populate(1).start(jvm_args=["-Dcassandra.triggers_dir=lib"])
        node1 = self.cluster.nodelist()[0]

        debug("Creating table")
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})

        debug("Creating trigger")
        session.execute("CREATE TRIGGER testtrigger ON ks.cf USING 'org.apache.cassandra.triggers.TestTrigger';")

        debug("Inserting data")
        insert_c1c2(session, n=10, consistency=ConsistencyLevel.ALL)

        debug("Checking trigger was triggered")
        triggered = node1.grep_log("Writing key")
        self.assertEqual(len(triggered), 10)

        debug("Stopping node")
        node1.stop()

        debug("Starting node without trigger jar on classpath - should fail")
        with self.assertRaises(TimeoutError):
            node1.start(wait_for_binary_proto=30)
        self.assertTrue(node1.grep_log(ERROR_MSG))

        debug("Starting node with trigger jar on classpath - should work")
        node1.start(jvm_args=["-Dcassandra.triggers_dir=lib"], wait_for_binary_proto=True)
