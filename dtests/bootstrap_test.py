import os
import random
import re
import shutil
import tempfile
import threading
import time
from functools import reduce

from ccmlib.node import NodeError
from dse import ConsistencyLevel
from dse.cluster import NoHostAvailable
from dse.concurrent import execute_concurrent_with_args
from nose.plugins.attrib import attr

from dtest import DISABLE_VNODES, Tester, debug
from tools.assertions import (assert_almost_equal, assert_bootstrap_state,
                              assert_not_running, assert_one,
                              assert_stderr_clean)
from tools.data import create_cf, create_ks, query_c1c2
from tools.decorators import no_vnodes, since
from tools.intervention import InterruptBootstrap
from tools.misc import new_node
from tools.sslkeygen import generate_ssl_stores


class BaseBootstrapTest(Tester):
    __test__ = False

    allow_log_errors = True
    ignore_log_patterns = (
        # This one occurs when trying to send the migration to a
        # node that hasn't started yet, and when it does, it gets
        # replayed and everything is fine.
        r'Can\'t send migration request: node.*is down',
        # ignore streaming error during bootstrap
        r'Exception encountered during startup',
        r'Streaming error occurred'
    )

    def _base_bootstrap_test(self, bootstrap=None, bootstrap_from_version=None,
                             enable_ssl=None):
        def default_bootstrap(cluster, token):
            node2 = new_node(cluster)
            node2.set_configuration_options(values={'initial_token': token})
            node2.start(wait_for_binary_proto=True)
            return node2

        if bootstrap is None:
            bootstrap = default_bootstrap

        cluster = self.cluster

        if enable_ssl:
            debug("***using internode ssl***")
            generate_ssl_stores(self.test_path)
            cluster.enable_internode_ssl(self.test_path)
            # This is an artifact of having https://github.com/pcmanus/ccm/pull/639 merged into CCM
            # but not having CASSANDRA-10404 merged to Apollo
            if cluster.version() >= '4.0':
                del cluster._config_options["server_encryption_options"]["enabled"]

        tokens = cluster.balanced_tokens(2)
        cluster.set_configuration_options(values={'num_tokens': 1})

        debug("[node1, node2] tokens: %r" % (tokens,))

        keys = 10000

        # Create a single node cluster
        cluster.populate(1)
        node1 = cluster.nodelist()[0]
        if bootstrap_from_version:
            debug("starting source node on version {}".format(bootstrap_from_version))
            node1.set_install_dir(version=bootstrap_from_version)
        node1.set_configuration_options(values={'initial_token': tokens[0]})
        cluster.start()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})

        # record the size before inserting any of our own data
        empty_size = node1.data_size()
        debug("node1 empty size : %s" % float(empty_size))

        insert_statement = session.prepare("INSERT INTO ks.cf (key, c1, c2) VALUES (?, 'value1', 'value2')")
        execute_concurrent_with_args(session, insert_statement, [['k%d' % k] for k in range(keys)])

        node1.flush()
        node1.compact()
        initial_size = node1.data_size()
        debug("node1 size before bootstrapping node2: %s" % float(initial_size))

        # Reads inserted data all during the bootstrap process. We shouldn't
        # get any error
        debug("Starting reader thread")
        reader = self.go(lambda _: query_c1c2(session, random.randint(0, keys - 1), ConsistencyLevel.ONE), endless=True)

        # Bootstrapping a new node in the current version
        node2 = bootstrap(cluster, tokens[1])
        node2.compact()

        reader.check()
        node1.cleanup()
        debug("node1 size after cleanup: %s" % float(node1.data_size()))
        node1.compact()
        debug("node1 size after compacting: %s" % float(node1.data_size()))
        time.sleep(.5)
        reader.check()

        debug("node2 size after compacting: %s" % float(node2.data_size()))

        size1 = float(node1.data_size())
        size2 = float(node2.data_size())
        assert_almost_equal(size1, size2, error=0.3)
        assert_almost_equal(float(initial_size - empty_size), 2 * (size1 - float(empty_size)))

        debug("Waiting for node2's bootstrap state COMPLETED")
        assert_bootstrap_state(self, node2, 'COMPLETED')
        debug("bootstrap state for node2 is COMPLETED")
        if bootstrap_from_version:
            debug("waiting for 'does not support keep-alive' in log")
            self.assertTrue(node2.grep_log('does not support keep-alive', filename='debug.log'))
        debug("Stopping reader thread")
        reader.stop()
        debug("Done")


class TestBootstrap(BaseBootstrapTest):
    __test__ = True

    @no_vnodes()
    def simple_bootstrap_test_with_ssl(self):
        self._base_bootstrap_test(enable_ssl=True)

    @no_vnodes()
    def simple_bootstrap_test(self):
        self._base_bootstrap_test()

    @no_vnodes()
    def bootstrap_on_write_survey_test(self):
        def bootstrap_on_write_survey_and_join(cluster, token):
            node2 = new_node(cluster)
            node2.set_configuration_options(values={'initial_token': token})
            node2.start(jvm_args=["-Dcassandra.write_survey=true"], wait_for_binary_proto=True)

            self.assertTrue(len(node2.grep_log('Startup complete, but write survey mode is active, not becoming an active ring member.')))
            assert_bootstrap_state(self, node2, 'IN_PROGRESS')

            node2.nodetool("join")
            self.assertTrue(len(node2.grep_log('Leaving write survey mode and joining ring at operator request')))
            return node2

        self._base_bootstrap_test(bootstrap_on_write_survey_and_join)

    @since('3.10')
    @no_vnodes()
    def simple_bootstrap_test_small_keepalive_period(self):
        """
        @jira_ticket CASSANDRA-11841
        Test that bootstrap completes if it takes longer than streaming_socket_timeout_in_ms or
        2*streaming_keep_alive_period_in_secs to receive a single sstable
        """
        cluster = self.cluster
        yaml_opts = {'streaming_keep_alive_period_in_secs': 2}
        # TODO remove "unconditional if condition" when merging netty-based internode messaging/streaming from trunk !
        if cluster.version() < '4.0':
            yaml_opts['streaming_socket_timeout_in_ms'] = 1000
        cluster.set_configuration_options(values=yaml_opts)

        # Create a single node cluster
        cluster.populate(1)
        node1 = cluster.nodelist()[0]

        debug("Setting up byteman on {}".format(node1.name))
        # set up byteman
        node1.byteman_port = '8100'
        node1.import_config_files()

        cluster.start()

        # Create more than one sstable larger than 1MB
        node1.stress(['write', 'n=1K', '-rate', 'threads=8', '-schema',
                      'compaction(strategy=SizeTieredCompactionStrategy, enabled=false)'])
        cluster.flush()

        debug("Submitting byteman script to {} to".format(node1.name))
        # Sleep longer than streaming_socket_timeout_in_ms to make sure the node will not be killed
        node1.byteman_submit(['./byteman/stream_5s_sleep.btm'])

        # Bootstraping a new node with very small streaming_socket_timeout_in_ms
        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=True)

        # Shouldn't fail due to streaming socket timeout timeout
        assert_bootstrap_state(self, node2, 'COMPLETED')

        for node in cluster.nodelist():
            self.assertTrue(node.grep_log('Scheduling keep-alive task with 2s period.', filename='debug.log'))
            self.assertTrue(node.grep_log('Sending keep-alive', filename='debug.log'))
            self.assertTrue(node.grep_log('Received keep-alive', filename='debug.log'))

    @attr("smoke-test")
    def simple_bootstrap_test_nodata(self):
        """
        @jira_ticket CASSANDRA-11010
        Test that bootstrap completes if streaming from nodes with no data
        """

        cluster = self.cluster
        # Create a two-node cluster
        cluster.populate(2)
        cluster.start()

        # Bootstrapping a new node
        node3 = new_node(cluster)
        node3.start(wait_for_binary_proto=True, wait_other_notice=True)

        assert_bootstrap_state(self, node3, 'COMPLETED')

    @attr("smoke-test")
    def read_from_bootstrapped_node_test(self):
        """
        Test bootstrapped node sees existing data
        @jira_ticket CASSANDRA-6648
        """
        cluster = self.cluster
        cluster.populate(3)
        cluster.start()

        node1 = cluster.nodes['node1']
        node1.stress(['write', 'n=10K', 'no-warmup', '-rate', 'threads=8', '-schema', 'replication(factor=2)'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1'
        original_rows = list(session.execute("SELECT * FROM %s" % (stress_table,)))

        node4 = new_node(cluster)
        node4.start(wait_for_binary_proto=True)

        session = self.patient_exclusive_cql_connection(node4)
        new_rows = list(session.execute("SELECT * FROM %s" % (stress_table,)))
        self.assertEquals(original_rows, new_rows)

    def consistent_range_movement_true_with_replica_down_should_fail_test(self):
        self._bootstrap_test_with_replica_down(True)

    def consistent_range_movement_false_with_replica_down_should_succeed_test(self):
        self._bootstrap_test_with_replica_down(False)

    def consistent_range_movement_true_with_rf1_should_fail_test(self):
        self._bootstrap_test_with_replica_down(True, rf=1)

    def consistent_range_movement_false_with_rf1_should_succeed_test(self):
        self._bootstrap_test_with_replica_down(False, rf=1)

    def _bootstrap_test_with_replica_down(self, consistent_range_movement, rf=2):
        """
        Test to check consistent bootstrap will not succeed when there are insufficient replicas

        A port of the range streamer logic is in the utest org.apache.cassandra.dht.RangeStreamerBootstrapTest

        @jira_ticket CASSANDRA-11848
        """
        cluster = self.cluster

        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        node3_token = None
        # Make token assignment deterministic
        if DISABLE_VNODES:
            cluster.set_configuration_options(values={'num_tokens': 1})
            tokens = cluster.balanced_tokens(3)
            debug("non-vnode tokens: %r" % (tokens,))
            node1.set_configuration_options(values={'initial_token': tokens[0]})
            node2.set_configuration_options(values={'initial_token': tokens[2]})
            node3_token = tokens[1]  # Add node 3 between node1 and node2

        cluster.start()

        node1.stress(['write', 'n=10K', 'no-warmup', '-rate', 'threads=8', '-schema', 'replication(factor={})'.format(rf)])

        # change system_auth keyspace to 2 (default is 1) to avoid
        # "Unable to find sufficient sources for streaming" warning
        if cluster.cassandra_version() >= '2.2.0':
            session = self.patient_cql_connection(node1)
            session.execute("""
                ALTER KEYSPACE system_auth
                    WITH replication = {'class':'SimpleStrategy', 'replication_factor':2};
            """)

        # Stop node2, so node3 will not be able to perform consistent range movement
        node2.stop(wait_other_notice=True)

        successful_bootstrap_expected = not consistent_range_movement

        node3 = new_node(cluster, token=node3_token)
        node3.start(wait_for_binary_proto=successful_bootstrap_expected, wait_other_notice=successful_bootstrap_expected,
                    jvm_args=["-Dcassandra.consistent.rangemovement={}".format(consistent_range_movement)])

        if successful_bootstrap_expected:
            # with rf=1 and cassandra.consistent.rangemovement=false, missing sources are ignored
            if rf == 1:
                node3.watch_log_for("Unable to find sufficient sources for streaming range")
            self.assertTrue(node3.is_running())
            assert_bootstrap_state(self, node3, 'COMPLETED')
        else:
            node3.watch_log_for("A node required to move the data consistently is down")
            assert_not_running(node3)

    @since('2.2')
    def resumable_bootstrap_test(self):
        """
        Test resuming bootstrap after data streaming failure
        """

        cluster = self.cluster
        cluster.populate(2)

        node1 = cluster.nodes['node1']
        # set up byteman
        node1.byteman_port = '8100'
        node1.import_config_files()

        cluster.start()
        # kill stream to node3 in the middle of streaming to let it fail
        # TODO remove "unconditional if condition" when merging netty-based internode messaging/streaming from trunk !
        if True or cluster.version() < '4.0':
            node1.byteman_submit(['./byteman/pre4.0/stream_failure.btm'])
        else:
            node1.byteman_submit(['./byteman/4.0/stream_failure.btm'])
        node1.stress(['write', 'n=1K', 'no-warmup', 'cl=TWO', '-schema', 'replication(factor=2)', '-rate', 'threads=50'])
        cluster.flush()

        # start bootstrapping node3 and wait for streaming
        node3 = new_node(cluster)
        node3.start(wait_other_notice=False, wait_for_binary_proto=True)

        # wait for node3 ready to query
        node3.watch_log_for("Starting listening for CQL clients")
        mark = node3.mark_log()
        # check if node3 is still in bootstrap mode
        assert_bootstrap_state(self, node3, 'IN_PROGRESS')

        # bring back node1 and invoke nodetool bootstrap to resume bootstrapping
        node3.nodetool('bootstrap resume')

        node3.watch_log_for("Resume complete", from_mark=mark)
        assert_bootstrap_state(self, node3, 'COMPLETED')

        # cleanup to guarantee each node will only have sstables of its ranges
        cluster.cleanup()

        debug("Check data is present")
        # Let's check stream bootstrap completely transferred data
        stdout, stderr, _ = node3.stress(['read', 'n=1k', 'no-warmup', '-schema', 'replication(factor=2)', '-rate', 'threads=8'])

        if stdout is not None:
            self.assertNotIn("FAILURE", stdout)

    @since('2.2')
    def bootstrap_with_reset_bootstrap_state_test(self):
        """Test bootstrap with resetting bootstrap progress"""

        cluster = self.cluster
        cluster.set_configuration_options(values={'stream_throughput_outbound_megabits_per_sec': 1})
        cluster.populate(2).start()

        node1 = cluster.nodes['node1']
        node1.stress(['write', 'n=100K', '-schema', 'replication(factor=2)'])
        node1.flush()

        # kill node1 in the middle of streaming to let it fail
        t = InterruptBootstrap(node1)
        t.start()

        # start bootstrapping node3 and wait for streaming
        node3 = new_node(cluster)
        try:
            node3.start()
        except NodeError:
            pass  # node doesn't start as expected
        t.join()
        node1.start()

        # restart node3 bootstrap with resetting bootstrap progress
        node3.stop()
        mark = node3.mark_log()
        node3.start(jvm_args=["-Dcassandra.reset_bootstrap_progress=true"])
        # check if we reset bootstrap state
        node3.watch_log_for("Resetting bootstrap progress to start fresh", from_mark=mark)
        # wait for node3 ready to query
        node3.wait_for_binary_interface(from_mark=mark)

        # check if 2nd bootstrap succeeded
        assert_bootstrap_state(self, node3, 'COMPLETED')

    @attr("smoke-test")
    def manual_bootstrap_test(self):
        """
            Test adding a new node and bootstrapping it manually. No auto_bootstrap.
            This test also verify that all data are OK after the addition of the new node.
            @jira_ticket CASSANDRA-9022
        """
        cluster = self.cluster
        cluster.populate(2).start()
        (node1, node2) = cluster.nodelist()

        node1.stress(['write', 'n=1K', 'no-warmup', '-schema', 'replication(factor=2)',
                      '-rate', 'threads=1', '-pop', 'dist=UNIFORM(1..1000)'])

        session = self.patient_exclusive_cql_connection(node2)
        stress_table = 'keyspace1.standard1'

        original_rows = list(session.execute("SELECT * FROM %s" % stress_table))

        # Add a new node
        node3 = new_node(cluster, bootstrap=False)
        node3.start(wait_for_binary_proto=True)
        node3.repair()
        node1.cleanup()

        current_rows = list(session.execute("SELECT * FROM %s" % stress_table))
        self.assertEquals(original_rows, current_rows)

    def local_quorum_bootstrap_test(self):
        """
        Test that CL local_quorum works while a node is bootstrapping.
        @jira_ticket CASSANDRA-8058
        """

        cluster = self.cluster
        cluster.populate([1, 1])
        cluster.start()

        node1 = cluster.nodes['node1']
        yaml_config = """
        # Create the keyspace and table
        keyspace: keyspace1
        keyspace_definition: |
          CREATE KEYSPACE keyspace1 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1};
        table: users
        table_definition:
          CREATE TABLE users (
            username text,
            first_name text,
            last_name text,
            email text,
            PRIMARY KEY(username)
          ) WITH compaction = {'class':'SizeTieredCompactionStrategy'};
        insert:
          partitions: fixed(1)
          batchtype: UNLOGGED
        queries:
          read:
            cql: select * from users where username = ?
            fields: samerow
        """
        with tempfile.NamedTemporaryFile(mode='w+') as stress_config:
            stress_config.write(yaml_config)
            stress_config.flush()
            node1.stress(['user', 'profile=' + stress_config.name, 'n=2M', 'no-warmup',
                          'ops(insert=1)', '-rate', 'threads=50'])

            node3 = new_node(cluster, data_center='dc2')
            node3.start(no_wait=True)
            time.sleep(3)

            out, err, _ = node1.stress(['user', 'profile=' + stress_config.name, 'ops(insert=1)',
                                        'n=500K', 'no-warmup', 'cl=LOCAL_QUORUM',
                                        '-rate', 'threads=5',
                                        '-errors', 'retries=2'])

        debug(out)
        assert_stderr_clean(err)
        regex = re.compile("Operation.+error inserting key.+Exception")
        failure = regex.search(out)
        self.assertIsNone(failure, "Error during stress while bootstrapping")

    def shutdown_wiped_node_cannot_join_test(self):
        self._wiped_node_cannot_join_test(gently=True)

    def killed_wiped_node_cannot_join_test(self):
        self._wiped_node_cannot_join_test(gently=False)

    def _wiped_node_cannot_join_test(self, gently):
        """
        @jira_ticket CASSANDRA-9765
        Test that if we stop a node and wipe its data then the node cannot join
        when it is not a seed. Test both a nice shutdown or a forced shutdown, via
        the gently parameter.
        """
        cluster = self.cluster
        cluster.populate(3)
        cluster.start()

        stress_table = 'keyspace1.standard1'

        # write some data
        node1 = cluster.nodelist()[0]
        node1.stress(['write', 'n=10K', 'no-warmup', '-rate', 'threads=8'])

        session = self.patient_cql_connection(node1)
        original_rows = list(session.execute("SELECT * FROM {}".format(stress_table,)))

        # Add a new node, bootstrap=True ensures that it is not a seed
        node4 = new_node(cluster, bootstrap=True)
        node4.start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(node4)
        self.assertEquals(original_rows, list(session.execute("SELECT * FROM {}".format(stress_table,))))

        # Stop the new node and wipe its data
        node4.stop(gently=gently)
        self._cleanup(node4)
        # Now start it, it should not be allowed to join.
        mark = node4.mark_log()
        node4.start(no_wait=True, wait_other_notice=False)
        node4.watch_log_for("A node with address /127.0.0.4 already exists, cancelling join", from_mark=mark)

    def decommissioned_wiped_node_can_join_test(self):
        """
        @jira_ticket CASSANDRA-9765
        Test that if we decommission a node and then wipe its data, it can join the cluster.
        """
        cluster = self.cluster
        cluster.populate(3)
        cluster.start()

        stress_table = 'keyspace1.standard1'

        # write some data
        node1 = cluster.nodelist()[0]
        node1.stress(['write', 'n=10K', 'no-warmup', '-rate', 'threads=8'])

        session = self.patient_cql_connection(node1)
        original_rows = list(session.execute("SELECT * FROM {}".format(stress_table,)))

        # Add a new node, bootstrap=True ensures that it is not a seed
        node4 = new_node(cluster, bootstrap=True)
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)

        session = self.patient_cql_connection(node4)
        self.assertEquals(original_rows, list(session.execute("SELECT * FROM {}".format(stress_table,))))

        # Decommission the new node and wipe its data
        node4.decommission()
        node4.stop()
        self._cleanup(node4)
        # Now start it, it should be allowed to join
        mark = node4.mark_log()
        node4.start(wait_other_notice=True)
        node4.watch_log_for("JOINING:", from_mark=mark)

    def decommissioned_wiped_node_can_gossip_to_single_seed_test(self):
        """
        @jira_ticket CASSANDRA-8072
        @jira_ticket CASSANDRA-8422
        Test that if we decommission a node, kill it and wipe its data, it can join a cluster with a single
        seed node.
        """
        cluster = self.cluster
        cluster.populate(1)
        cluster.start()

        node1 = cluster.nodelist()[0]
        # Add a new node, bootstrap=True ensures that it is not a seed
        node2 = new_node(cluster, bootstrap=True)
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)

        session = self.patient_cql_connection(node1)

        if cluster.version() >= '2.2':
            # reduce system_distributed RF to 2 so we don't require forceful decommission
            session.execute("ALTER KEYSPACE system_distributed WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'};")

        session.execute("ALTER KEYSPACE system_traces WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'};")

        # Decommision the new node and kill it
        debug("Decommissioning & stopping node2")
        node2.decommission()
        node2.stop(wait_other_notice=False)

        # Wipe its data
        for data_dir in node2.data_directories():
            debug("Deleting {}".format(data_dir))
            shutil.rmtree(data_dir)

        commitlog_dir = os.path.join(node2.get_path(), 'commitlogs')
        debug("Deleting {}".format(commitlog_dir))
        shutil.rmtree(commitlog_dir)

        # Now start it, it should be allowed to join
        mark = node2.mark_log()
        debug("Restarting wiped node2")
        node2.start(wait_other_notice=False)
        node2.watch_log_for("JOINING:", from_mark=mark)

    def failed_bootstrap_wiped_node_can_join_before_gossip_expiration_test(self):
        self._test_failed_bootstrap_wiped_node_can_join(phase="before_expiration")

    def failed_bootstrap_wiped_node_can_join_after_gossip_expiration_test(self):
        self._test_failed_bootstrap_wiped_node_can_join(phase="after_expiration")

    def failed_bootstrap_wiped_node_can_join_after_gossip_quarantine_test(self):
        self._test_failed_bootstrap_wiped_node_can_join(phase="after_quarantine")

    def _test_failed_bootstrap_wiped_node_can_join(self, phase):
        """
        @jira_ticket CASSANDRA-9765
        Test that if a node fails to bootstrap, it can join the cluster even if the data is wiped.
        """

        debug("Creating and starting cluster")
        cluster = self.cluster
        cluster.populate(1, install_byteman=True)
        cluster.start(jvm_args=['-Dcassandra.ring_delay_ms=10000'])

        debug("Writing some data to node1")
        node1 = cluster.nodelist()[0]
        node1.stress(['write', 'n=1K', 'no-warmup', '-rate', 'threads=8', '-schema', 'replication(factor=2)'])
        node1.flush()

        debug("Reading data to compare later")
        stress_table = 'keyspace1.standard1'
        session = self.patient_cql_connection(node1)
        original_rows = list(session.execute("SELECT * FROM {}".format(stress_table,)))

        debug("Starting new node2 and inject streaming failure on node1 via byteman so bootstrap fails")
        node2 = new_node(cluster, bootstrap=True)
        node1.byteman_submit(['./byteman/pre4.0/stream_failure.btm'])
        node2.start(wait_for_binary_proto=True, wait_other_notice=False, jvm_args=['-Dcassandra.ring_delay_ms=10000'])

        debug("Make sure bootstrap failed and stop node2")
        assert_bootstrap_state(self, node2, 'IN_PROGRESS')
        node2.stop()

        if phase == "after_expiration":
            debug("Wait node1 to remove node2 from gossip")
            node1.watch_log_for("FatClient /{} has been silent for".format(node2.address()), timeout=60)
        elif phase == "after_quarantine":
            debug("Wait node1 to remove node2 from quarantine")
            node1.watch_log_for("/{} gossip quarantine over".format(node2.address()), timeout=60, filename='debug.log')

        # wipe any data for node2
        debug("Wiping node2 and restarting bootstrap")
        self._cleanup(node2)
        # Now start it again, it should complete join
        mark = node2.mark_log()
        node2.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=['-Dcassandra.ring_delay_ms=10000'])

        debug("Bootstrap finished. Stopping node1")
        node1.stop()

        debug("Checking data is present on node2")
        session = self.patient_exclusive_cql_connection(node2)
        new_rows = list(session.execute("SELECT * FROM %s" % (stress_table,)))
        self.assertEquals(original_rows, new_rows)

    @since('2.1.1')
    def simultaneous_bootstrap_test(self):
        """
        Attempt to bootstrap two nodes at once, to assert the second bootstrapped node fails, and does not interfere.

        Start a one node cluster and run a stress write workload.
        Start up a second node, and wait for the first node to detect it has joined the cluster.
        While the second node is bootstrapping, start a third node. This should fail.

        @jira_ticket CASSANDRA-7069
        @jira_ticket CASSANDRA-9484
        """

        bootstrap_error = "Other bootstrapping/leaving/moving nodes detected," \
                          " cannot bootstrap while cassandra.consistent.rangemovement is true"

        cluster = self.cluster
        cluster.populate(1)
        cluster.start()

        node1, = cluster.nodelist()

        debug("Popuplating data...")
        node1.stress(['write', 'n=500K', 'no-warmup', '-schema', 'replication(factor=1)',
                      '-rate', 'threads=10'])

        # Using RING_DELAY of 60 seconds here to artificially delay the startup of node2,
        # because interleaving of node2's and node3's join/bootstrap is important for this test.

        node2 = new_node(cluster)
        debug("Starting node2 with RING_DELAY of 60 seconds")
        node2.start(wait_other_notice=True, jvm_args=['-Dcassandra.ring_delay_ms=60000'])
        debug("Node2 started")

        debug("Wait until node2 is joining/bootstrapping...")
        node2.watch_log_for("waiting for ring information", timeout=90)
        # The duration from this point to where whe check node3's log file _must not_ exceed RING_DELAY

        node3 = new_node(cluster, remote_debug_port='2003')
        try:
            debug("Node3 starting...")
            node3.start(wait_other_notice=False, verbose=False)
        except NodeError:
            pass  # node doesn't start as expected

        debug("Check for '{}' in node3's log...".format(bootstrap_error))
        node3.watch_log_for(bootstrap_error, timeout=60)

        debug("Wait for node2 to finish startup...")
        # DSE 5.0+ don't need such a long timeout, but C* 2.1 does
        node2.watch_log_for("Starting listening for CQL clients", timeout=180)

        debug("Checking whether bootstrap replicated the data to node2...")
        session = self.patient_exclusive_cql_connection(node2)

        # Repeat the select count(*) query, to help catch
        # bugs like 9484, where count(*) fails at higher
        # data loads.
        for _ in xrange(5):
            assert_one(session, "SELECT count(*) from keyspace1.standard1", [500000], cl=ConsistencyLevel.ONE)

    def test_cleanup(self):
        """
        @jira_ticket CASSANDRA-11179
        Make sure we remove processed files during cleanup
        """

        cluster = self.cluster
        cluster.set_configuration_options(values={'concurrent_compactors': 4})
        cluster.populate(1)
        cluster.start()
        node1, = cluster.nodelist()
        for x in xrange(0, 5):
            node1.stress(['write', 'n=100k', 'no-warmup', '-schema', 'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)', 'replication(factor=1)', '-rate', 'threads=10'])
            node1.flush()
        node2 = new_node(cluster)
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)

        # Use the old version of the test for C*/Apollo old versions
        if self.cluster.version() < "3.0":
            self._test_cleanup_pre30(node1)
            return

        # number of concurrent cleanups
        jobs = 1

        # Make sure that we do not accidentally consider "tmp" sstable as an artifact of the stress run.
        # I.e. wait until all flushes have completed.
        grace_time = 30
        tWaithUntil = time.time() + grace_time
        while len(node1.get_sstables_via_sstableutil("keyspace1", "standard1", sstabletype="tmp")) > 0:
            if time.time() >= tWaithUntil:
                self.fail("Temporary sstables after stress run did not disappear within {} seconds".format(grace_time))

            # wait a little while and retry the check
            time.sleep(1)

        # Memoize the "final" sstables before cleanup (there are no "tmp" ones at this point).
        sstables_before_cleanup = set(node1.get_sstables_via_sstableutil("keyspace1", "standard1", sstabletype="final"))

        # Run cleanup
        node1.nodetool("cleanup -j {} keyspace1 standard1".format(jobs))

        # Even when 'cleanup' has finished, old sstables are probably still present and new
        # sstables not yet there or still in "tmp" state. It may take a while until old sstables
        # disappear and new ones are present and "final". Wait up to 2 minutes.
        grace_time = 120
        tWaithUntil = time.time() + grace_time
        while True:

            # Current list of sstables
            current_sstables = node1.get_sstables_via_sstableutil("keyspace1", "standard1", sstabletype="final")

            # Check if any 'old' sstable is still contained in the current list of sstables
            overlap = reduce((lambda a, b: a | b), [sstable in sstables_before_cleanup for sstable in current_sstables])

            # Grab "tmp" sstables - there should be none after the cleanup at some point in the future
            tmp_sstables = node1.get_sstables_via_sstableutil("keyspace1", "standard1", sstabletype="tmp")

            if not overlap and len(tmp_sstables) == 0:
                # no old sstables present, check if the number of sstables is equal
                self.assertEqual(len(sstables_before_cleanup), len(current_sstables),
                                 "Expected {} sstables, but have {} final sstables".format(len(sstables_before_cleanup), len(current_sstables)))
                break

            # Assertion shall fail, if (not all) old sstables have disappeared and time to wait (2 minutes) has elapsed
            if time.time() >= tWaithUntil:
                self.fail("""Old sstables were not cleaned up within {} seconds.
                Old sstables: {}
                Current sstables: {}
                Temp sstables: {}
                """.format(grace_time, sstables_before_cleanup, current_sstables, tmp_sstables))

            # Wait a little while and retry the check
            time.sleep(1)

    def _test_cleanup_pre30(self, node1):
        """
        @jira_ticket CASSANDRA-11179
        Make sure we remove processed files during cleanup
        """
        event = threading.Event()
        failed = threading.Event()
        jobs = 1
        thread = threading.Thread(target=self._monitor_datadir, args=(node1, event, len(node1.get_sstables("keyspace1", "standard1")), jobs, failed))
        thread.start()
        node1.nodetool("cleanup -j {} keyspace1 standard1".format(jobs))
        event.set()
        thread.join()
        self.assertFalse(failed.is_set())

    def _monitor_datadir(self, node, event, basecount, jobs, failed):
        while True:
            sstables = [s for s in node.get_sstables("keyspace1", "standard1") if "tmplink" not in s]
            debug("---")
            for sstable in sstables:
                debug(sstable)
            if len(sstables) > basecount + jobs:
                debug("Current count is {}, basecount was {}".format(len(sstables), basecount))
                failed.set()
                return
            if event.is_set():
                return
            time.sleep(.1)

    def _cleanup(self, node):
        commitlog_dir = os.path.join(node.get_path(), 'commitlogs')
        for data_dir in node.data_directories():
            debug("Deleting {}".format(data_dir))
            shutil.rmtree(data_dir)
        shutil.rmtree(commitlog_dir)

    def failed_bootstrap_with_auth_test(self):
        """
        @jira_ticket DB-1274
        Test that failed streaming will not cause NPE on auth
        """
        cluster = self.cluster
        cluster.populate(1, install_byteman=True)
        cluster.set_configuration_options(values={'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                                                  'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer'})
        cluster.start()
        cluster.wait_for_any_log('Created default superuser', 25)

        # write some data, enough for the bootstrap to fail later on
        debug("Prepare data for streaming")
        node1 = cluster.nodelist()[0]
        node1.stress(['write', 'n=100K', 'no-warmup', '-rate', 'threads=8', '-mode native cql3', 'user=cassandra', 'password=cassandra'])
        node1.flush()

        session = self.patient_cql_connection(node1, user='cassandra', password='cassandra')
        session.shutdown()

        # kills the node right before it starts sending a file
        node1.byteman_submit(['./byteman/interrupt_bootstrap.btm'])

        # Add a new node, bootstrap=True ensures that it is not a seed
        node2 = new_node(cluster, bootstrap=True)
        node2.start()

        debug("Check node2 failed to receive data")
        node2.watch_log_for("Some data streaming failed.", timeout=120)

        node1.stop()  # looks strange to stop a killed node, but ccm hasn't realized yet that the node is actually down
        self.assertFalse(node1.is_running())

        debug("Start node1")
        node1.start()

        debug("Connect to node1")
        session = self.patient_cql_connection(node1, user='cassandra', password='cassandra')
        session.shutdown()

        debug("Connect to node2 and expect no-host-exception because unable to fetch auth data during bootstrapping")
        with self.assertRaises(NoHostAvailable):
            self.patient_cql_connection(node2, user='cassandra', password='cassandra')

    def bootstrap_write_survey_with_auth_test(self):
        """
        @jira_ticket DB-1274
        Test bootstrap with auth, write_survey, join_ring
        """
        cluster = self.cluster
        cluster.populate(1)
        cluster.set_configuration_options(values={'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                                                  'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer'})
        cluster.start()
        cluster.wait_for_any_log('Created default superuser', 25)

        # write some data, enough for the bootstrap to fail later on
        debug("Prepare data for streaming")
        node1 = cluster.nodelist()[0]
        node1.stress(['write', 'n=100K', 'no-warmup', '-rate', 'threads=8', '-mode native cql3', 'user=cassandra', 'password=cassandra'])
        node1.flush()

        session = self.patient_cql_connection(node1, user='cassandra', password='cassandra')
        session.shutdown()

        # Add a new node, bootstrap=True ensures that it is not a seed
        debug("Bootstrap new node with write_survey=true and join_ring=true")
        node2 = new_node(cluster, bootstrap=True)
        node2.start(jvm_args=["-Dcassandra.write_survey=true", "-Dcassandra.join_ring=true"], wait_for_binary_proto=True)

        debug("Stream some data to node1 again")
        node1.stress(['write', 'n=100K', 'no-warmup', '-rate', 'threads=8', '-mode native cql3', 'user=cassandra', 'password=cassandra'])

        session = self.patient_cql_connection(node2, user='cassandra', password='cassandra')
        session.shutdown()