import time
from ccmlib.node import TimeoutError, ToolError
from dse import ConsistencyLevel
from threading import Thread

from dtest import Tester, debug
from tools.assertions import assert_length_equal, assert_nodetool_error
from tools.data import create_cf, create_ks, insert_c1c2, query_c1c2
from tools.decorators import no_vnodes, since


class TestRebuild(Tester):
    ignore_log_patterns = (
        # This one occurs when trying to send the migration to a
        # node that hasn't started yet, and when it does, it gets
        # replayed and everything is fine.
        r'Can\'t send migration request: node.*is down',
        # ignore streaming error during bootstrap
        r'Exception encountered during startup',
        r'Streaming error occurred'
    )

    def simple_rebuild_test(self):
        """
        @jira_ticket CASSANDRA-9119

        Test rebuild from other dc works as expected.
        """

        keys = 1000

        cluster = self.cluster
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.PropertyFileSnitch'})
        node1 = cluster.create_node('node1', False,
                                    None,
                                    ('127.0.0.1', 7000),
                                    '7100', '2000', None,
                                    binary_interface=('127.0.0.1', 9042))
        cluster.add(node1, True, data_center='dc1')

        # start node in dc1
        node1.start(wait_for_binary_proto=True)

        # populate data in dc1
        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, 'ks', {'dc1': 1})
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, n=keys, consistency=ConsistencyLevel.LOCAL_ONE)

        # check data
        for i in xrange(0, keys):
            query_c1c2(session, i, ConsistencyLevel.LOCAL_ONE)
        session.shutdown()

        # Bootstrapping a new node in dc2 with auto_bootstrap: false
        node2 = cluster.create_node('node2', False,
                                    ('127.0.0.2', 9160),
                                    ('127.0.0.2', 7000),
                                    '7200', '2001', None,
                                    binary_interface=('127.0.0.2', 9042))
        cluster.add(node2, False, data_center='dc2')
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # wait for snitch to reload
        time.sleep(60)
        # alter keyspace to replicate to dc2
        session = self.patient_exclusive_cql_connection(node2)
        session.execute("ALTER KEYSPACE ks WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1};")
        if self.cluster.version() >= '2.2':
            # alter system_auth -- rebuilding it no longer possible after
            # CASSANDRA-11848 prevented local node from being considered a source
            # Only do this on 2.2+, because on 2.1, this keyspace only
            # exists if auth is enabled, which it isn't in this test
            session.execute("ALTER KEYSPACE system_auth WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1};")
        session.execute('USE ks')

        self.rebuild_errors = 0

        # rebuild dc2 from dc1
        def rebuild():
            try:
                node2.nodetool('rebuild dc1')
            except ToolError as e:
                if 'Node is still rebuilding' in e.stdout:
                    self.rebuild_errors += 1
                else:
                    raise e

        class Runner(Thread):
            def __init__(self, func):
                Thread.__init__(self)
                self.func = func
                self.thread_exc_info = None

            def run(self):
                """
                Closes over self to catch any exceptions raised by func and
                register them at self.thread_exc_info
                Based on http://stackoverflow.com/a/1854263
                """
                try:
                    self.func()
                except Exception:
                    import sys
                    self.thread_exc_info = sys.exc_info()

        cmd1 = Runner(rebuild)
        cmd1.start()

        # concurrent rebuild should not be allowed (CASSANDRA-9119)
        # (following sleep is needed to avoid conflict in 'nodetool()' method setting up env.)
        time.sleep(.1)
        # we don't need to manually raise exeptions here -- already handled
        rebuild()

        cmd1.join()

        # manually raise exception from cmd1 thread
        # see http://stackoverflow.com/a/1854263
        if cmd1.thread_exc_info is not None:
            raise cmd1.thread_exc_info[1], None, cmd1.thread_exc_info[2]

        # exactly 1 of the two nodetool calls should fail
        # usually it will be the one in the main thread,
        # but occasionally it wins the race with the one in the secondary thread,
        # so we check that one succeeded and the other failed
        self.assertEqual(self.rebuild_errors, 1,
                         msg='rebuild errors should be 1, but found {}. Concurrent rebuild should not be allowed, but one rebuild command should have succeeded.'.format(self.rebuild_errors))

        # check data
        for i in xrange(0, keys):
            query_c1c2(session, i, ConsistencyLevel.LOCAL_ONE)

    @since('2.2')
    def resumable_rebuild_test(self):
        """
        @jira_ticket CASSANDRA-10810

        Test rebuild operation is resumable
        """
        self.ignore_log_patterns = list(self.ignore_log_patterns) + [r'Error while rebuilding node',
                                                                     r'Streaming error occurred on session with peer 127.0.0.3',
                                                                     r'Remote peer 127.0.0.3 failed stream session']
        cluster = self.cluster
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.PropertyFileSnitch'})

        # Create 2 nodes on dc1
        node1 = cluster.create_node('node1', False,
                                    ('127.0.0.1', 9160),
                                    ('127.0.0.1', 7000),
                                    '7100', '2000', None,
                                    binary_interface=('127.0.0.1', 9042))
        node2 = cluster.create_node('node2', False,
                                    ('127.0.0.2', 9160),
                                    ('127.0.0.2', 7000),
                                    '7200', '2001', None,
                                    binary_interface=('127.0.0.2', 9042))

        cluster.add(node1, True, data_center='dc1')
        cluster.add(node2, True, data_center='dc1')

        node1.start(wait_for_binary_proto=True)
        node2.start(wait_for_binary_proto=True)

        # Insert data into node1 and node2
        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, 'ks', {'dc1': 1})
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, n=10000, consistency=ConsistencyLevel.ALL)
        key = list(range(10000, 20000))
        session = self.patient_exclusive_cql_connection(node2)
        session.execute('USE ks')
        insert_c1c2(session, keys=key, consistency=ConsistencyLevel.ALL)
        session.shutdown()

        # Create a new node3 on dc2
        node3 = cluster.create_node('node3', False,
                                    ('127.0.0.3', 9160),
                                    ('127.0.0.3', 7000),
                                    '7300', '2002', None,
                                    binary_interface=('127.0.0.3', 9042),
                                    byteman_port='8300')

        cluster.add(node3, False, data_center='dc2')

        node3.start(wait_other_notice=False, wait_for_binary_proto=True)

        # Wait for snitch to be refreshed
        time.sleep(5)

        # Alter necessary keyspace for rebuild operation
        session = self.patient_exclusive_cql_connection(node3)
        session.execute("ALTER KEYSPACE ks WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1};")
        session.execute("ALTER KEYSPACE system_auth WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1};")

        # Path to byteman script which makes the streaming to node2 throw an exception, making rebuild fail
        script = ['./byteman/inject_failure_streaming_to_node2.btm']
        node3.byteman_submit(script)

        # First rebuild must fail and data must be incomplete
        with self.assertRaises(ToolError, msg='Unexpected: SUCCEED'):
            debug('Executing first rebuild -> '),
            node3.nodetool('rebuild dc1')
        debug('Expected: FAILED')

        session.execute('USE ks')
        with self.assertRaises(AssertionError, msg='Unexpected: COMPLETE'):
            debug('Checking data is complete -> '),
            for i in xrange(0, 20000):
                query_c1c2(session, i, ConsistencyLevel.LOCAL_ONE)
        debug('Expected: INCOMPLETE')

        debug('Executing second rebuild -> '),
        node3.nodetool('rebuild dc1')
        debug('Expected: SUCCEED')

        # Check all streaming sessions completed, streamed ranges are skipped and verify streamed data
        node3.watch_log_for('All sessions completed')
        node3.watch_log_for('Skipping streaming those ranges.')
        debug('Checking data is complete -> '),
        for i in xrange(0, 20000):
            query_c1c2(session, i, ConsistencyLevel.LOCAL_ONE)
        debug('Expected: COMPLETE')

    @since('3.6')
    def rebuild_ranges_test(self):
        """
        @jira_ticket CASSANDRA-10406
        """
        keys = 1000

        cluster = self.cluster
        tokens = cluster.balanced_tokens_across_dcs(['dc1', 'dc2'])
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.PropertyFileSnitch'})
        cluster.set_configuration_options(values={'num_tokens': 1})
        node1 = cluster.create_node('node1', False,
                                    ('127.0.0.1', 9160),
                                    ('127.0.0.1', 7000),
                                    '7100', '2000', tokens[0],
                                    binary_interface=('127.0.0.1', 9042))
        node1.set_configuration_options(values={'initial_token': tokens[0]})
        cluster.add(node1, True, data_center='dc1')
        node1 = cluster.nodelist()[0]

        # start node in dc1
        node1.start(wait_for_binary_proto=True)

        # populate data in dc1
        session = self.patient_exclusive_cql_connection(node1)
        # ks1 will be rebuilt in node2
        create_ks(session, 'ks1', {'dc1': 1})
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, n=keys, consistency=ConsistencyLevel.ALL)
        # ks2 will not be rebuilt in node2
        create_ks(session, 'ks2', {'dc1': 1})
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, n=keys, consistency=ConsistencyLevel.ALL)
        session.shutdown()

        # Bootstraping a new node in dc2 with auto_bootstrap: false
        node2 = cluster.create_node('node2', False,
                                    ('127.0.0.2', 9160),
                                    ('127.0.0.2', 7000),
                                    '7200', '2001', tokens[1],
                                    binary_interface=('127.0.0.2', 9042))
        node2.set_configuration_options(values={'initial_token': tokens[1]})
        cluster.add(node2, False, data_center='dc2')
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # wait for snitch to reload
        time.sleep(60)
        # alter keyspace to replicate to dc2
        session = self.patient_exclusive_cql_connection(node2)
        session.execute("ALTER KEYSPACE ks1 WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1};")
        session.execute("ALTER KEYSPACE ks2 WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1};")
        session.execute('USE ks1')

        # rebuild only ks1 with range that is node1's replica
        node2.nodetool('rebuild -ks ks1 -ts (%s,%s] dc1' % (tokens[1], str(pow(2, 63) - 1)))

        # check data is sent by stopping node1
        node1.stop()
        for i in xrange(0, keys):
            query_c1c2(session, i, ConsistencyLevel.ONE)
        # ks2 should not be streamed
        session.execute('USE ks2')
        for i in xrange(0, keys):
            query_c1c2(session, i, ConsistencyLevel.ONE, tolerate_missing=True, must_be_missing=True)

    @since('3.10')
    @no_vnodes()
    def disallow_rebuild_nonlocal_range_test(self):
        """
        @jira_ticket CASSANDRA-9875
        Verifies that nodetool rebuild throws an error when an operator
        attempts to rebuild a range that does not actually belong to the
        current node

        1. Set up a 3 node cluster
        2. Create a new keyspace with replication factor 2
        3. Run rebuild on node1 with a range that it does not own and assert that an error is raised
        """
        cluster = self.cluster
        tokens = cluster.balanced_tokens(3)
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.PropertyFileSnitch'})
        cluster.set_configuration_options(values={'num_tokens': 1})

        cluster.populate(3)
        node1, node2, node3 = cluster.nodelist()

        node1_token, node2_token, node3_token = tokens[:3]
        node1.set_configuration_options(values={'initial_token': node1_token})
        node2.set_configuration_options(values={'initial_token': node2_token})
        node3.set_configuration_options(values={'initial_token': node3_token})
        cluster.start(wait_for_binary_proto=True)

        session = self.patient_exclusive_cql_connection(node1)
        session.execute("CREATE KEYSPACE ks1 WITH replication = {'class':'SimpleStrategy', 'replication_factor':2};")

        with self.assertRaisesRegexp(ToolError, 'is not a range that is owned by this node'):
            node1.nodetool('rebuild -ks ks1 -ts (%s,%s] DC1' % (node1_token, node2_token))

    @since('3.10')
    @no_vnodes()
    def disallow_rebuild_from_nonreplica_test(self):
        """
        @jira_ticket CASSANDRA-9875
        Verifies that nodetool rebuild throws an error when an operator
        attempts to rebuild a range and specifies sources that are not
        replicas of that range.

        1. Set up a 3 node cluster
        2. Create a new keyspace with replication factor 2
        3. Run rebuild on node1 with a specific range using a source that
           does not own the range and assert that an error is raised
        """
        cluster = self.cluster
        tokens = cluster.balanced_tokens(3)
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.PropertyFileSnitch'})
        cluster.set_configuration_options(values={'num_tokens': 1})

        cluster.populate(3)
        node1, node2, node3 = cluster.nodelist()

        node1_token, node2_token, node3_token = tokens[:3]
        node1.set_configuration_options(values={'initial_token': node1_token})
        node2.set_configuration_options(values={'initial_token': node2_token})
        node3.set_configuration_options(values={'initial_token': node3_token})
        cluster.start(wait_for_binary_proto=True)

        node3_address = node3.network_interfaces['binary'][0]

        session = self.patient_exclusive_cql_connection(node1)
        session.execute("CREATE KEYSPACE ks1 WITH replication = {'class':'SimpleStrategy', 'replication_factor':2};")

        with self.assertRaisesRegexp(ToolError, 'Unable to find sufficient sources for streaming range'):
            node1.nodetool('rebuild -ks ks1 -ts (%s,%s] -s %s' % (node3_token, node1_token, node3_address))

    @since('3.10')
    @no_vnodes()
    def rebuild_with_specific_sources_test(self):
        """
        @jira_ticket CASSANDRA-9875
        Verifies that an operator can specify specific sources to use
        when rebuilding.

        1. Set up a 2 node cluster across dc1 and dc2
        2. Create new keyspaces with replication factor 2 (one replica in each datacenter)
        4. Populate nodes with data
        5. Create a new node in dc3 and update the keyspace replication
        6. Run rebuild on the new node with a specific source in dc2
        7. Assert that streaming only occurred between the new node and the specified source
        8. Assert that the rebuild was successful by checking the data
        """
        keys = 1000

        cluster = self.cluster
        tokens = cluster.balanced_tokens_across_dcs(['dc1', 'dc2', 'dc3'])
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.PropertyFileSnitch'})
        cluster.set_configuration_options(values={'num_tokens': 1})

        cluster.populate([1, 1], tokens=tokens[:2])
        node1, node2 = cluster.nodelist()

        cluster.start(wait_for_binary_proto=True)

        # populate data in dc1, dc2
        session = self.patient_exclusive_cql_connection(node1)
        # ks1 will be rebuilt in node3
        create_ks(session, 'ks1', {'dc1': 1, 'dc2': 1})
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, n=keys, consistency=ConsistencyLevel.ALL)
        # ks2 will not be rebuilt in node3
        create_ks(session, 'ks2', {'dc1': 1, 'dc2': 1})
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, n=keys, consistency=ConsistencyLevel.ALL)
        session.shutdown()

        # bootstrap a new node in dc3 with auto_bootstrap: false
        node3 = cluster.create_node('node3', False,
                                    ('127.0.0.3', 9160),
                                    ('127.0.0.3', 7000),
                                    '7300', '2002', tokens[2],
                                    binary_interface=('127.0.0.3', 9042))
        cluster.add(node3, False, data_center='dc3')
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)

        # wait for snitch to reload
        time.sleep(60)
        # alter keyspace to replicate to dc3
        session = self.patient_exclusive_cql_connection(node3)
        session.execute("ALTER KEYSPACE ks1 WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1, 'dc3':1};")
        session.execute("ALTER KEYSPACE ks2 WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1, 'dc3':1};")
        session.execute('USE ks1')

        node2_address = node2.network_interfaces['binary'][0]
        node3_address = node3.network_interfaces['binary'][0]

        # rebuild only ks1, restricting the source to node2
        node3.nodetool('rebuild -ks ks1 -ts (%s,%s] -s %s' % (tokens[2], str(pow(2, 63) - 1), node2_address))

        # verify that node2 streamed to node3
        log_matches = node2.grep_log('Session with /%s is complete' % node3_address)
        self.assertTrue(len(log_matches) > 0)

        # verify that node1 did not participate
        log_matches = node1.grep_log('streaming plan for Rebuild')
        self.assertEqual(len(log_matches), 0)

        # check data is sent by stopping node1, node2
        node1.stop()
        node2.stop()
        for i in xrange(0, keys):
            query_c1c2(session, i, ConsistencyLevel.ONE)
        # ks2 should not be streamed
        session.execute('USE ks2')
        for i in xrange(0, keys):
            query_c1c2(session, i, ConsistencyLevel.ONE, tolerate_missing=True, must_be_missing=True)

    def _refetch_reset_iteration(self, rebuild_node, sessions, nodetoolCmd):
        """
        Stop the 4th node and populate _more_ data. Since the 4th node is down and hints are disabled,
        the 4th node won't get the new data when it starts.
        Then start the 4th node, issue the rebuild command and verify the data is on the 4th node.
        """

        # verify topology
        assert_length_equal(sessions, 3, "Expecting 3 driver sessions in param 'sessions'")
        nodes = rebuild_node.cluster.nodelist()
        assert_length_equal(nodes, 4, "Expecting exactly 4 nodes in the cluster")
        self.assertEqual(nodes[0].data_center, 'dc1', "1st node must be in dc1")
        self.assertEqual(nodes[1].data_center, 'dc1', "2nd node must be in dc1")
        self.assertEqual(nodes[2].data_center, 'dc2', "3rd node must be in dc2")
        self.assertEqual(nodes[3].data_center, 'dc2', "4th node must be in dc2")

        # ensure the test table is clean
        sessions[0].execute('TRUNCATE TABLE ks1.cf')

        keysInitial = xrange(0, 100)
        keysDown = xrange(100, 200)

        # insert initial values while all are nodes up
        insert_c1c2(sessions[0], keys=keysInitial, consistency=ConsistencyLevel.ALL, c1value='initial')

        # verify nodes 1, 2, 3 and 4 have the initial data
        for key in keysInitial:
            for session in sessions:
                query_c1c2(session, key, ConsistencyLevel.LOCAL_ONE, c1value='initial',
                           additional_error_text="all nodes must have initial data, key={}, session={}".format(key, sessions.index(session)))
        rebuild_node_session = self.patient_exclusive_cql_connection(rebuild_node, keyspace='ks1')
        for key in keysInitial:
            query_c1c2(rebuild_node_session, key, ConsistencyLevel.LOCAL_ONE, c1value='initial',
                       additional_error_text="all nodes must have initial data, key={}, rebuild-node".format(key))
        rebuild_node_session.shutdown()
        rebuild_node_session.cluster.shutdown()

        rebuild_node.stop(wait_other_notice=True)

        # update initial data
        insert_c1c2(sessions[0], keys=keysInitial, consistency=ConsistencyLevel.THREE, c1value='updated')
        # insert new values with 4th node down (so it doesn't get these mutations)
        insert_c1c2(sessions[0], keys=keysDown, consistency=ConsistencyLevel.THREE, c1value='down')

        # verify nodes 1, 2, 3 have the new data
        for key in keysInitial:
            for session in sessions:
                query_c1c2(session, key, ConsistencyLevel.LOCAL_ONE, c1value='updated',
                           additional_error_text="remaining nodes must have updated rows, key={}, session={}".format(key, sessions.index(session)))

        # start 4th node - no hints will be replayed since hints are disabled
        rebuild_node.start(wait_other_notice=True, wait_for_binary_proto=True)
        rebuild_node_session = self.patient_exclusive_cql_connection(rebuild_node, keyspace='ks1')

        # down node must have the old state of the initial data (before it went down)
        for key in keysInitial:
            query_c1c2(rebuild_node_session, key, ConsistencyLevel.LOCAL_ONE, c1value='initial',
                       additional_error_text="down node must not have updated rows, key={}".format(key))
        # down node must not have the added data while it was down
        for key in keysDown:
            query_c1c2(rebuild_node_session, key, ConsistencyLevel.LOCAL_ONE, tolerate_missing=True, must_be_missing=True,
                       additional_error_text="down node must not have new rows, key={}, rebuild-node".format(key))

        # rebuild refetch/reset
        rebuild_node.mark_log()
        rebuild_node.mark_log_for_errors()
        rebuild_node.nodetool(nodetoolCmd)
        # Expect the nodetool command to _not_ let the node log the "Streaming range ... from 127.0.0.3" message,
        # because that would indicate that it is streaming from that node, which is what don't want.
        with self.assertRaises(TimeoutError):
            rebuild_node.watch_log_for('Streaming range .* from endpoint /127.0.0.3 for keyspace ks1', from_mark=True, timeout=3)

        # down node must now have updated data
        for key in keysInitial:
            query_c1c2(rebuild_node_session, key, ConsistencyLevel.LOCAL_ONE, c1value='updated',
                       additional_error_text="down node must have updated rows after rebuild/reset/refetch, key={}".format(key))
        # down node must now have new data
        for key in keysDown:
            query_c1c2(rebuild_node_session, key, ConsistencyLevel.LOCAL_ONE, c1value='down',
                       additional_error_text="down node must have new rows after rebuild/reset/refetch, key={}".format(key))

        rebuild_node_session.shutdown()
        rebuild_node_session.cluster.shutdown()

    def _expect_bootstrap_error(self, node, jvm_args, expected_error):
        node.start(wait_other_notice=False, wait_for_binary_proto=False, jvm_args=jvm_args)
        node.watch_log_for(expected_error, timeout=60)
        # TL;DR: node didn't start successfully - have to set node.pid=None
        # Long version: The method expects an error during bootstrap. node.start() starts the process and stores
        # the PID in pid. However, since bootstrap fails (as expected), the process dies, but pid is still not None.
        # Following calls against node (e.g. stop() or start()) assume the node is still up, because pid still holds
        # the PID. Especially Node.__update_status() relies on the state of pid.
        node.pid = None
        node.mark_log_for_errors()

    @since("3.0")
    def rebuild_reset_refetch_test(self):
        """
        @jira_ticket APOLLO-581
        Add rebuild-mode option: normal, reset, refetch & add white-/blacklisting filtering for DCs, racks and nodes.
        """

        dse6 = self.cluster.version() >= '4.0'

        # prepare a cluster with 2 DCs and two nodes in each DC, using vnodes

        cluster = self.cluster
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.PropertyFileSnitch',
                                                  'auto_bootstrap': 'true',
                                                  # don't want hints - to make the tests work
                                                  'hinted_handoff_enabled': 'false',
                                                  # need a deterministic behavior for CL LOCAL_ONE reads (i.e. local node first)
                                                  'dynamic_snitch': 'false'})

        cluster.populate([2, 1])
        nodes = cluster.nodelist()

        cluster.start()

        sessions = list(map(lambda node: self.patient_exclusive_cql_connection(node), nodes))

        # check that an invalid rebuild mode is not accepted
        assert_nodetool_error(self, nodes[0],
                              'rebuild -m use_ai_mode',
                              '.*Unknown mode used for rebuild: use_ai_mode.*')

        if dse6:
            # Must error out if no sources have been specified
            assert_nodetool_error(self, nodes[0],
                                  'rebuild',
                                  '.*At least one of the specific_sources, exclude_sources, source_dc_names, exclude_dc_names '
                                  '\(or src-dc-name\) arguments must be specified for rebuild.*')

        # just check a single error - remaining error situations checked in StreamingOptionsTest.java
        assert_nodetool_error(self, nodes[0],
                              'rebuild --dcs dc1,dc1:Rack-1',
                              '.*The source_dc_names argument contains both a rack restriction and a datacenter '
                              'restriction for the dc1 datacenter.*')

        for ks in ['system_auth', 'system_traces', 'system_distributed']:
            sessions[0].execute("ALTER KEYSPACE {} WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'dc1':1, 'dc2':1}};".format(ks))

        # bootstrap a 2nd node in DC2

        rebuild_node = cluster.create_node(name='node4',
                                           auto_bootstrap=True,
                                           thrift_interface=('127.0.0.4', 9160),
                                           storage_interface=('127.0.0.4', 7000),
                                           binary_interface=('127.0.0.4', 9042),
                                           jmx_port='7400', remote_debug_port='2004',
                                           initial_token=None)

        cluster.add(rebuild_node, False, data_center='dc2')

        # bootstrap system property error test
        self._expect_bootstrap_error(rebuild_node, ['-Dcassandra.bootstrap.excludeDCs=dc1,dc1:Rack-1',
                                                    '-Dcassandra.ring_delay_ms=5000'],
                                     'org.apache.cassandra.exceptions.ConfigurationException: '
                                     'The cassandra.bootstrap.excludeDCs system property contains both a '
                                     'rack restriction and a datacenter restriction for the dc1 datacenter')

        # Specifying a non-existing source must not succeed.
        self._expect_bootstrap_error(rebuild_node, ['-Dcassandra.bootstrap.includeDCs=dc-moon',
                                                    '-Dcassandra.ring_delay_ms=5000'],
                                     "org.apache.cassandra.exceptions.ConfigurationException: DC 'dc-moon' is not a "
                                     "known DC in this cluster")

        # Specifying a non-existing source must not succeed.
        self._expect_bootstrap_error(rebuild_node, ['-Dcassandra.bootstrap.includeSources=123.123.123.123',
                                                    '-Dcassandra.ring_delay_ms=5000'],
                                     "org.apache.cassandra.exceptions.ConfigurationException: Source '/123.123.123.123' "
                                     "is not a known node in this cluster")

        # This cannot succeed either (consistent range movement doesn't work with "advanced" source filtering).
        # Consistent range movement requires to stream pending ranges from the node losing that range.
        self._expect_bootstrap_error(rebuild_node, ['-Dcassandra.bootstrap.includeDCs=dc1',
                                                    '-Dcassandra.ring_delay_ms=5000'],
                                     'java.lang.IllegalStateException: Unable to find sufficient sources for streaming range')

        # finally, this works - now without consistent range movement
        rebuild_node.start(wait_other_notice=True, wait_for_binary_proto=True,
                           jvm_args=['-Dcassandra.bootstrap.includeDCs=dc1',
                                     '-Dcassandra.ring_delay_ms=5000',
                                     '-Dcassandra.consistent.rangemovement=false'])

        # create table + populate
        create_ks(sessions[0], 'ks1', {'dc1': 2, 'dc2': 2})
        # note: RR+SR options are mandatory for the test !
        create_cf(sessions[0], 'cf', columns={'c1': 'text', 'c2': 'text'}, read_repair=0, speculative_retry='NONE')
        for session in sessions:
            session.execute('USE ks1')

        if dse6:
            # Specifying no rebuild source must not succeed.
            assert_nodetool_error(self, nodes[2],
                                  'rebuild -m refetch',
                                  'At least one of the specific_sources, exclude_sources, source_dc_names, exclude_dc_names '
                                  '\(or src-dc-name\) arguments must be specified for rebuild.')

        # Specifying a non-existing source must not succeed.
        assert_nodetool_error(self, nodes[2],
                              'rebuild -m refetch -dc dc-moon',
                              "DC 'dc-moon' is not a known DC in this cluster")

        # Specifying a non-existing source must not succeed.
        assert_nodetool_error(self, nodes[2],
                              'rebuild -m refetch -s 123.123.123.123',
                              "Source '/123.123.123.123' is not a known node in this cluster")

        # refetch, include DC
        self._refetch_reset_iteration(rebuild_node, sessions, 'rebuild -m refetch -dc dc1')

        # refetch, exclude DC
        self._refetch_reset_iteration(rebuild_node, sessions, 'rebuild -m refetch -xdc dc2')

        # reset, include DC
        self._refetch_reset_iteration(rebuild_node, sessions, 'rebuild -m reset -dc dc1')

        # reset, exclude DC
        self._refetch_reset_iteration(rebuild_node, sessions, 'rebuild -m reset -xdc dc2')

        # refetch, include source
        self._refetch_reset_iteration(rebuild_node, sessions, 'rebuild -m refetch -s 127.0.0.1,127.0.0.2')

        # refetch, exclude source
        self._refetch_reset_iteration(rebuild_node, sessions, 'rebuild -m refetch -x 127.0.0.3,127.0.0.4')

        # reset, include source
        self._refetch_reset_iteration(rebuild_node, sessions, 'rebuild -m reset -s 127.0.0.1,127.0.0.2')

        # reset, exclude source
        self._refetch_reset_iteration(rebuild_node, sessions, 'rebuild -m reset -x 127.0.0.3,127.0.0.4')
