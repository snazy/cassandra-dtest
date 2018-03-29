import os
import time
from collections import defaultdict
from distutils.version import LooseVersion  #pylint: disable=import-error
from nose.tools import assert_equal, assert_not_equal
from threading import Thread

import ccmlib.common
import parse
from ccmlib.node import ToolError
from dse import ConsistencyLevel as CL
from dse.cluster import ContinuousPagingOptions
from dse.policies import FallthroughRetryPolicy, WhiteListRoundRobinPolicy

from dtest import Tester, debug, get_ip_from_node, make_execution_profile
from tools.decorators import since
from tools.jmxutils import (JolokiaAgent, enable_jmx_ssl, make_mbean,
                            remove_perf_disable_shared_mem)
from tools.paging import ContinuousPageFetcher
from tools.sslkeygen import generate_ssl_stores

EXEC_PROFILE_CP_ONE = object()
EXEC_PROFILE_CP_ALL = object()


class TestJMX(Tester):

    def netstats_test(self):
        """
        Check functioning of nodetool netstats, especially with restarts.
        @jira_ticket CASSANDRA-8122, CASSANDRA-6577
        """

        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        node1.stress(['write', 'n=500K', 'no-warmup', '-schema', 'replication(factor=3)'])
        node1.flush()
        node1.stop(gently=False)

        with self.assertRaisesRegexp(ToolError, "ConnectException: 'Connection refused( \(Connection refused\))?'."):
            node1.nodetool('netstats')

        # don't wait; we're testing for when nodetool is called on a node mid-startup
        node1.start(wait_for_binary_proto=False)

        # until the binary interface is available, try `nodetool netstats`
        binary_interface = node1.network_interfaces['binary']
        time_out_at = time.time() + 30
        running = False
        while (not running and time.time() <= time_out_at):
            running = ccmlib.common.check_socket_listening(binary_interface, timeout=0.5)
            try:
                node1.nodetool('netstats')
            except Exception as e:
                self.assertNotIn('java.lang.reflect.UndeclaredThrowableException', str(e),
                                 'Netstats failed with UndeclaredThrowableException (CASSANDRA-8122)')
                if not isinstance(e, ToolError):
                    raise
                else:
                    self.assertRegexpMatches(str(e),
                                             "ConnectException: 'Connection refused( \(Connection refused\))?'.")

        self.assertTrue(running, msg='node1 never started')

    def table_metric_mbeans_test(self):
        """
        Test some basic table metric mbeans with simple writes.
        """
        cluster = self.cluster
        cluster.populate(3)
        node1, node2, node3 = cluster.nodelist()
        remove_perf_disable_shared_mem(node1)
        cluster.start()

        version = cluster.version()
        node1.stress(['write', 'n=10K', 'no-warmup', '-schema', 'replication(factor=3)'])

        typeName = "ColumnFamily" if version <= '2.2.X' else 'Table'
        debug('Version {} typeName {}'.format(version, typeName))

        # TODO the keyspace and table name are capitalized in 2.0
        memtable_size = make_mbean('metrics', type=typeName, keyspace='keyspace1', scope='standard1',
                                   name='AllMemtablesHeapSize')
        disk_size = make_mbean('metrics', type=typeName, keyspace='keyspace1', scope='standard1',
                               name='LiveDiskSpaceUsed')
        sstable_count = make_mbean('metrics', type=typeName, keyspace='keyspace1', scope='standard1',
                                   name='LiveSSTableCount')

        with JolokiaAgent(node1) as jmx:
            mem_size = jmx.read_attribute(memtable_size, "Value")
            self.assertGreater(int(mem_size), 10000)

            on_disk_size = jmx.read_attribute(disk_size, "Count")
            self.assertEquals(int(on_disk_size), 0)

            node1.flush()

            on_disk_size = jmx.read_attribute(disk_size, "Count")
            self.assertGreater(int(on_disk_size), 10000)

            sstables = jmx.read_attribute(sstable_count, "Value")
            self.assertGreaterEqual(int(sstables), 1)

    @since('3.0')
    def mv_metric_mbeans_release_test(self):
        """
        Test that the right mbeans are created and released when creating mvs
        """
        cluster = self.cluster
        cluster.populate(1)
        node = cluster.nodelist()[0]
        remove_perf_disable_shared_mem(node)
        cluster.start(wait_for_binary_proto=True)

        node.run_cqlsh(cmds="""
            CREATE KEYSPACE mvtest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor': 1 };
            CREATE TABLE mvtest.testtable (
                foo int,
                bar text,
                baz text,
                PRIMARY KEY (foo, bar)
            );

            CREATE MATERIALIZED VIEW mvtest.testmv AS
                SELECT foo, bar, baz FROM mvtest.testtable WHERE
                foo IS NOT NULL AND bar IS NOT NULL AND baz IS NOT NULL
            PRIMARY KEY (foo, bar, baz);""")

        table_memtable_size = make_mbean('metrics', type='Table', keyspace='mvtest', scope='testtable',
                                         name='AllMemtablesHeapSize')
        table_view_read_time = make_mbean('metrics', type='Table', keyspace='mvtest', scope='testtable',
                                          name='ViewReadTime')
        table_view_lock_time = make_mbean('metrics', type='Table', keyspace='mvtest', scope='testtable',
                                          name='ViewLockAcquireTime')
        mv_memtable_size = make_mbean('metrics', type='Table', keyspace='mvtest', scope='testmv',
                                      name='AllMemtablesHeapSize')
        mv_view_read_time = make_mbean('metrics', type='Table', keyspace='mvtest', scope='testmv',
                                       name='ViewReadTime')
        mv_view_lock_time = make_mbean('metrics', type='Table', keyspace='mvtest', scope='testmv',
                                       name='ViewLockAcquireTime')

        missing_metric_message = "Table metric %s should have been registered after creating table %s" \
                                 "but wasn't!"

        with JolokiaAgent(node) as jmx:
            self.assertIsNotNone(jmx.read_attribute(table_memtable_size, "Value"),
                                 missing_metric_message.format("AllMemtablesHeapSize", "testtable"))
            self.assertIsNotNone(jmx.read_attribute(table_view_read_time, "Count"),
                                 missing_metric_message.format("ViewReadTime", "testtable"))
            self.assertIsNotNone(jmx.read_attribute(table_view_lock_time, "Count"),
                                 missing_metric_message.format("ViewLockAcquireTime", "testtable"))
            self.assertIsNotNone(jmx.read_attribute(mv_memtable_size, "Value"),
                                 missing_metric_message.format("AllMemtablesHeapSize", "testmv"))
            self.assertRaisesRegexp(Exception, ".*InstanceNotFoundException.*", jmx.read_attribute,
                                    mbean=mv_view_read_time, attribute="Count", verbose=False)
            self.assertRaisesRegexp(Exception, ".*InstanceNotFoundException.*", jmx.read_attribute,
                                    mbean=mv_view_lock_time, attribute="Count", verbose=False)

        node.run_cqlsh(cmds="DROP KEYSPACE mvtest;")
        with JolokiaAgent(node) as jmx:
            self.assertRaisesRegexp(Exception, ".*InstanceNotFoundException.*", jmx.read_attribute,
                                    mbean=table_memtable_size, attribute="Value", verbose=False)
            self.assertRaisesRegexp(Exception, ".*InstanceNotFoundException.*", jmx.read_attribute,
                                    mbean=table_view_lock_time, attribute="Count", verbose=False)
            self.assertRaisesRegexp(Exception, ".*InstanceNotFoundException.*", jmx.read_attribute,
                                    mbean=table_view_read_time, attribute="Count", verbose=False)
            self.assertRaisesRegexp(Exception, ".*InstanceNotFoundException.*", jmx.read_attribute,
                                    mbean=mv_memtable_size, attribute="Value", verbose=False)
            self.assertRaisesRegexp(Exception, ".*InstanceNotFoundException.*", jmx.read_attribute,
                                    mbean=mv_view_lock_time, attribute="Count", verbose=False)
            self.assertRaisesRegexp(Exception, ".*InstanceNotFoundException.*", jmx.read_attribute,
                                    mbean=mv_view_read_time, attribute="Count", verbose=False)

    def test_compactionstats(self):
        """
        @jira_ticket CASSANDRA-10504
        @jira_ticket CASSANDRA-10427

        Test that jmx MBean used by nodetool compactionstats
        properly updates the progress of a compaction
        """

        cluster = self.cluster
        cluster.populate(1)
        node = cluster.nodelist()[0]
        remove_perf_disable_shared_mem(node)
        cluster.start()

        # Run a quick stress command to create the keyspace and table
        node.stress(['write', 'n=1', 'no-warmup'])
        # Disable compaction on the table
        node.nodetool('disableautocompaction keyspace1 standard1')
        node.nodetool('setcompactionthroughput 1')
        node.stress(['write', 'n=150K', 'no-warmup'])
        node.flush()
        # Run a major compaction. This will be the compaction whose
        # progress we track.
        node.nodetool_process('compact')
        # We need to sleep here to give compaction time to start
        # Why not do something smarter? Because if the bug regresses,
        # we can't rely on jmx to tell us that compaction started.
        time.sleep(5)

        compaction_manager = make_mbean('db', type='CompactionManager')
        with JolokiaAgent(node) as jmx:
            progress_string = jmx.read_attribute(compaction_manager, 'CompactionSummary')[0]

            # Pause in between reads
            # to allow compaction to move forward
            time.sleep(2)

            updated_progress_string = jmx.read_attribute(compaction_manager, 'CompactionSummary')[0]
            var = 'Compaction@{uuid}(keyspace1, standard1, {progress}/{total})bytes'
            progress = int(parse.search(var, progress_string).named['progress'])
            updated_progress = int(parse.search(var, updated_progress_string).named['progress'])

            debug(progress_string)
            debug(updated_progress_string)

            # We want to make sure that the progress is increasing,
            # and that values other than zero are displayed.
            self.assertGreater(updated_progress, progress)
            self.assertGreaterEqual(progress, 0)
            self.assertGreater(updated_progress, 0)

            # Block until the major compaction is complete
            # Otherwise nodetool will throw an exception
            # Give a timeout, in case compaction is broken
            # and never ends.
            start = time.time()
            max_query_timeout = 600
            debug("Waiting for compaction to finish:")
            while (len(jmx.read_attribute(compaction_manager, 'CompactionSummary')) > 0) and (
                    time.time() - start < max_query_timeout):
                debug(jmx.read_attribute(compaction_manager, 'CompactionSummary'))
                time.sleep(2)

    @since('2.2')
    def phi_test(self):
        """
        Check functioning of nodetool failuredetector.
        @jira_ticket CASSANDRA-9526
        """

        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        phivalues = node1.nodetool("failuredetector").stdout.splitlines()
        endpoint1Values = phivalues[1].split()
        endpoint2Values = phivalues[2].split()

        endpoint1 = endpoint1Values[0][1:-1]
        endpoint2 = endpoint2Values[0][1:-1]

        self.assertItemsEqual([endpoint1, endpoint2], ['127.0.0.2', '127.0.0.3'])

        endpoint1Phi = float(endpoint1Values[1])
        endpoint2Phi = float(endpoint2Values[1])

        max_phi = 2.0
        self.assertGreater(endpoint1Phi, 0.0)
        self.assertLess(endpoint1Phi, max_phi)

        self.assertGreater(endpoint2Phi, 0.0)
        self.assertLess(endpoint2Phi, max_phi)

    @since('3.0')
    def test_metrics_reporter(self):
        cluster = self.cluster
        cluster.populate(1)

        node = cluster.nodelist()[0]

        debug("Writing metrics yaml file")
        metrics_out = os.path.join(node.get_path(), 'logs', 'metrics.out')
        with open(os.path.join(node.get_conf_dir(), 'metrics.yaml'), 'w') as metrics_config:
            yaml_content = """
                console:
                  -
                    outfile: '{}'
                    period: 1
                    timeunit: 'SECONDS'
                    predicate:
                      color: "white"
                      useQualifiedName: true
                      patterns:
                        - ".+"
                """.format(metrics_out)
            metrics_config.write(yaml_content)
            metrics_config.flush()

        debug("Starting cluster")
        cluster.start(jvm_args=["-Dcassandra.metricsReporterConfigFile=metrics.yaml"])

        self.assertTrue(node.grep_log('Trying to load metrics-reporter-config from file: metrics.yaml'))
        # This will throw TimeoutException if the metrics log is not populated within 5 seconds
        node.watch_log_for('Timers', filename='metrics.out', timeout=5)
        node.watch_log_for('Counters', filename='metrics.out', timeout=5)
        node.watch_log_for('Histograms', filename='metrics.out', timeout=5)
        node.watch_log_for('Meters', filename='metrics.out', timeout=5)
        node.watch_log_for('Timers', filename='metrics.out', timeout=5)

    @since('3.11')
    def test_continuous_paging(self):
        """
        Verify the continuous paging metrics.
        """
        cluster = self.cluster
        cluster.populate(3)

        options = {
            'max_concurrent_sessions': 24,
            'max_session_pages': 10,
            'max_page_size_mb': 8,
            'max_local_query_time_ms': 5000
        }

        if self.cluster.version() < LooseVersion('4.0'):
            options['max_client_wait_time_ms'] = 30000

        cluster.set_configuration_options({'continuous_paging': options})

        node1 = cluster.nodelist()[0]
        remove_perf_disable_shared_mem(node1)
        cluster.start()

        # insert some data, we need replication factor 3 to ensure each node owns a copy of all the data,
        # so that continuous paging requests at CL.ONE will be executed with the locally optimized path
        node1.stress(['write', 'n=100K', 'no-warmup', '-schema', 'replication(factor=3)'])
        address = get_ip_from_node(node1)

        # retrieve some data with continuous paging, using both CL.ONE and CL.ALL so that both slow and
        # optimized continuous paging paths are used, use an exclusive connection, so that node1 receives both requests
        paging_options = ContinuousPagingOptions(max_pages_per_second=15)
        profiles = {
            EXEC_PROFILE_CP_ONE: make_execution_profile(consistency_level=CL.ONE,
                                                        load_balancing_policy=WhiteListRoundRobinPolicy([address]),
                                                        retry_policy=FallthroughRetryPolicy(),
                                                        continuous_paging_options=paging_options),
            EXEC_PROFILE_CP_ALL: make_execution_profile(consistency_level=CL.ALL,
                                                        load_balancing_policy=WhiteListRoundRobinPolicy([address]),
                                                        retry_policy=FallthroughRetryPolicy(),
                                                        continuous_paging_options=paging_options)
        }

        # execute two small CP queries, one local and one not, so that the mbeans are initialized and so that
        # the latency histograms have at least 1 operation registered for both the slow and optimized path
        session = self.patient_exclusive_cql_connection(node1, protocol_version=65, execution_profiles=profiles)
        session.default_fetch_size = 1000
        session.execute("SELECT * from keyspace1.standard1 LIMIT 1000", execution_profile=EXEC_PROFILE_CP_ONE)
        session.execute("SELECT * from keyspace1.standard1 LIMIT 1000", execution_profile=EXEC_PROFILE_CP_ALL)

        # create the metrics mbeans
        optimized_path_latency = make_mbean('metrics', type='ContinuousPaging', scope='OptimizedPathLatency', name='Latency')
        slow_path_latency = make_mbean('metrics', type='ContinuousPaging', scope='SlowPathLatency', name='Latency')
        live_sessions = make_mbean('metrics', type='ContinuousPaging', scope='', name='LiveSessions')
        pending_pages = make_mbean('metrics', type='ContinuousPaging', scope='', name='PendingPages')
        requests = make_mbean('metrics', type='ContinuousPaging', scope='', name='Requests')
        creation_failures = make_mbean('metrics', type='ContinuousPaging', scope='', name='CreationFailures')
        too_many_sessions = make_mbean('metrics', type='ContinuousPaging', scope='', name='TooManySessions')
        client_write_exceptions = make_mbean('metrics', type='ContinuousPaging', scope='', name='ClientWriteExceptions')
        failures = make_mbean('metrics', type='ContinuousPaging', scope='', name='Failures')
        waiting_time = make_mbean('metrics', type='ContinuousPaging', scope='WaitingTime', name='Latency')
        server_blocked = make_mbean('metrics', type='ContinuousPaging', scope='', name='ServerBlocked')
        server_blocked_latency = make_mbean('metrics', type='ContinuousPaging', scope='ServerBlockedLatency', name='Latency')

        collecting = True
        collected_values = defaultdict(list)

        def read_metrics_values(jmx):
            collected_values[optimized_path_latency].append(jmx.read_attribute(optimized_path_latency, "Count"))
            collected_values[slow_path_latency].append(jmx.read_attribute(slow_path_latency, "Count"))
            collected_values[live_sessions].append(jmx.read_attribute(live_sessions, "Value"))
            collected_values[pending_pages].append(jmx.read_attribute(pending_pages, "Value"))
            collected_values[requests].append(jmx.read_attribute(requests, "Count"))
            collected_values[creation_failures].append(jmx.read_attribute(creation_failures, "Count"))
            collected_values[too_many_sessions].append(jmx.read_attribute(too_many_sessions, "Count"))
            collected_values[client_write_exceptions].append(jmx.read_attribute(client_write_exceptions, "Count"))
            collected_values[failures].append(jmx.read_attribute(failures, "Count"))
            collected_values[waiting_time].append(jmx.read_attribute(waiting_time, "Count"))
            collected_values[server_blocked].append(jmx.read_attribute(server_blocked, "Count"))
            collected_values[server_blocked_latency].append(jmx.read_attribute(server_blocked_latency, "Count"))

        # histograms snapshots are created every metrics_histogram_update_interval_millis or 1 sec by default
        hist_update_interval_millis = node1.get_conf_option('metrics_histogram_update_interval_millis') or 1000
        debug('Metrics histogram snapshots updated every {} millis'.format(hist_update_interval_millis))

        # start real time monitoring of some metrics
        def check_real_time_metrics():
            with JolokiaAgent(node1) as jmx:
                while collecting:
                    read_metrics_values(jmx)
                    time.sleep(0.05)
                debug("Sleeping for {} seconds to ensure histogram's snapshots are updated"
                      .format(1 + hist_update_interval_millis / 1000.))
                time.sleep(1 + hist_update_interval_millis / 1000.)
                read_metrics_values(jmx)

        monitoring_thread = Thread(target=check_real_time_metrics)
        monitoring_thread.start()

        fetchers = []
        for _ in xrange(4):
            fut = session.execute_async("SELECT * from keyspace1.standard1", execution_profile=EXEC_PROFILE_CP_ONE)
            fetchers.append(ContinuousPageFetcher(fut, session.default_fetch_size))

        for _ in xrange(6):
            fut = session.execute_async("SELECT * from keyspace1.standard1", execution_profile=EXEC_PROFILE_CP_ALL)
            fetchers.append(ContinuousPageFetcher(fut, session.default_fetch_size))

        # wait until we've received all the pages
        for fetcher in fetchers:
            fetcher.wait(timeout=60)  # this will fail if the last page is not received

        # stop the monitoring thread
        collecting = False
        monitoring_thread.join(timeout=5)

        # Now print and check the metrics
        for metric, values in collected_values.iteritems():
            debug('{}: {}'.format(metric, values))

        self.assertGreater(max(collected_values[optimized_path_latency]), 0)
        self.assertGreater(max(collected_values[slow_path_latency]), 0)
        self.assertGreater(max(collected_values[live_sessions]), 0)
        self.assertGreater(max(collected_values[pending_pages]), 0)
        self.assertEquals(12, max(collected_values[requests]))
        self.assertEquals(0, max(collected_values[creation_failures]))
        self.assertEquals(0, max(collected_values[too_many_sessions]))
        self.assertEquals(0, max(collected_values[client_write_exceptions]))
        self.assertEquals(0, max(collected_values[failures]))
        self.assertGreater(max(collected_values[waiting_time]), 0)
        self.assertGreater(max(collected_values[server_blocked]), 0)
        self.assertGreater(max(collected_values[server_blocked_latency]), 0)

    @since('4.0')
    def test_set_get_batchlog_replay_throttle(self):
        """
        @jira_ticket CASSANDRA-13614

        Test that batchlog replay throttle can be set and get through JMX
        """
        cluster = self.cluster
        cluster.populate(2)
        node = cluster.nodelist()[0]
        remove_perf_disable_shared_mem(node)
        cluster.start()

        # Set and get throttle with JMX, ensuring that the rate change is logged
        with JolokiaAgent(node) as jmx:
            mbean = make_mbean('db', 'StorageService')
            jmx.write_attribute(mbean, 'BatchlogReplayThrottleInKB', 4096)
            self.assertTrue(len(node.grep_log('Updating batchlog replay throttle to 4096 KB/s, 2048 KB/s per endpoint',
                                              filename='debug.log')) > 0)
            self.assertEqual(4096, jmx.read_attribute(mbean, 'BatchlogReplayThrottleInKB'))

    @since('3.0')
    def test_read_coordination_metrics(self):
        """
        @jira_ticket APOLLO-637

        Test basic functionality of read coordination metrics.

        Debugging notes: this test relies on some behaviors that may not be maintained if this test
        is run in a very underpowered environment. In particular, the node1 read delay of 5ms must be
        sufficient to ensure that it answers read queries slower than node2 in order for the DES to prefer
        node2.
        """
        cluster = self.cluster
        supports_read_delay = cluster.version() >= '3.2'
        debug("Supports_read_delay " + str(supports_read_delay))
        cluster.populate(2)
        [node1, node2] = cluster.nodelist()
        remove_perf_disable_shared_mem(node1)
        node1.start(jvm_args=['-Dcassandra.test.read_iteration_delay_ms=5'])
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        session = self.patient_exclusive_cql_connection(node1)

        session.execute("CREATE KEYSPACE readksrf1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
        session.execute("CREATE KEYSPACE readksrf2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
        session.execute("CREATE TABLE readksrf1.tbl (key int PRIMARY KEY) WITH speculative_retry = 'NONE' AND dclocal_read_repair_chance = 0.0")
        session.execute("CREATE TABLE readksrf2.tbl (key int PRIMARY KEY) WITH speculative_retry = 'NONE' AND dclocal_read_repair_chance = 0.0")
        rf1_read_stmt = session.prepare("SELECT * FROM readksrf1.tbl WHERE key = ?")
        rf1_read_stmt.consistency_level = CL.ONE
        rf2_read_stmt = session.prepare("SELECT * FROM readksrf2.tbl WHERE key = ?")
        rf2_read_stmt.consistency_level = CL.ONE
        rf1_insert_stmt = session.prepare("INSERT INTO readksrf1.tbl (key) VALUES (?)")
        rf1_insert_stmt.consistency_level = CL.ALL
        rf2_insert_stmt = session.prepare("INSERT INTO readksrf2.tbl (key) VALUES (?)")
        rf2_insert_stmt.consistency_level = CL.ALL

        node1_latency = make_mbean('metrics', type='ReadCoordination',
                                   scope=node1.address(), name='ReplicaLatency')
        node2_latency = make_mbean('metrics', type='ReadCoordination',
                                   scope=node2.address(), name='ReplicaLatency')
        nonreplica_requests = make_mbean('metrics', type='ReadCoordination', name='LocalNodeNonreplicaRequests')
        preferred_other_replicas = make_mbean('metrics', type='ReadCoordination', name='PreferredOtherReplicas')
        des = make_mbean('db', type='DynamicEndpointSnitch')

        # we find keys with node1 and node2 as primary so that we can ensure
        key_with_node1_primary = None
        key_with_node2_primary = None
        key = 0
        while not key_with_node1_primary or not key_with_node2_primary:
            out, _, _ = node1.nodetool("getendpoints readksrf1 tbl {}".format(key))
            address = out.split('\n')[-2]
            if node1.address() == address:
                key_with_node1_primary = key
            elif node2.address() == address:
                key_with_node2_primary = key
            key = key + 1

        with JolokiaAgent(node1) as jmx:
            # node2 is primary replica for key 1, node1 is primary replica for key 3
            session.execute(rf1_insert_stmt, [key_with_node2_primary])
            session.execute(rf1_insert_stmt, [key_with_node1_primary])
            session.execute(rf2_insert_stmt, [key_with_node2_primary])
            session.execute(rf2_insert_stmt, [key_with_node1_primary])
            # do some reads to populate snitch - we use degraded reads on node1 to ensure queries
            # are routed to node2 when both nodes are replicas
            if supports_read_delay:
                for x in range(0, 2000):
                    session.execute(rf1_insert_stmt, [key_with_node1_primary])
                    session.execute(rf1_read_stmt, [key_with_node1_primary])
                    session.execute(rf1_insert_stmt, [key_with_node2_primary])
                    session.execute(rf1_read_stmt, [key_with_node2_primary])
                time.sleep(0.1)

            # This should increase the nonreplica count, since it goes to node1 and node2 is the primary
            # replica. This should not increase the preferred other replicas count, since node1 is not a replica.
            nonreplica_requests_before = jmx.read_attribute(nonreplica_requests, 'Count')
            preferred_other_replicas_before = jmx.read_attribute(preferred_other_replicas, 'Count')
            session.execute(rf1_read_stmt, [key_with_node2_primary])
            assert_equal(nonreplica_requests_before + 1, jmx.read_attribute(nonreplica_requests, 'Count'))
            assert_equal(preferred_other_replicas_before, jmx.read_attribute(preferred_other_replicas, 'Count'))

            # This shouldn't increase the nonreplica count, since node1 owns this data. It should also
            # not increase the preferred other replicas count, since we necessarily select node1 due to
            # the rf1 replication.
            nonreplica_requests_before = jmx.read_attribute(nonreplica_requests, 'Count')
            preferred_other_replicas_before = jmx.read_attribute(preferred_other_replicas, 'Count')
            session.execute(rf1_read_stmt, [key_with_node1_primary])
            assert_equal(nonreplica_requests_before, jmx.read_attribute(nonreplica_requests, 'Count'))
            assert_equal(preferred_other_replicas_before, jmx.read_attribute(preferred_other_replicas, 'Count'))

            # We can only conveniently test preferred other replicas when read_delay works. Since node1
            # is degraded, we should prefer node2 but not see this as a nonreplica request.
            debug("Scores " + str(jmx.read_attribute(des, 'Scores')))
            nonreplica_requests_before = jmx.read_attribute(nonreplica_requests, 'Count')
            preferred_other_replicas_before = jmx.read_attribute(preferred_other_replicas, 'Count')
            session.execute(rf2_read_stmt, [key_with_node1_primary])
            assert_equal(nonreplica_requests_before, jmx.read_attribute(nonreplica_requests, 'Count'))
            if supports_read_delay:
                assert_equal(preferred_other_replicas_before + 1, jmx.read_attribute(preferred_other_replicas, 'Count'))

            # Above reads should have populated latencies.
            assert_not_equal(0, jmx.read_attribute(node1_latency, 'Count'))
            assert_not_equal(0, jmx.read_attribute(node2_latency, 'Count'))


@since('3.9')
class TestJMXSSL(Tester):
    keystore_password = 'cassandra'
    truststore_password = 'cassandra'

    def truststore(self):
        return os.path.join(self.test_path, 'truststore.jks')

    def keystore(self):
        return os.path.join(self.test_path, 'keystore.jks')

    def jmx_connection_test(self):
        """
        Check connecting with a JMX client (via nodetool) where SSL is enabled for JMX
        @jira_ticket CASSANDRA-12109
        """
        cluster = self._populateCluster(require_client_auth=False)
        node = cluster.nodelist()[0]
        cluster.start()

        self.assert_insecure_connection_rejected(node)

        node.nodetool("info --ssl -Djavax.net.ssl.trustStore={ts} -Djavax.net.ssl.trustStorePassword={ts_pwd}"
                      .format(ts=self.truststore(), ts_pwd=self.truststore_password))

    def require_client_auth_test(self):
        """
        Check connecting with a JMX client (via nodetool) where SSL is enabled and
        client certificate auth is also configured
        @jira_ticket CASSANDRA-12109
        """
        cluster = self._populateCluster(require_client_auth=True)
        node = cluster.nodelist()[0]
        cluster.start()

        self.assert_insecure_connection_rejected(node)

        # specifying only the truststore containing the server cert should fail
        with self.assertRaisesRegexp(ToolError, ".*SSLHandshakeException.*"):
            node.nodetool("info --ssl -Djavax.net.ssl.trustStore={ts} -Djavax.net.ssl.trustStorePassword={ts_pwd}"
                          .format(ts=self.truststore(), ts_pwd=self.truststore_password))

        # when both truststore and a keystore containing the client key are supplied, connection should succeed
        node.nodetool(
            "info --ssl -Djavax.net.ssl.trustStore={ts} -Djavax.net.ssl.trustStorePassword={ts_pwd} -Djavax.net.ssl.keyStore={ks} -Djavax.net.ssl.keyStorePassword={ks_pwd}"
            .format(ts=self.truststore(), ts_pwd=self.truststore_password, ks=self.keystore(),
                    ks_pwd=self.keystore_password))

    def assert_insecure_connection_rejected(self, node):
        """
        Attempts to connect to JMX (via nodetool) without any client side ssl parameters, expecting failure
        """
        with self.assertRaises(ToolError):
            node.nodetool("info")

    def _populateCluster(self, require_client_auth=False):
        cluster = self.cluster
        cluster.populate(1)

        generate_ssl_stores(self.test_path)
        if require_client_auth:
            ts = self.truststore()
            ts_pwd = self.truststore_password
        else:
            ts = None
            ts_pwd = None

        enable_jmx_ssl(cluster.nodelist()[0],
                       require_client_auth=require_client_auth,
                       keystore=self.keystore(),
                       keystore_password=self.keystore_password,
                       truststore=ts,
                       truststore_password=ts_pwd)
        return cluster
