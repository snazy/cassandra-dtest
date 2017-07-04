import os
import time

import ccmlib.common
import parse

from collections import defaultdict
from ccmlib.node import ToolError
from threading import Thread

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
                    self.assertRegexpMatches(str(e), "ConnectException: 'Connection refused( \(Connection refused\))?'.")

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
        memtable_size = make_mbean('metrics', type=typeName, keyspace='keyspace1', scope='standard1', name='AllMemtablesHeapSize')
        disk_size = make_mbean('metrics', type=typeName, keyspace='keyspace1', scope='standard1', name='LiveDiskSpaceUsed')
        sstable_count = make_mbean('metrics', type=typeName, keyspace='keyspace1', scope='standard1', name='LiveSSTableCount')

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
            while (len(jmx.read_attribute(compaction_manager, 'CompactionSummary')) > 0) and (time.time() - start < max_query_timeout):
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
                    period: 100
                    timeunit: 'MILLISECONDS'
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
        self.assertTrue(node.grep_log('Timers', filename='metrics.out'))
        self.assertTrue(node.grep_log('Counters', filename='metrics.out'))
        self.assertTrue(node.grep_log('Histograms', filename='metrics.out'))
        self.assertTrue(node.grep_log('Meters', filename='metrics.out'))
        self.assertTrue(node.grep_log('Timers', filename='metrics.out'))

    @since('3.11')
    def test_continuous_paging(self):
        """
        Verify the continuous paging metrics.
        """
        cluster = self.cluster
        cluster.populate(3)

        cluster.set_configuration_options({
            'continuous_paging': {
                'max_concurrent_sessions': 24,
                'max_session_pages': 10,
                'max_page_size_mb': 8,
                'max_client_wait_time_ms': 30000,
                'max_local_query_time_ms': 5000
            }
        })

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

        # execute a small CP query so that the mbeans are initialized
        session = self.patient_exclusive_cql_connection(node1, protocol_version=65, execution_profiles=profiles)
        session.default_fetch_size = 1000
        session.execute("SELECT * from keyspace1.standard1 LIMIT 1000", execution_profile=EXEC_PROFILE_CP_ONE)

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

        # start real time monitoring of some metrics
        def check_real_time_metrics():
            with JolokiaAgent(node1) as jmx:
                while collecting:
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
                    time.sleep(0.05)

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
        self.assertEquals(11, max(collected_values[requests]))
        self.assertEquals(0, max(collected_values[creation_failures]))
        self.assertEquals(0, max(collected_values[too_many_sessions]))
        self.assertEquals(0, max(collected_values[client_write_exceptions]))
        self.assertEquals(0, max(collected_values[failures]))
        self.assertGreater(max(collected_values[waiting_time]), 0)
        self.assertGreater(max(collected_values[server_blocked]), 0)
        self.assertGreater(max(collected_values[server_blocked_latency]), 0)


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
        node.nodetool("info --ssl -Djavax.net.ssl.trustStore={ts} -Djavax.net.ssl.trustStorePassword={ts_pwd} -Djavax.net.ssl.keyStore={ks} -Djavax.net.ssl.keyStorePassword={ks_pwd}"
                      .format(ts=self.truststore(), ts_pwd=self.truststore_password, ks=self.keystore(), ks_pwd=self.keystore_password))

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
