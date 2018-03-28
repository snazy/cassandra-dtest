import time

from dse import ConsistencyLevel
from nose.tools import assert_equal

from dtest import Tester
from tools.decorators import since
from tools.jmxutils import (JolokiaAgent, make_mbean,
                            remove_perf_disable_shared_mem)


class TestSpeculativeRetry(Tester):

    def _attempt_speculative_retry(self, speculative_retry_config, read_iteration_delay):
        cluster = self.cluster
        cluster.populate(3)
        node1 = cluster.nodelist()[0]
        # we lower read_request_timeout because it determines how often speculative retry thresholds
        # are recalculated
        cluster.set_configuration_options(values={'read_request_timeout_in_ms': read_iteration_delay * 2})
        remove_perf_disable_shared_mem(node1)

        node1.start(jvm_args=['-Dcassandra.allow_unsafe_join=true'])
        # slow reads on nodes 2 and 3 so that we speculatively retry no matter which nodes chosen for local quorum read
        cluster.start(wait_for_binary_proto=30, wait_other_notice=True, jvm_args=['-Dcassandra.test.read_iteration_delay_ms=' + str(read_iteration_delay)])
        session = self.patient_exclusive_cql_connection(node1)
        session.execute("CREATE KEYSPACE specretrytestks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}")
        # need to eliminate read repair chance to have predictable number of hosts contacted
        session.execute("CREATE TABLE specretrytestks.tbl1 (key int PRIMARY KEY) WITH speculative_retry = '" + speculative_retry_config + "' AND dclocal_read_repair_chance = 0.0")
        read_stmt = session.prepare("SELECT * FROM specretrytestks.tbl1 where key = ?")
        read_stmt.consistency_level = ConsistencyLevel.QUORUM
        insert_stmt = session.prepare("INSERT INTO specretrytestks.tbl1 (key) VALUES (?)")
        insert_stmt.consistency_level = ConsistencyLevel.ALL
        with JolokiaAgent(node1) as jmx:
            # populate coordinator read latency with fast reads and sleep
            # if it isn't a fixed speculative retry threshold
            if speculative_retry_config.endswith('percentile'):
                for i in range(0, 10):
                    session.execute(read_stmt, [i])

                # sleep for read_request_timeout to ensure retry threshold is recalculated
                time.sleep(read_iteration_delay / 1000.0 * 2)

            # insert data so read_iteration_delay_ms will slow subsequent reads
            session.execute(insert_stmt, [1])

            speculative_retry_count = make_mbean('metrics', type='ColumnFamily',
                                                 keyspace='specretrytestks', scope='tbl1',
                                                 name='SpeculativeRetries')
            spec_retries_before = jmx.read_attribute(speculative_retry_count, 'Count')
            session.execute(read_stmt, [1])
            spec_retries_after = jmx.read_attribute(speculative_retry_count, 'Count')
            # check that we speculatively retried on the previous read
            assert_equal(1, spec_retries_after - spec_retries_before)

    @since('3.10')
    def test_attempt_percentile_speculative_retry(self):
        '''
        Provides rudimentary coverage that a speculative retry can be attempted using a percentile configuration
        '''
        self._attempt_speculative_retry('99percentile', 500)

    @since('3.10')
    def test_attempt_custom_speculative_retry(self):
        '''
        Provides rudimentary coverage that a speculative retry can be attempted using a custom time configuration
        '''
        self._attempt_speculative_retry('250ms', 500)
