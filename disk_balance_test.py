import os
import os.path
import re

from ccmlib.node import Node
from dtest import DISABLE_VNODES, Tester, debug
from tools.assertions import assert_almost_equal
from tools.data import create_c1c2_table, create_ks, insert_c1c2, query_c1c2
from tools.decorators import since
from tools.jmxutils import (JolokiaAgent, make_mbean,
                            remove_perf_disable_shared_mem)
from tools.misc import new_node
from compaction_test import grep_sstables_in_each_level


@since('3.2')
class TestDiskBalance(Tester):
    """
    @jira_ticket CASSANDRA-6696
    """

    def disk_balance_stress_test(self):
        cluster = self.cluster
        if not DISABLE_VNODES:
            cluster.set_configuration_options(values={'num_tokens': 256})
        cluster.populate(4).start()
        node1 = cluster.nodes['node1']

        node1.stress(['write', 'n=50k', 'no-warmup', '-rate', 'threads=100', '-schema', 'replication(factor=3)', 'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)'])
        cluster.flush()
        # make sure the data directories are balanced:
        for node in cluster.nodelist():
            self.assert_balanced(node)

    def disk_balance_bootstrap_test(self):
        cluster = self.cluster
        if not DISABLE_VNODES:
            cluster.set_configuration_options(values={'num_tokens': 256})
        # apparently we have legitimate errors in the log when bootstrapping (see bootstrap_test.py)
        self.allow_log_errors = True
        cluster.populate(4).start()
        node1 = cluster.nodes['node1']

        node1.stress(['write', 'n=50k', 'no-warmup', '-rate', 'threads=100', '-schema', 'replication(factor=3)', 'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)'])
        cluster.flush()
        node5 = new_node(cluster)
        node5.start(wait_for_binary_proto=True)
        self.assert_balanced(node5)

    def disk_balance_replace_same_address_test(self):
        self._test_disk_balance_replace(same_address=True)

    def disk_balance_replace_different_address_test(self):
        self._test_disk_balance_replace(same_address=False)

    def _test_disk_balance_replace(self, same_address):
        debug("Creating cluster")
        cluster = self.cluster
        if not DISABLE_VNODES:
            cluster.set_configuration_options(values={'num_tokens': 256})
        # apparently we have legitimate errors in the log when bootstrapping (see bootstrap_test.py)
        self.allow_log_errors = True
        cluster.populate(4).start(wait_for_binary_proto=True)
        node1 = cluster.nodes['node1']

        debug("Populating")
        node1.stress(['write', 'n=50k', 'no-warmup', '-rate', 'threads=100', '-schema', 'replication(factor=3)', 'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)'])
        cluster.flush()

        debug("Stopping and removing node2")
        node2 = cluster.nodes['node2']
        node2.stop(gently=False)
        self.cluster.remove(node2)

        node5_address = node2.address() if same_address else '127.0.0.5'
        debug("Starting replacement node")
        node5 = Node('node5', cluster=self.cluster, auto_bootstrap=True,
                     thrift_interface=None, storage_interface=(node5_address, 7000),
                     jmx_port='7500', remote_debug_port='0', initial_token=None,
                     binary_interface=(node5_address, 9042))
        self.cluster.add(node5, False)
        node5.start(jvm_args=["-Dcassandra.replace_address_first_boot={}".format(node2.address())],
                    wait_for_binary_proto=True,
                    wait_other_notice=True)

        debug("Checking replacement node is balanced")
        self.assert_balanced(node5)

    def disk_balance_decommission_test(self):
        cluster = self.cluster
        if not DISABLE_VNODES:
            cluster.set_configuration_options(values={'num_tokens': 256})
        cluster.populate(4).start()
        node1 = cluster.nodes['node1']
        node4 = cluster.nodes['node4']
        node1.stress(['write', 'n=50k', 'no-warmup', '-rate', 'threads=100', '-schema', 'replication(factor=2)', 'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)'])
        cluster.flush()

        node4.decommission()

        for node in cluster.nodelist():
            node.nodetool('relocatesstables')

        for node in cluster.nodelist():
            self.assert_balanced(node)

    def blacklisted_directory_test(self):
        cluster = self.cluster
        cluster.set_datadir_count(3)
        cluster.populate(1)
        [node] = cluster.nodelist()
        remove_perf_disable_shared_mem(node)
        cluster.start()

        session = self.patient_cql_connection(node)
        create_ks(session, 'ks', 1)
        create_c1c2_table(self, session)
        insert_c1c2(session, n=10000)
        node.flush()
        for k in xrange(0, 10000):
            query_c1c2(session, k)

        node.compact()
        mbean = make_mbean('db', type='BlacklistedDirectories')
        with JolokiaAgent(node) as jmx:
            jmx.execute_method(mbean, 'markUnwritable', [os.path.join(node.get_path(), 'data0')])

        for k in xrange(0, 10000):
            query_c1c2(session, k)

        node.nodetool('relocatesstables')

        for k in xrange(0, 10000):
            query_c1c2(session, k)

    def alter_replication_factor_test(self):
        cluster = self.cluster
        if not DISABLE_VNODES:
            cluster.set_configuration_options(values={'num_tokens': 256})
        cluster.populate(3).start()
        node1 = cluster.nodes['node1']
        node1.stress(['write', 'n=1', 'no-warmup', '-rate', 'threads=100', '-schema', 'replication(factor=1)'])
        cluster.flush()
        session = self.patient_cql_connection(node1)
        session.execute("ALTER KEYSPACE keyspace1 WITH replication = {'class':'SimpleStrategy', 'replication_factor':2}")
        node1.stress(['write', 'n=100k', 'no-warmup', '-rate', 'threads=100'])
        cluster.flush()
        for node in cluster.nodelist():
            self.assert_balanced(node)

    def assert_balanced(self, node):
        sums = []
        for sstabledir in node.get_sstables_per_data_directory('keyspace1', 'standard1'):
            sum = 0
            for sstable in sstabledir:
                sum = sum + os.path.getsize(sstable)
            sums.append(sum)
        assert_almost_equal(*sums, error=0.1, error_message=node.name)

    @since('3.10')
    def disk_balance_after_boundary_change_stcs_test(self):
        """
        @jira_ticket CASSANDRA-13948
        """
        self._disk_balance_after_boundary_change_test(lcs=False)

    @since('3.10')
    def disk_balance_after_boundary_change_lcs_test(self):
        """
        @jira_ticket CASSANDRA-13948
        """
        self._disk_balance_after_boundary_change_test(lcs=True)

    def _disk_balance_after_boundary_change_test(self, lcs):
        """
            @jira_ticket CASSANDRA-13948

            - Creates a 1 node cluster with 5 disks and insert data with compaction disabled
            - Bootstrap a node2 to make disk boundary changes on node1
            - Enable compaction on node1 and check disks are balanced
            - Decommission node1 to make disk boundary changes on node2
            - Enable compaction on node2 and check disks are balanced
        """

        cluster = self.cluster
        if not DISABLE_VNODES:
            cluster.set_configuration_options(values={'num_tokens': 1024})
        num_disks = 5
        cluster.set_datadir_count(num_disks)
        cluster.set_configuration_options(values={'concurrent_compactors': num_disks})

        debug("Starting node1 with {} data dirs and concurrent_compactors".format(num_disks))
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        # reduce system_distributed RF to 1 so we don't require forceful decommission
        session.execute("ALTER KEYSPACE system_distributed WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'};")
        session.execute("ALTER KEYSPACE system_traces WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'};")

        num_flushes = 10
        keys_per_flush = 10000
        keys_to_write = num_flushes * keys_per_flush

        compaction_opts = "LeveledCompactionStrategy,sstable_size_in_mb=1" if lcs else "SizeTieredCompactionStrategy"
        debug("Writing {} keys in {} flushes (compaction_opts={})".format(keys_to_write, num_flushes, compaction_opts))
        total_keys = num_flushes * keys_per_flush
        current_keys = 0
        while current_keys < total_keys:
            start_key = current_keys + 1
            end_key = current_keys + keys_per_flush
            debug("Writing keys {}..{} and flushing".format(start_key, end_key))
            node1.stress(['write', 'n={}'.format(keys_per_flush), "no-warmup", "cl=ALL", "-pop",
                          "seq={}..{}".format(start_key, end_key), "-rate", "threads=1", "-schema", "replication(factor=1)",
                          "compaction(strategy={},enabled=false)".format(compaction_opts)])
            node1.nodetool('flush keyspace1 standard1')
            current_keys = end_key

        # Add a new node, so disk boundaries will change
        debug("Bootstrap node2 and flush")
        node2 = new_node(cluster, bootstrap=True)
        node2.start(wait_for_binary_proto=True, jvm_args=["-Dcassandra.migration_task_wait_in_seconds=10"], set_migration_task=False)
        node2.flush()

        self._assert_balanced_after_boundary_change(node1, total_keys, lcs)

        debug("Decommissioning node1")
        node1.decommission()
        node1.stop()

        self._assert_balanced_after_boundary_change(node2, total_keys, lcs)

    @since('3.10')
    def disk_balance_after_joining_ring_stcs_test(self):
        """
        @jira_ticket CASSANDRA-13948
        """
        self._disk_balance_after_joining_ring_test(lcs=False)

    @since('3.10')
    def disk_balance_after_joining_ring_lcs_test(self):
        """
        @jira_ticket CASSANDRA-13948
        """
        self._disk_balance_after_joining_ring_test(lcs=True)

    def _disk_balance_after_joining_ring_test(self, lcs):
        """
            @jira_ticket CASSANDRA-13948

            - Creates a 3 node cluster with 5 disks and insert data with compaction disabled
            - Stop node1
            - Start node1 without joining gossip and loading ring state so disk boundaries will not reflect actual ring state
            - Join node1 to the ring to make disk boundaries change
            - Enable compaction on node1 and check disks are balanced
        """

        cluster = self.cluster
        if not DISABLE_VNODES:
            cluster.set_configuration_options(values={'num_tokens': 1024})
        num_disks = 5
        cluster.set_datadir_count(num_disks)
        cluster.set_configuration_options(values={'concurrent_compactors': num_disks})

        debug("Starting 3 nodes with {} data dirs and concurrent_compactors".format(num_disks))
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        num_flushes = 10
        keys_per_flush = 10000
        keys_to_write = num_flushes * keys_per_flush

        compaction_opts = "LeveledCompactionStrategy,sstable_size_in_mb=1" if lcs else "SizeTieredCompactionStrategy"
        debug("Writing {} keys in {} flushes (compaction_opts={})".format(keys_to_write, num_flushes, compaction_opts))
        total_keys = num_flushes * keys_per_flush
        current_keys = 0
        while current_keys < total_keys:
            start_key = current_keys + 1
            end_key = current_keys + keys_per_flush
            debug("Writing keys {}..{} and flushing".format(start_key, end_key))
            node1.stress(['write', 'n={}'.format(keys_per_flush), "no-warmup", "cl=ALL", "-pop",
                          "seq={}..{}".format(start_key, end_key), "-rate", "threads=1", "-schema", "replication(factor=1)",
                          "compaction(strategy={},enabled=false)".format(compaction_opts)])
            node1.nodetool('flush keyspace1 standard1')
            current_keys = end_key

        debug("Stopping node1")
        node1.stop()

        debug("Starting node1 without joining ring")
        node1.start(wait_for_binary_proto=True, wait_other_notice=False, join_ring=False,
                    jvm_args=["-Dcassandra.load_ring_state=false", "-Dcassandra.write_survey=true"])

        debug("Joining node1 to the ring")
        node1.nodetool("join")
        node1.nodetool("join")  # Need to run join twice - one to join ring, another to leave write survey mode

        self._assert_balanced_after_boundary_change(node1, total_keys, lcs)

    def _assert_balanced_after_boundary_change(self, node, total_keys, lcs):
        debug("Cleanup {}".format(node.name))
        node.cleanup()

        debug("Enabling compactions on {} now that boundaries changed".format(node.name))
        node.nodetool('enableautocompaction')

        debug("Waiting for compactions on {}".format(node.name))
        node.wait_for_compactions()

        debug("Disabling compactions on {} should not block forever".format(node.name))
        node.nodetool('disableautocompaction')

        debug("Major compact {} and check disks are balanced".format(node.name))
        node.compact()

        node.wait_for_compactions()
        self.assert_balanced(node)

        debug("Reading data back ({} keys)".format(total_keys))
        node.stress(['read', 'n={}'.format(total_keys), "no-warmup", "cl=ALL", "-pop", "seq=1...{}".format(total_keys), "-rate", "threads=1"])

        if lcs:
            output = grep_sstables_in_each_level(node, "standard1")
            debug("SSTables in each level: {}".format(output))

            # [0, ?/, 0, 0, 0, 0...]
            p = re.compile(r'(\d+)(/\d+)?,\s(\d+).*')
            m = p.search(output)
            cs_count = int(m.group(1)) + int(m.group(3))
            sstable_count = len(node.get_sstables('keyspace1', 'standard1'))
            debug("Checking that compaction strategy sstable # ({}) is equal to actual # ({})".format(cs_count, sstable_count))
            self.assertEqual(sstable_count, cs_count)
            self.assertFalse(node.grep_log("is already present on leveled manifest"))
