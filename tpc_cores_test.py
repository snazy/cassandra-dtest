import os
import os.path
import re

from dtest import Tester, debug
from tools.decorators import since
from tools.misc import new_node

default_op_count = 20000
tolerance = int(default_op_count / 20)
tpc_read_task = 'read_(remote|local)'
tpc_write_task = 'write_(remote|local)'
tpc_write_switch_task = 'write_switch_for_memtable'
prefix_tpc_read_tasks = 'read_'
prefix_tpc_write_tasks = 'write_'


@since('4.0')
class TestTPCCores(Tester):
    # TODO Calculate tolerance based on the currently used op_count, not based on some default
    @staticmethod
    def _get_tolerance(tpc_task):
        if re.match(prefix_tpc_read_tasks, tpc_task):
            return default_op_count / 5
        if re.match(prefix_tpc_write_tasks, tpc_task):
            return default_op_count / 20
        raise AssertionError('Expected a read or write TPC task, but got {}'.format(tpc_task))

    @staticmethod
    def _stress_and_get_tpstats(node, stress_op, phase, op_count=default_op_count):
        debug(os.linesep + '=== ' + phase + ' ===')
        node.stress([stress_op, 'n=' + str(op_count), 'no-warmup', '-rate', 'threads=100'])
        out, err, _ = node.nodetool('tpstats --cores')
        debug(out)
        return out

    @staticmethod
    def _get_all_completed(out, tpc_task):
        all_completed = 0
        tpc_task_pattern = re.compile('TPC/all/' + tpc_task.upper() + ' ')
        for line in out.split(os.linesep):
            if re.match(tpc_task_pattern, line):
                all_completed += int(line.split()[5])
        return all_completed

    def _assert_count_expected(self, tpc_task, actual_count, expected_count, phase):
        upper_bound = expected_count + TestTPCCores._get_tolerance(tpc_task)
        self.assertTrue(actual_count <= upper_bound,
                        'Too many {0} operations during \'{1}\': {2!s} (Expected at most {3!s})'
                        .format(tpc_task, phase, actual_count, upper_bound))
        lower_bound = expected_count - TestTPCCores._get_tolerance(tpc_task)
        self.assertTrue(actual_count >= lower_bound,
                        'Too few {0} operations during \'{1}\': {2!s} (Expected at least {3!s})'
                        .format(tpc_task, phase, actual_count, lower_bound))
        debug('Actual {0} operations during \'{1}\' ({2!s}) correctly fit in the expected range [{3!s}, {4!s}]'
              .format(tpc_task, phase, actual_count, lower_bound, upper_bound))

    def tpc_startup_switch_cores_test(self):
        """
        Check that we end up with the correct assignment of TPC cores after node startup / restart in a single-node
        cluster, by verifying that requests don't need to switch cores too much.
        As single-partition reads are no longer switched to the core corresponding to their partition key if they are
        already on a TPC thread, this check applies solely for writes.
        Also verify that we report the correct number of reads / writes in the TPC stats.
        """
        debug("Creating and starting a single-node cluster")
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        # Check that we are not switching cores too much and that we are reporting the expected number of writes
        phase = 'initial write'
        num_writes = default_op_count
        tpstats = TestTPCCores._stress_and_get_tpstats(node1, 'write', phase, num_writes)
        self._assert_count_expected(tpc_write_switch_task, TestTPCCores._get_all_completed(tpstats, tpc_write_switch_task), 0, phase)
        self._assert_count_expected(tpc_write_task, TestTPCCores._get_all_completed(tpstats, tpc_write_task), num_writes, phase)

        # Check that we are reporting the expected number of reads
        phase = 'initial read'
        num_reads = default_op_count
        tpstats = TestTPCCores._stress_and_get_tpstats(node1, 'read', phase, num_reads)
        self._assert_count_expected(tpc_read_task, TestTPCCores._get_all_completed(tpstats, tpc_read_task), num_reads, phase)

        debug(os.linesep + "Restarting a node abruptly (commit log replay will be needed)")
        node1.stop(gently=False)
        node1.start(wait_for_binary_proto=True)

        # Check that we are reporting the expected number of reads and write switches (the latter due to commit
        # log replay) after stopping and restarting
        phase = 'read after restart'
        tpstats = TestTPCCores._stress_and_get_tpstats(node1, 'read', phase, num_reads)
        self._assert_count_expected(tpc_read_task, TestTPCCores._get_all_completed(tpstats, tpc_read_task), num_reads, phase)
        self._assert_count_expected(tpc_write_switch_task, TestTPCCores._get_all_completed(tpstats, tpc_write_switch_task), num_writes, phase)

        # Check that we are not switching cores too much (i.e. that the number of switches has not changed since the
        # "read after restart" phase) and that we are reporting the expected number of writes after stopping and
        # restarting with replay
        phase = 'write after restart'
        tpstats = TestTPCCores._stress_and_get_tpstats(node1, 'write', phase, num_writes)
        self._assert_count_expected(tpc_write_switch_task, TestTPCCores._get_all_completed(tpstats, tpc_write_switch_task), num_writes, phase)
        self._assert_count_expected(tpc_write_task, TestTPCCores._get_all_completed(tpstats, tpc_write_task), num_writes, phase)

    @staticmethod
    def _is_balanced_distribution(total, cores, counts, tpc_task, phase):
        expected_per_core = int(total / len(cores))
        for i in range(len(cores)):
            upper_bound = expected_per_core + TestTPCCores._get_tolerance(tpc_task)
            lower_bound = expected_per_core - TestTPCCores._get_tolerance(tpc_task)
            if counts[i] > upper_bound or counts[i] < lower_bound:
                debug('Actual {0} operations during \'{1}\' ({2}) do not fit in the expected range [{3!s}, {4!s}]'
                      .format(tpc_task, phase, counts[i], lower_bound, upper_bound))
                return False
        return True

    def _stress_and_check_balance(self, node, tpc_task, phase, stress_op=False, op_count=default_op_count):
        if stress_op:
            node.stress([stress_op, 'n=' + str(op_count), 'no-warmup', '-rate', 'threads=100'])

        out, err, _ = node.nodetool('tpstats --cores')
        debug(out)

        stat = re.compile('TPC/(\d|all)/' + tpc_task.upper() + ' ')
        total = 0
        cores = []
        counts = []

        for line in out.split(os.linesep):
            match = re.match(stat, line)
            if match is not None:
                count = int(line.split()[5])
                core = match.group(1)
                debug('{0} on core {1}: {2!s}'.format(tpc_task, core, count))
                if core == "all":
                    total = count
                else:
                    counts += [count]
                    cores += [core]

        debug(cores)
        debug(counts)
        debug(total)

        self.assertTrue(TestTPCCores._is_balanced_distribution(total, cores, counts, tpc_task, phase))
        return out

    def _run_phase(self, node, tpc_task, phase, last_node_stats=None, stress_op=False, op_count=default_op_count):
        debug(os.linesep + '=== ' + phase + ' ===')
        node_stats = self._stress_and_check_balance(node, tpc_task, phase, stress_op, op_count)
        last_op_count = 0
        if last_node_stats is not None:
            last_op_count = TestTPCCores._get_all_completed(last_node_stats, tpc_task)
        expected_op_count = last_op_count + op_count / len(self.cluster.nodelist())
        self._assert_count_expected(tpc_task, TestTPCCores._get_all_completed(node_stats, tpc_task), expected_op_count, phase)
        return node_stats

    def tpc_ring_change_balance_cores_test(self):
        """
        Check that we end up with the correct assignment of TPC cores after token distribution change in a
        multi-node cluster, by verifying that each core receives nearly the same number of requests.
        Even though single-partition reads are no longer switched to the core corresponding to their partition key if
        they are already on a TPC thread, this check can still be applied to them.
        Naturally, this check also verifies that we report the correct number of reads / writes in the TPC stats.

        @jira_ticket APOLLO-694
        """

        debug("Creating and starting a single-node cluster")
        cluster = self.cluster
        tokens = cluster.balanced_tokens(2)
        cluster.set_configuration_options(values={'num_tokens': 1})
        cluster.populate(1)
        node1 = cluster.nodelist()[0]
        node1.set_configuration_options(values={'initial_token': tokens[0]})
        cluster.start(wait_for_binary_proto=True)

        # Do some initial loading to make sure it's balanced
        phase = 'initial write'
        node1_stats = self._run_phase(node1, tpc_write_task, phase, stress_op='write')

        debug(os.linesep + "Adding a new node to the cluster and rebalancing the ring")
        node2 = new_node(cluster)
        node2.set_configuration_options(values={'initial_token': tokens[1]})
        node2.start(wait_for_binary_proto=True)

        # Check that read requests are properly distributed after node addition, even without flush
        phase = 'read on node1 after node addition'
        node1_stats = self._run_phase(node1, tpc_read_task, phase, node1_stats, 'read')
        phase = 'check reads on node2 after read on node1'
        node2_stats = self._run_phase(node2, tpc_read_task, phase)

        node1.flush()

        # Check that read requests are properly distributed after flush, both across cores and across nodes
        phase = 'read on node2'
        node2_stats = self._run_phase(node2, tpc_read_task, phase, node2_stats, 'read')
        phase = 'check reads on node1 after flush and read on node2'
        node1_stats = self._run_phase(node1, tpc_read_task, phase, node1_stats)

        # Check that write requests are properly distributed after flush, both across cores and across nodes
        phase = 'write on node1 after flush'
        node1_stats = self._run_phase(node1, tpc_write_task, phase, node1_stats, 'write', 10000)
        phase = 'check writes on node2 after write on node1'
        node2_stats = self._run_phase(node2, tpc_write_task, phase, node2_stats, False, 10000)
