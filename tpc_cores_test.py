import os
import os.path
import re

from dtest import Tester, debug
from tools.decorators import since
from tools.misc import new_node

default_op_count = 20000
tolerance = int(default_op_count / 20)
read_op = 'read'
write_op = 'write'
write_switch_op = 'write_switch_for_memtable'


@since('4.0')
class TestTPCCores(Tester):
    @staticmethod
    def _stress_and_get_tpstats(node, op, phase, op_count=default_op_count):
        node.stress([op, 'n=' + str(op_count), 'no-warmup', '-rate', 'threads=100'])
        out, err, _ = node.nodetool('tpstats --cores')
        debug(os.linesep + '=== ' + phase + ' ===')
        debug(out)
        return out

    @staticmethod
    def _get_completed(out, stat):
        for line in out.split(os.linesep):
            if "TPC/all/" + stat.upper() in line:
                completed = line.split()[3]
                return int(completed)
        return 0

    def _assert_count_expected(self, op, actual_count, expected_count, phase):
        upper_bound = expected_count + tolerance
        self.assertTrue(actual_count <= upper_bound,
                        'Too many {0} operations during \'{1}\': {2!s} (Expected at most {3!s})'
                        .format(op, phase, actual_count, upper_bound))
        lower_bound = expected_count - tolerance
        self.assertTrue(actual_count >= lower_bound,
                        'Too few {0} operations during \'{1}\': {2!s} (Expected at least {3!s})'
                        .format(op, phase, actual_count, lower_bound))
        debug('Actual {0} operations during \'{1}\' ({2!s}) correctly fit in the expected range [{3!s}, {4!s}]'
              .format(op, phase, actual_count, lower_bound, upper_bound))

    def tpc_startup_switch_cores_test(self):
        """
        Check that we end up with the correct assignment of TPC cores after node startup / restart in a single-node
        cluster, by verifying that requests don't need to switch cores too much.
        As single-partition reads are no longer switched to the core corresponding to their partition key if they are
        already on a TPC thread, this check applies solely for writes.
        Also verify that we report the correct number of reads / writes in the TPC stats.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodes['node1']

        # Check that we are not switching cores too much and that we are reporting the expected number of writes
        phase = 'initial write'
        num_writes = default_op_count
        tpstats = self._stress_and_get_tpstats(node1, write_op, phase, num_writes)
        self._assert_count_expected(write_switch_op, self._get_completed(tpstats, write_switch_op), 0, phase)
        self._assert_count_expected(write_op, self._get_completed(tpstats, write_op), num_writes, phase)

        # Check that we are reporting the expected number of reads
        phase = 'initial read'
        num_reads = default_op_count
        tpstats = self._stress_and_get_tpstats(node1, read_op, phase, num_reads)
        self._assert_count_expected(read_op, self._get_completed(tpstats, read_op), num_reads, phase)

        # Abruptly restart the node (will require commit log replay after start)
        node1.stop(gently=False)
        node1.start(wait_for_binary_proto=True)

        # Check that we are reporting the expected number of reads and write switches (the latter due to commit
        # log replay) after stopping and restarting
        phase = 'read after restart'
        tpstats = self._stress_and_get_tpstats(node1, read_op, phase, num_reads)
        self._assert_count_expected(read_op, self._get_completed(tpstats, read_op), num_reads, phase)
        self._assert_count_expected(write_switch_op, self._get_completed(tpstats, write_switch_op), num_writes, phase)

        # Check that we are not switching cores too much (i.e. that the number of switches has not changed since the
        # "read after restart" phase) and that we are reporting the expected number of writes after stopping and
        # restarting with replay
        phase = 'write after restart'
        tpstats = self._stress_and_get_tpstats(node1, write_op, phase, num_writes)
        self._assert_count_expected(write_switch_op, self._get_completed(tpstats, write_switch_op), num_writes, phase)
        self._assert_count_expected(write_op, self._get_completed(tpstats, write_op), num_writes, phase)

    @staticmethod
    def _is_balanced_distribution(total, cores, counts, op, phase):
        expected_per_core = int(total / len(cores))
        for i in range(len(cores)):
            upper_bound = expected_per_core + tolerance
            lower_bound = expected_per_core - tolerance
            if counts[i] > upper_bound or counts[i] < lower_bound:
                debug('Actual {0} operations during \'{1}\' ({2}) do not fit in the expected range [{3!s}, {4!s}]'
                      .format(op, phase, counts[i], lower_bound, upper_bound))
                return False
        return True

    def _stress_and_check_balance(self, node, op, phase, run_stress=True, op_count=default_op_count):
        if run_stress:
            node.stress([op, 'n=' + str(op_count), 'no-warmup', '-rate', 'threads=100'])

        out, err, _ = node.nodetool('tpstats --cores')
        debug(os.linesep + '=== ' + phase + ' ===')
        debug(out)

        stat = re.compile('TPC/(\d|all)/' + op.upper() + ' ')
        total = 0
        cores = []
        counts = []

        for line in out.split(os.linesep):
            match = re.match(stat, line)
            if match is not None:
                count = int(line.split()[3])
                core = match.group(1)
                debug('{0} on core {1}: {2!s}'.format(op, core, count))
                if core == "all":
                    total = count
                else:
                    counts += [count]
                    cores += [core]

        debug(cores)
        debug(counts)
        debug(total)

        self.assertTrue(self._is_balanced_distribution(total, cores, counts, op, phase))
        return out

    def _run_phase(self, node, op, phase, last_node_stats=None, run_stress=True, op_count=default_op_count):
        node_stats = self._stress_and_check_balance(node, op, phase, run_stress, op_count)
        last_op_count = 0
        if last_node_stats is not None:
            last_op_count = self._get_completed(last_node_stats, op)
        expected_op_count = last_op_count + op_count / len(self.cluster.nodelist())
        self._assert_count_expected(op, self._get_completed(node_stats, op), expected_op_count, phase)
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

        cluster = self.cluster

        tokens = cluster.balanced_tokens(2)
        cluster.set_configuration_options(values={'num_tokens': 1})

        debug("[node1, node2] tokens: %r" % (tokens,))

        # Create a single node cluster
        cluster.populate(1)
        node1 = cluster.nodelist()[0]
        node1.set_configuration_options(values={'initial_token': tokens[0]})
        cluster.start()

        # Do some initial loading to make sure it's balanced
        phase = 'initial write'
        node1_stats = self._run_phase(node1, write_op, phase)

        # Add node
        node2 = new_node(cluster)
        node2.set_configuration_options(values={'initial_token': tokens[1]})
        node2.start(wait_for_binary_proto=True)

        # Check that read requests are properly distributed after node addition, even without flush
        phase = 'read on node1 after node addition'
        node1_stats = self._run_phase(node1, read_op, phase, node1_stats)
        phase = 'check reads on node2 after read on node1'
        node2_stats = self._run_phase(node2, read_op, phase, None, False)

        node1.flush()

        # Check that read requests are properly distributed after flush, both across cores and across nodes
        phase = 'read on node2'
        node2_stats = self._run_phase(node2, read_op, phase, node2_stats)
        phase = 'check reads on node1 after flush and read on node2'
        node1_stats = self._run_phase(node1, read_op, phase, node1_stats, False)

        # Check that write requests are properly distributed after flush, both across cores and across nodes
        phase = 'write on node1 after flush'
        node1_stats = self._run_phase(node1, write_op, phase, node1_stats, True, 10000)
        phase = 'check writes on node2 after write on node1'
        node2_stats = self._run_phase(node2, write_op, phase, node2_stats, False, 10000)
