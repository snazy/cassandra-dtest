import os
import os.path
import re

from dtest import Tester, debug
from tools.decorators import since
from tools.misc import new_node

limit = 1000


@since('4.0')
class TestTPCCores(Tester):
    def _stress_and_check_switches(self, node, op, op_in_message=None, op_limit=limit, op_count="50k"):
        node.stress([op, 'n=' + op_count, 'no-warmup', '-rate', 'threads=100'])
        out, err, _ = node.nodetool('tpstats')

        if (op_in_message is None):
            op_in_message = op
        debug(op_in_message)
        debug(out)
        switches = self._get_completed(out, op.upper() + '_SWITCH_FOR_MEMTABLE')
        self.assertTrue(switches < op_limit, "Too many switches for memtable on " + op_in_message + ": " + str(switches))

    def _get_completed(self, out, stat):
        for line in out.split(os.linesep):
            if stat in line:
                switches = line.split()[3]
                debug(stat + ': ' + switches)
                return int(switches)
        return 0

    def tpc_startup_cores_test(self):
        """
        Verify that we end up with the correct assignment of tpc cores after startup / restart.
        Nodes would start with memtables using only core 0 before APOLLO-939, causing everything to be
        processed there. This was visible in high READ_SWITCH_FOR_MEMTABLE counts (~ 50k * (cores-1)/cores).

        @jira_ticket APOLLO-939
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodes['node1']

        # Check  we are using the correct cores for writes
        self._stress_and_check_switches(node1, 'write')

        # Check  we are using the correct cores for reads
        self._stress_and_check_switches(node1, 'read', 'initial read')

        # Check  we are using the correct cores for reads after stopping and restarting with replay
        node1.stop(gently=False)
        node1.start(wait_for_binary_proto=True)
        self._stress_and_check_switches(node1, 'read', 'read after restart')

        # Check  we are using the correct cores for reads after flush with empty memtables
        cluster.flush()
        self._stress_and_check_switches(node1, 'read', 'read after flush', limit * 2)

        # Check  we are using the correct cores for reads after restart with empty memtables
        node1.stop(gently=True)
        node1.start(wait_for_binary_proto=True)
        self._stress_and_check_switches(node1, 'read', 'read after flush and restart')

    def _stress_and_check_balance(self, node, op, message, op_limit=limit, op_count="50k", switches_limit=limit):
        if op_count is not None:
            node.stress([op, 'n=' + op_count, 'no-warmup', '-rate', 'threads=100'])

        out, err, _ = node.nodetool('tpstats --cores')

        stat = re.compile('TPC/(\d|all)/' + op.upper() + ' ')
        debug(message)
        debug(out)
        total = 0
        cores = []
        counts = []

        for line in out.split(os.linesep):
            match = re.match(stat, line)
            if match is not None:
                count = int(line.split()[3])
                core = match.group(1)
                debug(op + ' on ' + core + ': ' + str(count))
                if (core == "all"):
                    total = count
                else:
                    counts += [count]
                    cores += [core]

        debug(cores)
        debug(counts)
        debug(total)

        expected = total / len(cores)
        failed = False
        for i in range(len(cores)):
            if counts[i] > expected + op_limit or counts[i] < expected - op_limit:
                failed = True
                debug("Expecting around " + str(expected) + " " + op + " operations on core " + cores[i] + " but got " + str(counts[i]) + " after " + message)

        self.assertFalse(failed)

        switches = self._get_completed(out, "TPC/all/" + op.upper() + "_SWITCH_FOR_MEMTABLE")
        if switches_limit is not None:
            self.assertTrue(switches < switches_limit, "Too many switches for memtable on " + message + ": " + str(switches))
        return switches

    def tpc_reassign_tokens_cores_test(self):
        """
        Verify that we end up with the correct assignment of tpc cores after token distribution change.

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
        self._stress_and_check_balance(node1, 'write', 'initial_write')

        # Add node
        node2 = new_node(cluster)
        node2.set_configuration_options(values={'initial_token': tokens[1]})
        node2.start(wait_for_binary_proto=True)

        # Check that read requests are properly distributed.
        # We haven't flushed, so they'd still jump for memtable.
        switches = self._stress_and_check_balance(node1, 'read', 'read on node1 after node added', switches_limit=None)

        node1.flush()

        # Check we no longer switch for memtable.
        self._stress_and_check_balance(node2, 'read', 'read on node1 after node added and flush')
        self._stress_and_check_balance(node1, 'read', 'read on node2 after node added', op_count=None, switches_limit=switches + limit)

        self._stress_and_check_balance(node1, 'write', 'write on node1 after node added', op_count='10k')
        self._stress_and_check_balance(node2, 'write', 'write on node2 after node added', op_count=None)
