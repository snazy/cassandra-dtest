import os
import os.path

from dtest import DISABLE_VNODES, Tester, debug
from tools.decorators import since


@since('4.0')
class TestTPCStartup(Tester):
    """
    @jira_ticket APOLLO-939
    """

    def tpc_balance_stress_test(self):
        """
        Verify that we end up with the correct assignment of tpc cores after startup / restart.
        Nodes would start with memtables using only core 0 before APOLLO-939, causing everything to be
        processed there. This was visible in high READ_SWITCH_FOR_MEMTABLE counts (~ 50k * (cores-1)/cores).

        @jira_ticket APOLLO-939
        """
        cluster = self.cluster
        if not DISABLE_VNODES:
            cluster.set_configuration_options(values={'num_tokens': 256})
        cluster.populate(1).start()
        node1 = cluster.nodes['node1']
        limit = 1000

        # Check  we are using the correct cores for writes
        node1.stress(['write', 'n=50k', 'no-warmup', '-rate', 'threads=100'])
        out, err, _ = node1.nodetool('tpstats')
        debug(out)
        for line in out.split(os.linesep):
            if 'WRITE_SWITCH_FOR_MEMTABLE' in line:
                switches = line.split()[3]
                self.assertTrue(int(switches) < limit, "Too many switches for memtable on write: " + switches)

        # Check  we are using the correct cores for reads
        node1.stress(['read', 'n=50k', 'no-warmup', '-rate', 'threads=100'])
        out, err, _ = node1.nodetool('tpstats')
        debug(out)
        for line in out.split(os.linesep):
            if 'READ_SWITCH_FOR_MEMTABLE' in line:
                switches = line.split()[3]
                self.assertTrue(int(switches) < limit, "Too many switches for memtable on read before flush: " + switches)

        # Check  we are using the correct cores for reads after stopping and restarting with replay
        node1.stop(gently=False)
        node1.start(wait_for_binary_proto=True)

        node1.stress(['read', 'n=50k', 'no-warmup', '-rate', 'threads=100'])
        out, err, _ = node1.nodetool('tpstats')
        debug(out)
        for line in out.split(os.linesep):
            if 'READ_SWITCH_FOR_MEMTABLE' in line:
                switches = line.split()[3]
                self.assertTrue(int(switches) < limit, "Too many switches for memtable on read after restart: " + switches)

        # Check  we are using the correct cores for reads after flush with empty memtables
        cluster.flush()
        node1.stress(['read', 'n=50k', 'no-warmup', '-rate', 'threads=100'])
        out, err, _ = node1.nodetool('tpstats')
        debug(out)
        for line in out.split(os.linesep):
            if 'READ_SWITCH_FOR_MEMTABLE' in line:
                switches = line.split()[3]
                self.assertTrue(int(switches) < limit * 2, "Too many switches for memtable on read after flush: " + switches)

        # Check  we are using the correct cores for reads after restart with empty memtables
        node1.stop(gently=True)
        node1.start(wait_for_binary_proto=True)

        node1.stress(['read', 'n=50k', 'no-warmup', '-rate', 'threads=100'])
        out, err, _ = node1.nodetool('tpstats')
        debug(out)
        for line in out.split(os.linesep):
            if 'READ_SWITCH_FOR_MEMTABLE' in line:
                switches = line.split()[3]
                self.assertTrue(int(switches) < limit, "Too many switches for memtable on read after flush and restart: " + switches)
