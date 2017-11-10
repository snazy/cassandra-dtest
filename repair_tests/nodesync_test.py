# -*- coding: UTF-8 -*-
import time

from ccmlib.node import ToolError

from dtest import Tester, debug
from tools.decorators import since
from tools.nodesync import nodesync_opts, assert_all_segments, not_validated, enable_nodesync, disable_nodesync
from tools.preparation import prepare, config_opts


class SingleTableNodeSyncTester(Tester):

    def prepare(self, nodes=2, rf=2, tokens=None):
        opts = None
        if tokens:
            opts = config_opts(tokens=tokens)
        debug('Creating cluster...')
        self.session = prepare(self, nodes=nodes, rf=rf, options=opts, nodesync_options=nodesync_opts())

    def create_table(self, nodesync=True):
        debug('Creating table t...')
        query = "CREATE TABLE ks.t (k int PRIMARY KEY) WITH nodesync={{ 'enabled' : '{}' }}".format(nodesync)
        self.session.execute(query)
        time.sleep(0.2)

    def do_inserts(self, inserts=1000):
        debug("Inserting data...")
        for i in range(0, inserts):
            self.session.execute("INSERT INTO ks.t(k) VALUES({})".format(i))

    def enable_nodesync(self):
        enable_nodesync(self.session, 'ks', 't')

    def disable_nodesync(self):
        disable_nodesync(self.session, 'ks', 't')

    def assert_in_sync(self, reenable_nodesync=True):
        """ Validate that all the node of the cluster are in sync for the test table (using --validate option to repair)

        Because anti-entropy repairs are not allowed on nodes with NodeSync, this method first disable NodeSync, re-enabling it after
        it has run. The 're-enabling after' behavior can be turned off with :reenable_nodesync.
        """
        # Need to disable NodeSync or the repairs will error out
        self.disable_nodesync()
        for node in self.cluster.nodelist():
            result = node.nodetool("repair ks t --validate")
            self.assertIn("Repaired data is in sync", result.stdout)
        if reenable_nodesync:
            self.enable_nodesync()

    def assert_all_segments(self, predicate=None):
        assert_all_segments(self.session, 'ks', 't', predicate=predicate)


@since('4.0')
class TestNodeSync(SingleTableNodeSyncTester):

    def test_decommission(self):
        self.prepare(nodes=4, rf=3)
        self.create_table()
        self.cluster.nodelist()[2].decommission()

    def test_no_replication(self):
        self.prepare(nodes=[2, 2], rf={'dc1': 3, 'dc2': 3})
        self.create_table()

        # reset RF at dc1 as 0
        self.session.execute("ALTER KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 0, 'dc2': 3};")

        # wait 10s for error
        time.sleep(10)

    def test_cannot_run_repair_on_nodesync_enabled_table(self):
        """
        * Check that ordinary repair cannot be run on NodeSync enabled table
        @jira_ticket APOLLO-966
        """
        self.ignore_log_patterns = [
            'Cannot run both full and incremental repair, choose either --full or -inc option.']
        self.prepare(nodes=2, rf=2)
        self.create_table(nodesync=False)

        node1 = self.node(1)
        # Check there is no problem executing repair on ks.table
        debug("Running repair on ks.t")
        node1.nodetool('repair ks t')

        # Now enable NodeSync - Check cannot run anti-entropy repair on NodeSync enabled table
        debug("Enabling nodesync on ks.t - cannot run repair")
        self.enable_nodesync()
        with self.assertRaises(ToolError) as ctx:
            node1.nodetool('repair ks t')
        self.assertIn('Cannot run anti-entropy repair on tables with NodeSync enabled', ctx.exception.stderr)

        # Now disable NodeSync on ks.t table - no problem in running repair
        debug("Disabling nodesync on ks.t - can run repair again")
        self.disable_nodesync()
        node1.nodetool('repair ks t')

    def test_basic_validation(self):
        """
        Validate basic behavior of NodeSync: that a table gets entirely validated and continuously so
        """
        self.prepare(nodes=2, rf=2)
        self.create_table(nodesync=False)
        self.do_inserts()

        # NodeSync is not yet running, so make sure there is no state
        self.assert_all_segments(not_validated())

        # Enable NodeSync and make sure everything gets validated
        debug("Enabling NodeSync and waiting on initial validations...")
        self.enable_nodesync()
        self.assert_all_segments()
        debug("Validating data is in sync (with repair --validate)...")
        self.assert_in_sync()

        # Now, test that segments gets continuously validated by repeatedly grabbing a timestamp and make sure
        # that everything gets validated past this timestamp.
        RUNS = 5
        for _ in range(0, RUNS):
            debug("Waiting on all segments to be re-validated from now")
            self.assert_all_segments()

    def test_validation_with_node_failure(self):
        """
        Validate that when a node dies, we do continue validating but those validation are not mark
        fully successful. And that when the node comes back, we get that to be fixed.
        """
        self.prepare(nodes=3, rf=3)
        self.create_table()
        self.do_inserts()

        # Initial sanity check
        debug("Checking everything validated...")
        self.assert_all_segments()

        debug("Stopping 3rd node...")
        self.node(3).stop()

        debug("Checking all partial validation...")
        # From that point on, no segment can be fully validated
        timestamp = time.time() * 1000
        self.assert_all_segments(predicate=lambda r: r.last_time > timestamp and not r.last_was_success and r.missing_nodes == {self.node(3).address()})

        debug("Restarting 3rd node...")
        self.node(3).start()

        debug("Checking everything now successfully validated...")
        self.assert_all_segments()
        self.assert_in_sync()

    def test_validation_with_node_move(self):
        """
        Validate data remains continue to be properly validated after a move.
        """
        self.prepare(nodes=3, rf=3, tokens=1)
        self.create_table()
        self.do_inserts()

        # Initial sanity check
        debug("Checking everything validated...")
        self.assert_all_segments()

        self.node(3).nodetool('move {}'.format(2**16))

        # Everything should still get eventually validated
        debug("Checking everything still validated...")
        self.assert_all_segments()
        self.assert_in_sync()
