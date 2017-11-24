# -*- coding: UTF-8 -*-
import time

from ccmlib.node import ToolError

from dtest import Tester, debug
from tools.decorators import since
from tools.interceptors import fake_write_interceptor
from tools.nodesync import nodesync_opts, assert_all_segments, not_validated, enable_nodesync, disable_nodesync
from tools.preparation import prepare, config_opts


class SingleTableNodeSyncTester(Tester):

    def prepare(self, nodes=2, rf=2, num_tokens=None, interceptors=None):
        opts = None
        if num_tokens:
            opts = config_opts(num_tokens=num_tokens)
        debug('Creating cluster...')
        self.session = prepare(self, nodes=nodes, rf=rf, options=opts,
                               nodesync_options=nodesync_opts(), interceptors=interceptors)

    def create_table(self, nodesync=True):
        debug('Creating table t...')
        query = "CREATE TABLE ks.t (k int PRIMARY KEY) WITH nodesync={{ 'enabled' : '{}' }}".format(nodesync)
        self.session.execute(query)
        # This shouldn't be needed, so there may be some bug driver or service side which should be investigated,
        # but we seem to sometime gets traces server side with some node not knowing a particular table on reads
        # following this call without the sleep
        self.session.cluster.control_connection.wait_for_schema_agreement()

    def drop_table(self):
        debug('Dropping table t...')
        self.session.execute("DROP TABLE ks.t")

    def do_inserts(self, inserts=1000):
        debug("Inserting data...")
        for i in range(0, inserts):
            self.session.execute("INSERT INTO ks.t(k) VALUES({})".format(i))

    def enable_nodesync(self):
        enable_nodesync(self.session, 'ks', 't')

    def disable_nodesync(self):
        disable_nodesync(self.session, 'ks', 't')

    def assert_in_sync(self, reenable_nodesync=True):
        self.assertTrue(self.__check_in_sync(reenable_nodesync=reenable_nodesync))

    def assert_not_in_sync(self, reenable_nodesync=True):
        self.assertFalse(self.__check_in_sync(reenable_nodesync=reenable_nodesync))

    def __check_in_sync(self, reenable_nodesync=True):
        """ Validate that all the node of the cluster are in sync for the test table (using --validate option to repair)

        Because anti-entropy repairs are not allowed on nodes with NodeSync, this method first disable NodeSync, re-enabling it after
        it has run. The 're-enabling after' behavior can be turned off with :reenable_nodesync.
        """
        # Need to disable NodeSync or the repairs will error out
        self.disable_nodesync()
        in_sync = True
        for node in self.cluster.nodelist():
            # Note: we want --preview here as --validate only takes "repaired" data into account.
            result = node.nodetool("repair ks t --preview")
            if "Previewed data was in sync" not in result.stdout:
                in_sync = False
        if reenable_nodesync:
            self.enable_nodesync()
        return in_sync

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
        Validate basic behavior of NodeSync: that a table gets entirely validated and continuously so.
        This test also ensure that:
            - Disabling NodeSync on a table works properly.
            - Dropping a table with NodeSync enabled doesn't produce errors (and is acknowledged by NodeSync)
        """
        interceptor = fake_write_interceptor()
        self.prepare(nodes=2, rf=2, interceptors=interceptor)
        self.create_table(nodesync=False)

        # Start an interceptor that simulate losing half of the inserts on node2 (the writes are acknowledged
        # but not truly persisted).
        with interceptor.enable(self.node(2)) as interception:
            interception.set_option(interception_chance=0.5)
            self.do_inserts()
            self.assertGreater(interception.intercepted_count(), 0)

        # NodeSync is not yet running, so make sure there is no state
        self.assert_all_segments(not_validated())

        # Sanity check that there is some inconsistency
        debug("Validating data is not yet in sync (with repair --validate)...")
        self.assert_not_in_sync(reenable_nodesync=False)

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

        # Then disable NodeSync, and check not new validation happens from that point on
        debug("Disabling NodeSync and making no validation happens (for at least 2 seconds)")
        self.disable_nodesync()
        # NodeSync react to schema changes asynchronously (with those changes), and we don't forcefully cancel
        # ongoing segments at the time the table is disabled, so we have to give it some time (1s) to stop all
        # validations, and then some more time (2s) to make sure we indeed don't have anymore validations
        time.sleep(3)
        timestamp = (time.time() - 2) * 1000
        # check all segment haven't been validated since at least 2 seconds (the 'is not None' is just to
        # double check we haven't lost the old NodeSync info completely)
        self.assert_all_segments(predicate=lambda r: r.last_time is not None and r.last_time < timestamp)

        # Now re-enable and check it gets validated again
        debug("Re-enabling NodeSync and making sure validation resumes")
        self.enable_nodesync()
        self.assert_all_segments()

        # Lastly, drop the table and wait a bit for error (the test won't pass if this trigger an error)
        self.drop_table()
        time.sleep(3)
        # TODO: at this point, it could make sense to validate the status is now empty (for that table), but
        # we actually don't clean it up server side yet (we rely on the normal timeout of the status table,
        # but that takes a month) so this will have to wait for this to be implemented

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
        # Moving is only allowed with a single token
        self.prepare(nodes=3, rf=3, num_tokens=1)
        self.create_table()
        self.do_inserts()

        # Initial sanity check
        debug("Checking everything validated...")
        self.assert_all_segments()

        debug("Moving 3rd node...")
        self.node(3).nodetool('move {}'.format(2**16))

        # Everything should still get eventually validated
        debug("Checking everything still validated...")
        self.assert_all_segments()
        self.assert_in_sync()

    def test_rf_increase(self):
        """
        Validate that NodeSync doesn't run on RF=1, but that it starts if the RF is increased,
        and eventually replicate everything.
        """
        self.prepare(nodes=2, rf=1)
        self.create_table()
        self.do_inserts()

        # RF=1, we shouldn't be validating anything
        self.assert_all_segments(not_validated())

        debug("Raising keyspace replication")
        self.session.execute("ALTER KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'}")

        # Now everything should get validated, and everything should be in sync following that
        debug("Ensuring everything validated and in sync...")
        self.assert_all_segments()
        self.assert_in_sync()
