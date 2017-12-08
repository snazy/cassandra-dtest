# -*- coding: UTF-8 -*-
import time

from ccmlib.node import ToolError

from dtest import Tester, debug
from tools.decorators import since
from tools.interceptors import fake_write_interceptor
from tools.misc import new_node
from tools.nodesync import (nodesync_opts, assert_all_segments, not_validated, enable_nodesync,
                            disable_nodesync, read_nodesync_status)
from tools.preparation import prepare, config_opts, jvm_args


class SingleTableNodeSyncTester(Tester):

    def prepare(self, nodes=2, rf=2, num_tokens=None, interceptors=None, nodesync_options=None):
        opts = None
        if num_tokens:
            opts = config_opts(num_tokens=num_tokens)
        debug('Creating cluster...')
        # Saving config in case the test adds new nodes with bootstrap_node()
        self.interceptors = interceptors
        self.nodesync_options = nodesync_options
        if not self.nodesync_options:
            self.nodesync_options = nodesync_opts()
        self.session = prepare(self, nodes=nodes, rf=rf, options=opts,
                               nodesync_options=self.nodesync_options, interceptors=self.interceptors,
                               schema_timeout=30, request_timeout=30)

    def bootstrap_node(self):
        node = new_node(self.cluster)
        args = jvm_args(interceptors=self.interceptors, nodesync_options=self.nodesync_options)
        node.start(wait_for_binary_proto=True, jvm_args=args)
        return node

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

    def truncate_table(self):
        debug('Truncating table t...')
        self.session.execute("TRUNCATE TABLE ks.t")

    def truncate_status_table(self):
        debug('Truncating NodeSync status table ...')
        self.session.execute("TRUNCATE TABLE system_distributed.nodesync_status")

    def do_inserts(self, inserts=1000, start_at=0):
        debug("Inserting data...")
        for i in range(start_at, start_at + inserts):
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

    def read_nodesync_status(self, print_debug=False):
        return read_nodesync_status(self.session, 'ks', 't', print_debug=debug)

    def assert_segments_count(self, min, max=None):
        """ Validate that number of segments that are currently used.

        If only :min is provided, this must be the exact number of segments. If :max is provided as well,
        then the method check that the number of segment is between :min and :max, both inclusive.
        """
        # The small subtlety here is that if local ranges or the depth changes, there is will be 'stale'
        # records in the status table (which will eventually expire, but not fast enough for tests), and
        # while those should theoretically be ignored, the consolidation of records in read_nodesync_status
        # can still be impacted. While we could theoretically improve that method, it's a bit painful.
        # So we work around that by truncating the nodesync table and waiting for all segments to be
        # re-validated.
        self.truncate_status_table()
        self.assert_all_segments()
        record_count = len(self.read_nodesync_status())
        if not max:
            self.assertEqual(record_count, min)
        else:
            self.assertGreaterEqual(record_count, min)
            self.assertLessEqual(record_count, max)


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

        # Give some time for node 3 to breath.
        time.sleep(1)

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

    def test_depth_update(self):
        """
        Test that when data grow, the depth is increased (to create more segments).
        """
        # Note: not using vnodes because that would require a lot more data to get a depth increase and make
        # things a bit harder to reason about for this test.
        self.prepare(nodes=2, rf=2, num_tokens=1)
        self.create_table()
        # Insert a tiny bit of data just to have something for initial checks
        self.do_inserts(inserts=10)
        # Wait for all segments to be validated so the status table is fully populated and we get an accurate segment count
        self.assert_all_segments()

        # In most cases, we will have 3 segments, because we have 2 nodes/2 tokens so 2 local ranges (and not enough data
        # to have a depth > 0), but one will wrap and will thus be split. In theory, if one of the node get the minimum
        # token as token however (is it possible? don't remember and feeling like checking), we'll only have 2 segments,
        # so playing it safe and not using equality.
        debug("Checking initial segment count...")
        self.assert_segments_count(2, 3)

        # Now insert more data, enough to guarantee a depth increase.
        # We have 3 segments, and a target of maximum 100kb per segment, so we need a bit more than 300kb to get an increase.
        # From quick experiments, every row we insert (each is a distinct partition) account for a bit below 30 bytes and
        # 3 * 100kb / 30 is 10240, so 15k rows should be enough to get the size increase.
        INSERTIONS = 15000
        self.do_inserts(inserts=INSERTIONS)
        # Then we need to wait for at least 5 seconds because it's how often the check for size changes runs
        time.sleep(5.5)
        # Make sure all segment have been validated post-increase, and then check we do have twice more records
        debug("Checking segment count has increased...")
        self.assert_all_segments()
        self.assert_segments_count(4, 6)

        # Check for one more increase by doubling the size.
        debug("Doubling size...")
        self.do_inserts(inserts=INSERTIONS, start_at=INSERTIONS)
        time.sleep(5.5)
        debug("Checking segment count has increased...")
        self.assert_all_segments()
        self.assert_segments_count(8, 12)

        # And now check for size decrease by truncating the table and making sure we get back to the initial
        # number of segments.
        # TODO: the following flush shouldn't be necessary, but somehow the test appears to fail without it:
        # there seems to be remaining accounted data post-truncate. This should be investigated, but as this
        # unrelated to this test purpose and doesn't meaningfully slow the test, using as a work-around.
        self.cluster.flush()
        self.truncate_table()
        time.sleep(5.5)
        debug("Checking segment count has decreased...")
        self.assert_all_segments()
        self.assert_segments_count(2, 3)

    def test_bootstrap(self):
        """
        Test that:
            - NodeSync don't run on a one node cluster (even if RF>1)
            - NodeSync does start if a 2nd node is added.
            - Adding a 3rd node doesn't break anything either.
        """
        self.prepare(nodes=1, rf=2)
        self.create_table()
        self.do_inserts()

        # Single node cluster, we shouldn't be validating anything
        self.assert_all_segments(not_validated())

        debug("Bootstrapping 2nd node")
        self.bootstrap_node()
        debug("Validating everything is validated...")
        self.assert_all_segments()

        debug("Bootstrapping 3rd node")
        self.bootstrap_node()
        debug("Validating everything is validated...")
        self.assert_all_segments()