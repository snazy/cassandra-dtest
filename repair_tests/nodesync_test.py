# -*- coding: UTF-8 -*-
import time
import os

from ccmlib.node import ToolError

from dtests.dtest import Tester, debug
from tools.assertions import assert_one
from tools.decorators import since
from tools.interceptors import fake_write_interceptor
from tools.misc import new_node
from tools.nodesync import (assert_all_segments, disable_nodesync,
                            enable_nodesync, nodesync_opts, not_validated,
                            read_nodesync_status)
from tools.preparation import config_opts, jvm_args, prepare


class SingleTableNodeSyncTester(Tester):

    def prepare(self, nodes=2, rf=2, num_tokens=None, interceptors=None,
                options=None, nodesync_options=None):
        if options is None:
            options = {}
        if num_tokens:
            options.update(config_opts(num_tokens=num_tokens))
        debug('Creating cluster...')
        # Saving config in case the test adds new nodes with bootstrap_node()
        self.interceptors = interceptors
        self.nodesync_options = nodesync_options
        if not self.nodesync_options:
            self.nodesync_options = nodesync_opts()
        self.session = prepare(self, nodes=nodes, rf=rf, options=options,
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

    def create_and_populate(self, ks, table):
        debug("Creating and populating {}.{} without NodeSync".format(ks, table))
        self.session.execute("CREATE KEYSPACE IF NOT EXISTS " + ks + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '2'};")
        self.session.execute("CREATE TABLE {}.{} (k int PRIMARY KEY)".format(ks, table))
        for i in xrange(0, 1000):
            self.session.execute("INSERT INTO {}.{} (k) VALUES ({})".format(ks, table, i))

    def test_legacy_repair_on_nodesync_enabled_table(self):
        """
        * Check that error is thrown when table-level repair is run on table with NodeSync enabled
        * Check that keyspace and global level repair skips tables with NodeSync enabled
        @jira_ticket APOLLO-966 / DB-1667
        """

        debug("Starting cluster")
        cluster = self.cluster
        cluster.populate(2).start()
        node1 = cluster.nodelist()[0]
        self.session = self.patient_cql_connection(node1)
        self.create_and_populate('ks1', 't1')

        # Check there is no problem executing repair on ks1.t1
        debug("Running table-level repair on ks1.t1 - should work fine")
        node1.nodetool('repair ks1 t1')

        # Now enable NodeSync  on ks1.t1- Check cannot run anti-entropy repair on NodeSync enabled table
        debug("Enabling nodesync on ks1.t1 - can no longer run table-level repair")
        enable_nodesync(self.session, 'ks1', 't1')
        mark = node1.mark_log()
        with self.assertRaises(ToolError) as ctx:
            node1.nodetool('repair ks1 t1')
        self.assertIn('Cannot run anti-entropy repair on tables with NodeSync enabled', ctx.exception.stderr)
        self.assertFalse(node1.grep_log('are consistent for t1', from_mark=mark))

        mark = node1.mark_log()
        # Check there is no problem executing keyspace repair with 1 NS-enabled table
        debug("Running keyspace-level repair on ks1 - should skip repair on table t1")
        stdout, stderr, _ = node1.nodetool('repair ks1')
        self.assertIn('Skipping anti-entropy repair on tables with NodeSync enabled: [ks1.t1].', stdout)
        self.assertFalse(node1.grep_log('are consistent for t', from_mark=mark))

        # Check there is no problem executing keyspace-repair with multiple tables
        mark = node1.mark_log()
        self.create_and_populate('ks1', 't2')

        debug("Running keyspace-level repair on ks1 - should skip repair only on ks1.t1")
        stdout, stderr, _ = node1.nodetool('repair ks1')
        self.assertIn('Skipping anti-entropy repair on tables with NodeSync enabled: [ks1.t1].', stdout)
        self.assertFalse(node1.grep_log('are consistent for t1', from_mark=mark), "Should not repair t1")
        self.assertTrue(node1.grep_log('are consistent for t2', from_mark=mark), "Should repair t2")

        # Check table-repair with multiple tables fails if any of them has nodesync enabled
        debug("Running table-level repair on tables t1 and t2 - should fail")
        with self.assertRaises(ToolError) as ctx:
            node1.nodetool('repair ks1 t2 t1')
        self.assertIn('Cannot run anti-entropy repair on tables with NodeSync enabled', ctx.exception.stderr)

        # Check that keyspace-level repair does not fail on keyspace without nodesync enabled
        self.create_and_populate('ks2', 't3')

        # Check there is no problem executing repair on ks2.t
        debug("Running repair on ks2.t3 - should work fine")
        node1.nodetool('repair ks2 t3')

        # Check that node-level repair does not fail when there is a table with nodesync
        debug("Running all-tables repair - should skip repair only on ks1.t1")
        mark = node1.mark_log()
        stdout, stderr, _ = node1.nodetool('repair')
        self.assertIn('Skipping anti-entropy repair on tables with NodeSync enabled: [ks1.t1].', stdout)
        self.assertFalse(node1.grep_log('are consistent for t1', from_mark=mark), "Should not repair t1")
        self.assertTrue(node1.grep_log('are consistent for t2', from_mark=mark), "Should repair t2")
        self.assertTrue(node1.grep_log('are consistent for t3', from_mark=mark), "Should repair t3")

        # Enable nodesync on table ks1.t2
        debug("Enable NodeSync on t2 and run all-tables repair - should skip repair on both ks1.t1 and ks1.t2")
        enable_nodesync(self.session, 'ks1', 't2')
        mark = node1.mark_log()
        stdout, stderr, _ = node1.nodetool('repair')
        self.assertIn('Skipping anti-entropy repair on tables with NodeSync enabled: [ks1.t1, ks1.t2].', stdout)
        self.assertFalse(node1.grep_log('are consistent for t1', from_mark=mark), "Should not repair t1")
        self.assertFalse(node1.grep_log('are consistent for t2', from_mark=mark), "Should not repair t2")
        self.assertTrue(node1.grep_log('are consistent for t3', from_mark=mark), "Should repair t3")

        debug("Running subrange repair on table t2 - should fail")
        mark = node1.mark_log()
        with self.assertRaises(ToolError) as ctx:
            node1.nodetool('repair -st 0 -et 1000 ks1 t2')
        self.assertIn('Cannot run anti-entropy repair on tables with NodeSync enabled', ctx.exception.stderr)
        self.assertFalse(node1.grep_log('are consistent for t2', from_mark=mark))

        # Now disable NodeSync on ks1.t1 table - no problem in running table-level repair again
        debug("Disabling nodesync on ks1.t1 - can run table-level repair again")
        disable_nodesync(self.session, 'ks1', 't1')
        node1.nodetool('repair ks1 t1')

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

    def test_short_read_protection(self):
        """
        @jira_ticket: DB-2323
        To verify nodesync validation with short read proection should run on nodesync threads
        """
        self.prepare(nodes=2, rf=2, options={'nodesync': {'page_size_in_kb': 1}, 'hinted_handoff_enabled': 'false'})
        [node1, node2] = self.cluster.nodelist()
        self.session.execute("CREATE TABLE ks.test_srp(pk int, ck int, v1 blob, primary key(pk,ck))")

        rows = 10

        random_bytes = os.urandom(1024 * 1)   # 1 kb
        update = self.session.prepare("UPDATE ks.test_srp SET v1=? WHERE pk=0 AND ck=?")
        delete = self.session.prepare("UPDATE ks.test_srp SET v1=null WHERE pk=0 AND ck=?")

        debug("Populate some data on node1 only")
        node2.stop(wait_other_notice=True)
        for i in xrange(rows):
            if i % 2 == 0:
                self.session.execute(update.bind([random_bytes, i]))
            else:
                self.session.execute(delete.bind([i]))

        debug("Populate some data on node2 only")
        node1.stop(wait_other_notice=True)
        node2.set_log_level("TRACE")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        self.session = self.patient_cql_connection(node2, keyspace="ks")
        for i in xrange(rows):
            if i % 2 == 1 or i == 0:
                self.session.execute(update.bind([random_bytes, i]))
            else:
                self.session.execute(delete.bind([i]))

        debug("Start cluster")
        node1.set_log_level("TRACE")
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        debug("Enable nodesync and wait for validation finished")
        self.session.execute("ALTER TABLE ks.test_srp WITH nodesync={ 'enabled' : 'true'}")

        assert_all_segments(self.session, 'ks', 'test_srp', predicate=None)
        srp_log = ' for short read protection'
        self.assertTrue(node1.grep_log(srp_log, filename='debug.log') or node2.grep_log(srp_log, filename='debug.log'),
                        "Expect short-read-protection, but found nothing")

    def test_single_node_cluster_does_not_print_not_able_to_sustain_rate_warning(self):
        """
        @jira_ticket DB-1470
        """

        debug("Starting 1-node cluster")
        options = {'nodesync': {'rate_in_kb': 1024,
                                'min_threads': 1,
                                'max_threads': 1,
                                'min_inflight_validations': 1,
                                'max_inflight_validations': 1}}
        self.prepare(nodes=1, rf=3,
                     options=options,
                     nodesync_options=nodesync_opts(controller_update_interval_sec=1))

        debug("Sleeping 2 seconds")
        time.sleep(2)

        debug("Checking that NodeSync will not print not able to sustain rate warning")
        node1 = self.node(1)
        self.assertFalse(node1.grep_log("NodeSync doesn't seem to be able to sustain the configured rate"),
                         "Should not print not able to sustain rate warning.")

    def test_concurrent_alter_table(self):
        """
        @jira_ticket DB-1830 NodeSync should include newly added column for validation.
        """
        # DB-1649, coordinator has new column but replicas don't when schema is not fully propagated
        self.ignore_log_patterns = ['MessageDeserializationException']
        options = {'hinted_handoff_enabled': 'false'}
        debug("Starting 3-node cluster with rf=2")
        self.prepare(nodes=3, rf=2, options=options)
        self.session.execute("ALTER KEYSPACE system_distributed WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };")

        debug("Prepare some data and verify that is's validated")
        self.create_table()
        self.do_inserts()
        self.assert_all_segments()

        debug("Add column and populate data into new column")
        self.session.execute("ALTER TABLE ks.t ADD new_value int")
        self.session.cluster.control_connection.wait_for_schema_agreement()
        for node in self.cluster.nodelist():
            self.assertTrue(node.grep_log('Updating NodeSync state for ks.t following table update', filename='debug.log'),
                            "Expected to find log about updating medatadata")
        for i in range(0, 1000):
            self.session.execute("UPDATE ks.t SET new_value = 1 WHERE k = {}".format(i))
        self.assert_all_segments()

        # Easier for us to verify data with rf=3
        debug("Increasing Keyspace ks rf to 3")
        self.session.execute("ALTER KEYSPACE ks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
        self.assert_all_segments()

        debug("Verify new column is repaired by nodesync on each single node")
        self.session.execute("ALTER TABLE ks.t WITH nodesync={ 'enabled' : 'false' }")
        self.cluster.stop()
        for node in self.cluster.nodelist():
            debug("Verifying {}".format(node.name))
            node.start(wait_for_binary_proto=True)
            session = self.patient_cql_connection(node)
            for i in range(0, 1000):
                assert_one(session, "SELECT new_value FROM ks.t WHERE k = {}".format(i), [1])
            node.stop(wait_for_binary_proto=True)
