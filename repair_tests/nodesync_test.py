# -*- coding: UTF-8 -*-
import calendar
import time
import heapq

from functools import total_ordering
from operator import attrgetter

from ccmlib.node import ToolError

from dtest import Tester, debug
from tools.data import create_cf
from tools.decorators import since
from tools.nodesync import *
from tools.preparation import prepare


@since('4.0')
class TestNodeSync(Tester):

    def test_decommission(self):
        session = prepare(self, nodes=4, rf=3, nodesync_options=nodesync_opts())
        create_cf(session, 't', key_type='int', columns={'v': 'int', 'v2': 'int'}, nodesync=True)
        self.cluster.nodelist()[2].decommission()

    def test_no_replication(self):
        session = prepare(self, nodes=[2, 2], rf={'dc1': 3, 'dc2': 3}, nodesync_options=nodesync_opts())
        create_cf(session, 't', key_type='int', columns={'v': 'int', 'v2': 'int'}, nodesync=True)

        # reset RF at dc1 as 0
        session.execute("ALTER KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 0, 'dc2': 3};")

        # wait 5s for error
        time.sleep(10)

    def test_cannot_run_repair_on_nodesync_enabled_table(self):
        """
        * Check that ordinary repair cannot be run on NodeSync enabled table
        @jira_ticket APOLLO-966
        """
        self.ignore_log_patterns = [
            'Cannot run both full and incremental repair, choose either --full or -inc option.']
        session = prepare(self, nodes=2, rf=2, nodesync_options=nodesync_opts())
        create_cf(session, 'table1', columns={'c1': 'text', 'c2': 'text'})

        node1 = self.cluster.nodelist()[0]
        # Check there is no problem executing repair on ks.table
        debug("Running repair on ks.table1")
        node1.nodetool('repair ks table1')

        # Now enable NodeSync - Check cannot run anti-entropy repair on NodeSync enabled table
        debug("Enabling nodesync on ks.table1 - cannot run repair")
        session.execute("ALTER TABLE ks.table1 WITH nodesync = {'enabled': 'true'}")
        with self.assertRaises(ToolError) as ctx:
            node1.nodetool('repair ks table1')
        self.assertIn('Cannot run anti-entropy repair on tables with NodeSync enabled', ctx.exception.stderr)

        # Now disable NodeSync on view_build_status table - no problem in running repair
        debug("Disabling nodesync on ks.table1 - can run repair again")
        session.execute("ALTER TABLE ks.table1 WITH nodesync = {'enabled': 'false'}")
        node1.nodetool('repair ks table1')

    def test_basic_nodesync_validation(self):
        """
        Validate basic behavior of NodeSync: that a table gets entirely validated and continuously so
        """
        session = prepare(self, nodes=2, rf=2, nodesync_options=nodesync_opts())
        create_cf(session, 'table1', key_type='int', columns={'v': 'text'})

        INSERTS = 1000
        debug("Inserting data...")
        for i in range(0, INSERTS):
            session.execute("INSERT INTO ks.table1(key, v) VALUES({}, 'foobar')".format(i))

        # NodeSync is not yet running, so make sure there is no state
        assert_all_segments(session, 'ks', 'table1', predicate=not_validated())

        # Enable NodeSync and make sure everything gets validated
        debug("Enabling NodeSync and waiting on initial validations...")
        session.execute("ALTER TABLE ks.table1 WITH nodesync = {'enabled': 'true'}")
        assert_all_segments(session, 'ks', 'table1')

        # Now, test that segments gets continuously validated by repeatedly grabbing a timestamp and make sure
        # that everything gets validated past this timestamp.
        RUNS = 5
        for _ in range(0, RUNS):
            timestamp = time.time() * 1000
            debug("Waiting on validations being older than {}".format(timestamp))
            assert_all_segments(session, 'ks', 'table1', predicate=validated_since(timestamp))
