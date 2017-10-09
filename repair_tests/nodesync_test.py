# -*- coding: UTF-8 -*-
import time

from ccmlib.node import ToolError
from dtest import Tester, debug
from tools.data import create_ks, create_cf
from tools.decorators import since


@since('4.0')
class TestNodeSync(Tester):
    """
    Functional Spec Verification:
    1b-e
    NodeSync will be performed on a small range of data at a time, in pages of 10s or 100s of KB.
NodeSync can both group small partitions into a single range, and break up large partitions into multiple ranges.
Repair progress will be saved in a system table for each completed range.  Current activity will also be available for inspection.
Segments to be repaired will be prioritized by how close they are to passing the gc_grace_seconds window.  (This means that an operator who notices NodeSync falling behind and increases throughput will have it do the Right Thing automatically.)

    2a-b
    Enabling it can be done live, without a restart.
What about disabling? I can think of a chaos monkey scenario where we just enable and disable a bunch of tables using NodeSync randomly
We will provide a script to enable it on multiple tables, keyspaces, or all tables in the cluster.
It is then up to the operator to remember to enable it for newly created tables.  We recognize that this is suboptimal, but inventing a new cluster-wide configuration system is out of scope for DSE 6.

    4a
    NodeSync will be tolerant of transient node failure.
NodeSync will continue to repair across all active replicas.  Non-participating nodes’ absence will be noted in the saved repair metadata, but repair will continue without them.

    8a-c
    NodeSync will expose information about its status and health via JMX and/or system tables.
Is configured throughput being achieved?
Are token ranges being repaired within gcgs?
Internal status for troubleshooting if something goes wrong.

    9a-b
    NodeSync will guard against foot-shooting.
Prevent launching classic repair against a table that has NodeSync enabled.
If a classic repair is in progress for a table that has NodeSync enabled, NodeSync will wait for the classic repair to finish before proceeding.
If a classic repair is launched against a keyspace with NodeSync enabled on some tables but not others, a warning will be logged and repair jobs will be started for the non-NodeSync tables.
Unreasonable configuration parameters will result in startup failure or warning

    Other Testing:
    Test that partial activation is impossible [moot if this is a table schema setting]
    Make sure nodes repair the correct assigned range
    “in practice, each node only concerns himself with validating/repairing data for the range it is a replica of, and replicas of a given range collaborate to avoid doing duplicate work.”
    Make sure all [and only] enabled tables are tested
    Mixed-version cluster testing
    Test dropping keyspaces + tables that are undergoing CBR
    Multiple DC’s
    Test all data types

    """
    def _prepare_cluster(self, nodes=1, byteman=False, jvm_arguments=[], options=None):
        cluster = self.cluster
        cluster.populate(nodes, install_byteman=byteman)
        if options:
            cluster.set_configuration_options(values=options)
        cluster.start(wait_for_binary_proto=True, jvm_args=jvm_arguments)
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self.session = session

        return self.session

    def test_decommission(self):
        session = self._prepare_cluster(nodes=4,
                                        jvm_arguments=["-Ddatastax.nodesync.min_validation_interval_ms={}".format(5000)])
        session.execute("""
                CREATE KEYSPACE IF NOT EXISTS ks
                WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' }
                """)
        session.execute('USE ks')
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 int) WITH nodesync = {'enabled': 'true'}")
        self.cluster.nodelist()[2].decommission()

    def test_no_replication(self):
        jvm_arguments = ["-Ddatastax.nodesync.min_validation_interval_ms={}".format(2000)]
        session = self._prepare_cluster(nodes=[2, 2], jvm_arguments=jvm_arguments)
        create_ks(session, 'ks', {'dc1': 3, 'dc2': 3})
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 int) WITH nodesync = {'enabled': 'true'}")

        # reset RF at dc1 as 0
        session.execute("ALTER KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 0, 'dc2': 3};")

        # wait 5s for error
        time.sleep(10)

    def cannot_run_repair_on_nodesync_enabled_table_test(self):
        """
        * Check that ordinary repair cannot be run on NodeSync enabled table
        @jira_ticket APOLLO-966
        """
        self.ignore_log_patterns = [
            'Cannot run both full and incremental repair, choose either --full or -inc option.']
        cluster = self.cluster
        debug("Starting cluster..")
        cluster.populate(2).start()
        node1, _ = cluster.nodelist()
        session = self.patient_cql_connection(node1)

        debug("Creating keyspace and table")
        create_ks(session, 'ks', 2)
        create_cf(session, 'table1', columns={'c1': 'text', 'c2': 'text'})

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
