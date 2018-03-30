import os
import re
import time

from dse import ConsistencyLevel
from dse.query import SimpleStatement

from dtest import PRINT_DEBUG, Tester, debug, get_dse_version_from_build
from tools.assertions import assert_length_equal, assert_none, assert_one
from tools.data import create_ks, rows_to_list
from tools.decorators import since


class TestReadRepair(Tester):

    def setUp(self):
        Tester.setUp(self)
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False,
                                                       'dynamic_snitch': False})
        self.cluster.populate(3).start()

    @since('3.0')
    def alter_rf_and_run_read_repair_test(self):
        """
        @jira_ticket CASSANDRA-10655
        @jira_ticket CASSANDRA-10657

        Test that querying only a subset of all the columns in a row doesn't confuse read-repair to avoid
        the problem described in CASSANDRA-10655.
        """
        self._test_read_repair()

    def test_read_repair_chance(self):
        """
        @jira_ticket CASSANDRA-12368
        """
        self._test_read_repair(cl_all=False)

    def _test_read_repair(self, cl_all=True):
        session = self.patient_cql_connection(self.cluster.nodelist()[0])
        session.execute("""CREATE KEYSPACE alter_rf_test
                           WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};""")
        session.cluster.refresh_schema_metadata()
        session.execute("CREATE TABLE alter_rf_test.t1 (k int PRIMARY KEY, a int, b int);")
        session.execute("INSERT INTO alter_rf_test.t1 (k, a, b) VALUES (1, 1, 1);")
        cl_one_stmt = SimpleStatement("SELECT * FROM alter_rf_test.t1 WHERE k=1",
                                      consistency_level=ConsistencyLevel.ONE)

        # identify the initial replica and trigger a flush to ensure reads come from sstables
        initial_replica, non_replicas = self.identify_initial_placement('alter_rf_test', 't1', 1)
        debug("At RF=1 replica for data is " + initial_replica.name)
        initial_replica.flush()

        # At RF=1, it shouldn't matter which node we query, as the actual data should always come from the
        # initial replica when reading at CL ONE
        for n in self.cluster.nodelist():
            debug("Checking " + n.name)
            session = self.patient_exclusive_cql_connection(n)
            assert_one(session, "SELECT * FROM alter_rf_test.t1 WHERE k=1", [1, 1, 1], cl=ConsistencyLevel.ONE)

        # Alter so RF=n but don't repair, then execute a query which selects only a subset of the columns. Run this at
        # CL ALL on one of the nodes which doesn't currently have the data, triggering a read repair.
        # The expectation will be that every replicas will have been repaired for that column (but we make no assumptions
        # on the other columns).
        debug("Changing RF from 1 to 3")
        session.execute("""ALTER KEYSPACE alter_rf_test
                           WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};""")

        if not cl_all:
            debug("Setting table read repair chance to 1")
            session.execute("""ALTER TABLE alter_rf_test.t1 WITH read_repair_chance = 1;""")

        cl = ConsistencyLevel.ALL if cl_all else ConsistencyLevel.ONE

        debug("Executing SELECT on non-initial replica to trigger read repair " + non_replicas[0].name)
        read_repair_session = self.patient_exclusive_cql_connection(non_replicas[0])

        if cl_all:
            # result of the read repair query at cl=ALL contains only the selected column
            assert_one(read_repair_session, "SELECT a FROM alter_rf_test.t1 WHERE k=1", [1], cl=cl)
        else:
            # With background read repair at CL=ONE, result may or may not be correct
            stmt = SimpleStatement("SELECT a FROM alter_rf_test.t1 WHERE k=1", consistency_level=cl)
            session.execute(stmt)

        # Check the results of the read repair by querying each replica again at CL ONE
        debug("Re-running SELECTs at CL ONE to verify read repair")
        for n in self.cluster.nodelist():
            debug("Checking " + n.name)
            session = self.patient_exclusive_cql_connection(n)
            res = rows_to_list(session.execute(cl_one_stmt))

            if len(res) == 0 and get_dse_version_from_build(install_dir=n.get_install_dir()) >= '6.0':
                # TPC (see DB-1771): With TPC, background read-repairs really happen in the background.
                # This means, that the original read (the one above) returns the _unrepaired_ row. Before
                # TPC, the "background" read-repair returned the already repaired row.
                time.sleep(2)  # 2 seconds, to be really safe even with the tiny openstack instances running the dtests.
                debug("Re-issuing read after empty row")
                res = rows_to_list(session.execute(cl_one_stmt))

            # Column a must be 1 everywhere, and column b must be either 1 or None everywhere
            self.assertNotEqual([], res, msg='Got an unexpected empty row from {}'.format(n.name))
            self.assertIn(res[0][:2], [[1, 1], [1, None]], msg='Got an unexpected row from {}'.format(n.name))

        # Now query selecting all columns
        query = "SELECT * FROM alter_rf_test.t1 WHERE k=1"
        debug("Executing SELECT on non-initial replica to trigger read repair " + non_replicas[0].name)
        read_repair_session = self.patient_exclusive_cql_connection(non_replicas[0])

        if cl_all:
            # result of the read repair query at cl=ALL should contain all columns
            assert_one(read_repair_session, query, [1, 1, 1], cl=cl)
        else:
            # With background read repair at CL=ONE, result may or may not be correct
            stmt = SimpleStatement(query, consistency_level=cl)
            read_repair_session.execute(stmt)

        # Check all replica is fully up to date
        debug("Re-running SELECTs at CL ONE to verify read repair")
        for n in self.cluster.nodelist():
            debug("Checking " + n.name)
            session = self.patient_exclusive_cql_connection(n)
            assert_one(session, query, [1, 1, 1], cl=ConsistencyLevel.ONE)

    def identify_initial_placement(self, keyspace, table, key):
        nodes = self.cluster.nodelist()
        out, _, _ = nodes[0].nodetool("getendpoints alter_rf_test t1 1")
        address = out.split('\n')[-2]
        initial_replica = None
        non_replicas = []
        for node in nodes:
            if node.address() == address:
                initial_replica = node
            else:
                non_replicas.append(node)

        self.assertIsNotNone(initial_replica, "Couldn't identify initial replica")

        return initial_replica, non_replicas

    @since('2.0')
    def range_slice_query_with_tombstones_test(self):
        """
        @jira_ticket CASSANDRA-8989
        @jira_ticket CASSANDRA-9502

        Range-slice queries with CL>ONE do unnecessary read-repairs.
        Reading from table which contains collection type using token function and with CL > ONE causes overwhelming writes to replicas.


        It's possible to check the behavior with tracing - pattern matching in system_traces.events.activity
        """
        node1 = self.cluster.nodelist()[0]
        session1 = self.patient_exclusive_cql_connection(node1)

        session1.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 2}")
        session1.execute("""
            CREATE TABLE ks.cf (
                key    int primary key,
                value  double,
                txt    text
            );
        """)

        for n in range(1, 2500):
            str = "foo bar %d iuhiu iuhiu ihi" % n
            session1.execute("INSERT INTO ks.cf (key, value, txt) VALUES (%d, %d, '%s')" % (n, n, str))

        self.cluster.flush()
        self.cluster.stop()
        self.cluster.start()
        session1 = self.patient_exclusive_cql_connection(node1)

        for n in range(1, 1000):
            session1.execute("DELETE FROM ks.cf WHERE key = %d" % (n))

        time.sleep(1)

        node1.flush()

        time.sleep(1)

        query = SimpleStatement("SELECT * FROM ks.cf LIMIT 100", consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        future = session1.execute_async(query, trace=True)
        future.result()
        trace = future.get_query_trace(max_wait=120)
        self.pprint_trace(trace)
        for trace_event in trace.events:
            # Step 1, find coordinator node:
            activity = trace_event.description
            self.assertNotIn("Appending to commitlog", activity)
            self.assertNotIn("Adding to cf memtable", activity)
            self.assertNotIn("Acquiring switchLock read lock", activity)

    @since('4.0')
    def oversized_read_repair_mutation_test(self):
        """
        @jira_ticket APOLLO-1521
        For 6.0, it will split oversized mutation into smaller batches
        """
        node1, node2, node3 = self.cluster.nodelist()
        session1 = self.patient_exclusive_cql_connection(node1)

        debug('Prepare schema')
        create_ks(session1, 'oversized_readrepair_mutation', 3)
        query = """
            CREATE TABLE oversized_readrepair_mutation.cf1 (
                pk int,
                ck int,
                value blob,
                PRIMARY KEY (pk, ck)
            )
        """
        session1.execute(query)

        debug('Shutdown node2')
        node2.stop(wait_other_notice=True)

        # each row having 0.1 MB, total read_mutation will be 0.1 * 500 = 50MB > 16MB, split into 4 batches with size 125
        rows = 500
        random_bytes = os.urandom(1024 * 102)
        insert_prepared = session1.prepare("INSERT INTO oversized_readrepair_mutation.cf1 (pk, ck, value) VALUES (?, ?, ?)")

        # prepare data in 1 partition
        for n in xrange(rows):
            session1.execute(insert_prepared.bind((1, n, random_bytes)))
        session1.execute("DELETE FROM oversized_readrepair_mutation.cf1 WHERE pk=1 and ck=200")

        debug('Startig node2')
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # there should be read_repair messages for single partition query
        query = SimpleStatement("SELECT * FROM oversized_readrepair_mutation.cf1 WHERE pk = 1", consistency_level=ConsistencyLevel.ALL)
        future = session1.execute_async(query, trace=True)
        future.result()
        trace = future.get_query_trace(max_wait=120)
        self.check_trace_events_digest_mismatch(trace, True)
        self.assertTrue(node1.grep_log('read repair mutation for table oversized_readrepair_mutation.cf1, key 1, node /127.0.0.2, will split the mutation by half', filename='debug.log'),
                        "Expect to find log for splitting oversized readrepair mutation")

        debug('Shutdown node1 and node3')
        node1.stop()
        node3.stop(wait_other_notice=True)
        session2 = self.patient_exclusive_cql_connection(node2)

        # node2 should have the read_repaired data
        for n in xrange(rows):
            if n is not 200:
                assert_one(session2, "SELECT * FROM oversized_readrepair_mutation.cf1 WHERE pk = 1 AND ck = %d" % n, [1, n, random_bytes], cl=ConsistencyLevel.ONE)
            else:
                assert_none(session2, "SELECT * FROM oversized_readrepair_mutation.cf1 WHERE pk = 1 AND ck = %d" % n, cl=ConsistencyLevel.ONE)

    @since('3.0')
    def test_read_repair_chance_in_single_partition_read(self):
        """
        @jira_ticket APOLLO-998

        Single Partition Read Command was not using read_repair_chance
        """
        # hinted_handoff_enabled is False
        node1, node2, node3 = self.cluster.nodelist()
        session1 = self.patient_exclusive_cql_connection(node1)

        create_ks(session1, 'rr_chance_ks', 3)
        query = """
            CREATE TABLE rr_chance_ks.cf1 (
                key int,
                c1 int,
                PRIMARY KEY (key, c1)
            )
            with read_repair_chance = 1.0
            and dclocal_read_repair_chance = 1.0;
        """
        session1.execute(query)

        debug('Shutdown node2')
        node2.stop(wait_other_notice=True)

        # create new row
        for n in range(1, 5):
            session1.execute("INSERT INTO rr_chance_ks.cf1 (key, c1) VALUES (%d, %d)" % (n, n))

        debug('Startig node2')
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # there should be read_repair messages for single partition query
        query = SimpleStatement("SELECT * FROM rr_chance_ks.cf1 WHERE key = 1", consistency_level=ConsistencyLevel.ONE)
        future = session1.execute_async(query, trace=True)
        future.result()
        trace = future.get_query_trace(max_wait=120)
        self.check_trace_events_read_repair(trace, True)

        # there should be read_repair messages for single partition query with IN
        query = SimpleStatement("SELECT * FROM rr_chance_ks.cf1 WHERE key in (2, 3, 4, 5)", consistency_level=ConsistencyLevel.ONE)
        future = session1.execute_async(query, trace=True)
        future.result()
        trace = future.get_query_trace(max_wait=120)
        self.check_trace_events_read_repair(trace, True)

        debug('Shutdown node1 and node3')
        node1.stop()
        node3.stop(wait_other_notice=True)
        session2 = self.patient_exclusive_cql_connection(node2)

        # node2 should have the read_repaired data
        for n in range(1, 5):
            assert_one(
                session2,
                "SELECT * FROM rr_chance_ks.cf1 WHERE key = %d" % n,
                [n, n],
                cl=ConsistencyLevel.ONE
            )

    def check_trace_events_read_repair(self, trace, expected):
        self.check_trace_events(trace, expected, regex=r"Read-repair")

    def check_trace_events_digest_mismatch(self, trace, expected):
        self.check_trace_events(trace, expected, regex=r"Digest mismatch: ([a-zA-Z.]+:\s)?Mismatch for key DecoratedKey")

    def check_trace_events(self, trace, expected, regex):
        for event in trace.events:
            desc = event.description
            match = re.match(regex, desc)
            if match:
                if expected:
                    break
                else:
                    self.fail("Encountered {} when we shouldn't".format(regex))
        else:
            if expected:
                self.fail("Didn't find {}".format(regex))

    @since('3.0')
    def test_digest_mismatch_in_local_response(self):
        """
        APOLLO-1028: there should be no digest mismatch for non-existing data.

        The error was due to LocalResponse: Btree has filtered empty parition, so partition header info is not digested
        """
        node1 = self.cluster.nodelist()[0]

        session = self.patient_exclusive_cql_connection(node1)
        session.max_trace_wait = 120

        create_ks(session, 'ks_no_data', 3)
        session.execute("CREATE TABLE ks_no_data.t (id int PRIMARY KEY, v int, v2 text, v3 decimal) WITH read_repair_chance=0.0 AND dclocal_read_repair_chance=0.0")

        # for non-existing data
        for i in range(0, 99):  # wait for localDelivery on coordinator
            query = SimpleStatement("SELECT * FROM ks_no_data.t WHERE id = {}".format(i), consistency_level=ConsistencyLevel.ALL)
            result = session.execute(query, trace=True)
            assert_length_equal(result.current_rows, 0)
            self.check_trace_events_digest_mismatch(result.get_query_trace(), False)

        for i in range(0, 99):  # wait for localDelivery on coordinator
            query = SimpleStatement("SELECT id, v FROM ks_no_data.t WHERE id = {}".format(i), consistency_level=ConsistencyLevel.ALL)
            result = session.execute(query, trace=True)
            assert_length_equal(result.current_rows, 0)
            self.check_trace_events_digest_mismatch(result.get_query_trace(), False)

        # for existing data
        for i in range(100, 200):
            session.execute(SimpleStatement("INSERT INTO ks_no_data.t (id, v, v2, v3) VALUES (%d, %d, '%s', %f)" % (i, i, 'a', 3.0),
                                            consistency_level=ConsistencyLevel.ALL))

        for i in range(100, 200):
            query = SimpleStatement("SELECT id, v FROM ks_no_data.t WHERE id = {}".format(i), consistency_level=ConsistencyLevel.ALL)
            result = session.execute(query, trace=True)
            assert_length_equal(result.current_rows, 1)
            self.check_trace_events_digest_mismatch(result.get_query_trace(), False)

        for i in range(100, 200):
            query = SimpleStatement("SELECT * FROM ks_no_data.t WHERE id = {}".format(i), consistency_level=ConsistencyLevel.ALL)
            result = session.execute(query, trace=True)
            assert_length_equal(result.current_rows, 1)
            self.check_trace_events_digest_mismatch(result.get_query_trace(), False)

        # test IN with existing key + non-existing key
        for i in range(100, 150):
            query = SimpleStatement("SELECT * FROM ks_no_data.t WHERE id in ({},{},{})".format(i, i + 1, 1), consistency_level=ConsistencyLevel.ALL)
            result = session.execute(query, trace=True)
            assert_length_equal(result.current_rows, 2)
            self.check_trace_events_digest_mismatch(result.get_query_trace(), False)

        # test tombstone
        for i in range(100, 200):
            session.execute(SimpleStatement("DELETE FROM ks_no_data.t WHERE id = {}".format(i),
                                            consistency_level=ConsistencyLevel.ALL))

        for i in range(100, 200):
            query = SimpleStatement("SELECT * FROM ks_no_data.t WHERE id = {}".format(i), consistency_level=ConsistencyLevel.ALL)
            result = session.execute(query, trace=True)
            assert_length_equal(result.current_rows, 0)
            self.check_trace_events_digest_mismatch(result.get_query_trace(), False)

    @since('3.0')
    def test_gcable_tombstone_resurrection_on_range_slice_query(self):
        """
        @jira_ticket CASSANDRA-11427

        Range queries before the 11427 will trigger read repairs for puregable tombstones on hosts that already compacted given tombstones.
        This will result in constant transfer and compaction actions sourced by few nodes seeding purgeable tombstones and triggered e.g.
        by periodical jobs scanning data range wise.
        """

        node1, node2, _ = self.cluster.nodelist()

        session1 = self.patient_cql_connection(node1)
        create_ks(session1, 'gcts', 3)
        query = """
            CREATE TABLE gcts.cf1 (
                key text,
                c1 text,
                PRIMARY KEY (key, c1)
            )
            WITH gc_grace_seconds=0
            AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'};
        """
        session1.execute(query)

        # create row tombstone
        delete_stmt = SimpleStatement("DELETE FROM gcts.cf1 WHERE key = 'a'", consistency_level=ConsistencyLevel.ALL)
        session1.execute(delete_stmt)

        # flush single sstable with tombstone
        node1.flush()
        node2.flush()

        # purge tombstones from node2 (gc grace 0)
        node2.compact()

        # execute range slice query, which should not trigger read-repair for purged TS
        future = session1.execute_async(SimpleStatement("SELECT * FROM gcts.cf1", consistency_level=ConsistencyLevel.ALL), trace=True)
        future.result()
        trace = future.get_query_trace(max_wait=120)
        self.pprint_trace(trace)
        for trace_event in trace.events:
            activity = trace_event.description
            self.assertNotIn("Sending READ_REPAIR message", activity)

    def pprint_trace(self, trace):
        """Pretty print a trace"""
        if PRINT_DEBUG:
            print("-" * 40)
            for t in trace.events:
                print("%s\t%s\t%s\t%s" % (t.source, t.source_elapsed, t.description, t.thread_name))
            print("-" * 40)
