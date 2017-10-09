import re
import time
from collections import Counter, namedtuple
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from re import compile, findall
from unittest import skip
from uuid import UUID, uuid1

from ccmlib.common import is_win
from ccmlib.node import Node, ToolError
from dse import ConsistencyLevel
from dse.query import SimpleStatement
from dse.metadata import Murmur3Token
from nose.plugins.attrib import attr

from dtest import Tester, debug
from tools.assertions import (assert_all, assert_almost_equal, assert_none,
                              assert_one, assert_unavailable)
from tools.data import create_cf, create_ks, insert_c1c2
from tools.decorators import no_vnodes, since
from tools.jmxutils import (JolokiaAgent, make_mbean,
                            remove_perf_disable_shared_mem)
from tools.misc import new_node


class ConsistentState(object):
    PREPARING = 0
    PREPARED = 1
    REPAIRING = 2
    FINALIZED = 3
    FAILED = 4


def get_repair_options(version, incremental, force_anti_compaction=False):
    args = []
    if incremental:
        if version >= "2.2":
            args += ["-inc"]
        else:
            args += [" -par -inc"]
    elif version < "4.0":
        # on version < "4.0" inc. repair is run on previously repaired table
        # so we force the '-full' flag to ensure full repair will always run
        args += ["-full"]
        if force_anti_compaction:
            args += ["--run-anticompaction"]
    return args


def table_metric(node, keyspace, table, name):
    version = node.get_cassandra_version()
    typeName = "ColumnFamily" if version <= '2.3' else 'Table'
    with JolokiaAgent(node) as jmx:
        mbean = make_mbean('metrics', type=typeName,
                           name=name, keyspace=keyspace, scope=table)
        value = jmx.read_attribute(mbean, 'Count')

    return value


class TestIncRepair(Tester):
    ignore_log_patterns = (r'Can\'t send migration request: node.*is down',)

    def _get_repaired_data(self, node, keyspace):
        _sstable_name = compile('SSTable: (.+)')
        _repaired_at = compile('Repaired at: (\d+)')
        _pending_repair = compile('Pending repair: (\-\-|null|[a-f0-9\-]+)')
        _sstable_data = namedtuple('_sstabledata', ('name', 'repaired', 'pending_id'))

        out = node.run_sstablemetadata(keyspace=keyspace).stdout

        def matches(pattern):
            return filter(None, [pattern.match(l) for l in out.split('\n')])
        names = [m.group(1) for m in matches(_sstable_name)]
        repaired_times = [int(m.group(1)) for m in matches(_repaired_at)]

        def uuid_or_none(s):
            return None if s == 'null' or s == '--' else UUID(s)
        pending_repairs = [uuid_or_none(m.group(1)) for m in matches(_pending_repair)] if self.cluster.version() >= "4.0" else [None for m in names]
        assert names
        assert repaired_times
        assert pending_repairs
        assert len(names) == len(repaired_times) == len(pending_repairs)
        return [_sstable_data(*a) for a in zip(names, repaired_times, pending_repairs)]

    def assertNoRepairedSSTables(self, node, keyspace):
        """ Checks that no sstables are marked repaired, and none are marked pending repair """
        data = self._get_repaired_data(node, keyspace)
        self.assertTrue(all([t.repaired == 0 for t in data]), '{}'.format(data))
        self.assertTrue(all([t.pending_id is None for t in data]))

    def assertAllPendingRepairSSTables(self, node, keyspace, pending_id=None):
        """ Checks that no sstables are marked repaired, and all are marked pending repair """
        data = self._get_repaired_data(node, keyspace)
        self.assertTrue(all([t.repaired == 0 for t in data]), '{}'.format(data))
        if pending_id:
            self.assertTrue(all([t.pending_id == pending_id for t in data]))
        else:
            self.assertTrue(all([t.pending_id is not None for t in data]))

    def assertAllRepairedSSTables(self, node, keyspace):
        """ Checks that all sstables are marked repaired, and none are marked pending repair """
        data = self._get_repaired_data(node, keyspace)
        self.assertTrue(all([t.repaired > 0 for t in data]), '{}'.format(data))
        self.assertTrue(all([t.pending_id is None for t in data]), '{}'.format(data))

    def assertRepairedAndUnrepaired(self, node, keyspace):
        """ Checks that a node has both repaired and unrepaired sstables for a given keyspace """
        data = self._get_repaired_data(node, keyspace)
        self.assertTrue(any([t.repaired > 0 for t in data]), '{}'.format(data))
        self.assertTrue(any([t.repaired == 0 for t in data]), '{}'.format(data))
        self.assertTrue(all([t.pending_id is None for t in data]), '{}'.format(data))

    @since('4.0')
    def consistent_repair_test(self):
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False, 'num_tokens': 1, 'commitlog_sync_period_in_ms': 500})
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        # make data inconsistent between nodes
        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")
        stmt = SimpleStatement("INSERT INTO ks.tbl (k,v) VALUES (%s, %s)")
        stmt.consistency_level = ConsistencyLevel.ALL
        for i in range(10):
            session.execute(stmt, (i, i))
        node3.flush()
        time.sleep(1)
        node3.stop(gently=False)
        stmt.consistency_level = ConsistencyLevel.QUORUM

        session = self.exclusive_cql_connection(node1)
        for i in range(10):
            session.execute(stmt, (i + 10, i + 10))
        node1.flush()
        time.sleep(1)
        node1.stop(gently=False)
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        session = self.exclusive_cql_connection(node2)
        for i in range(10):
            session.execute(stmt, (i + 20, i + 20))
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # flush and check that no sstables are marked repaired
        for node in cluster.nodelist():
            node.flush()
            self.assertNoRepairedSSTables(node, 'ks')
            session = self.patient_exclusive_cql_connection(node)
            results = list(session.execute("SELECT * FROM system.repairs"))
            self.assertEqual(len(results), 0, str(results))

        # repair
        self.repair(node1, options=['ks'])

        # check that all participating nodes have the repair recorded in their system
        # table, that all nodes are listed as participants, and that all sstables are
        # marked repair
        expected_participants = {n.address() for n in cluster.nodelist()}
        for node in cluster.nodelist():
            session = self.patient_exclusive_cql_connection(node)
            results = list(session.execute("SELECT * FROM system.repairs"))
            self.assertEqual(len(results), 1)
            result = results[0]
            self.assertEqual(set(result.participants), expected_participants)
            self.assertEqual(result.state, ConsistentState.FINALIZED, "3=FINALIZED")
            pending_id = result.parent_id
            self.assertAllRepairedSSTables(node, 'ks')

    def _make_fake_session(self, keyspace, table):
        node1 = self.cluster.nodelist()[0]
        session = self.patient_exclusive_cql_connection(node1)
        session_id = uuid1()
        cfid = list(session.execute("SELECT * FROM system_schema.tables WHERE keyspace_name='{}' AND table_name='{}'".format(keyspace, table)))[0].id
        now = datetime.now()
        # pulled from a repairs table
        ranges = {'\x00\x00\x00\x08K\xc2\xed\\<\xd3{X\x00\x00\x00\x08r\x04\x89[j\x81\xc4\xe6',
                  '\x00\x00\x00\x08r\x04\x89[j\x81\xc4\xe6\x00\x00\x00\x08\xd8\xcdo\x9e\xcbl\x83\xd4',
                  '\x00\x00\x00\x08\xd8\xcdo\x9e\xcbl\x83\xd4\x00\x00\x00\x08K\xc2\xed\\<\xd3{X'}
        ranges = {buffer(b) for b in ranges}

        for node in self.cluster.nodelist():
            session = self.patient_exclusive_cql_connection(node)
            session.execute("INSERT INTO system.repairs "
                            "(parent_id, cfids, coordinator, last_update, participants, ranges, repaired_at, started_at, state) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            [session_id, {cfid}, node1.address(), now, {n.address() for n in self.cluster.nodelist()},
                             ranges, now, now, ConsistentState.REPAIRING])  # 2=REPAIRING

        time.sleep(1)
        for node in self.cluster.nodelist():
            node.stop(gently=False)

        for node in self.cluster.nodelist():
            node.start()

        return session_id

    @since('4.0')
    def manual_session_fail_test(self):
        """ check manual failing of repair sessions via nodetool works properly """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False, 'num_tokens': 1, 'commitlog_sync_period_in_ms': 500})
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        # make data inconsistent between nodes
        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")

        for node in self.cluster.nodelist():
            out = node.nodetool('repair_admin')
            self.assertIn("no sessions", out.stdout)

        session_id = self._make_fake_session('ks', 'tbl')

        for node in self.cluster.nodelist():
            out = node.nodetool('repair_admin')
            lines = out.stdout.split('\n')
            self.assertGreater(len(lines), 1)
            line = lines[1]
            self.assertIn(str(session_id), line)
            self.assertIn("REPAIRING", line)

        node1.nodetool("repair_admin --cancel {}".format(session_id))

        for node in self.cluster.nodelist():
            out = node.nodetool('repair_admin --all')
            lines = out.stdout.split('\n')
            self.assertGreater(len(lines), 1)
            line = lines[1]
            self.assertIn(str(session_id), line)
            self.assertIn("FAILED", line)

    @since('4.0')
    def manual_session_cancel_non_coordinator_failure_test(self):
        """ check manual failing of repair sessions via a node other than the coordinator fails """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False, 'num_tokens': 1, 'commitlog_sync_period_in_ms': 500})
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        # make data inconsistent between nodes
        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")

        for node in self.cluster.nodelist():
            out = node.nodetool('repair_admin')
            self.assertIn("no sessions", out.stdout)

        session_id = self._make_fake_session('ks', 'tbl')

        for node in self.cluster.nodelist():
            out = node.nodetool('repair_admin')
            lines = out.stdout.split('\n')
            self.assertGreater(len(lines), 1)
            line = lines[1]
            self.assertIn(str(session_id), line)
            self.assertIn("REPAIRING", line)

        try:
            node2.nodetool("repair_admin --cancel {}".format(session_id))
            self.fail("cancel from a non coordinator should fail")
        except ToolError:
            pass  # expected

        # nothing should have changed
        for node in self.cluster.nodelist():
            out = node.nodetool('repair_admin')
            lines = out.stdout.split('\n')
            self.assertGreater(len(lines), 1)
            line = lines[1]
            self.assertIn(str(session_id), line)
            self.assertIn("REPAIRING", line)

    @since('4.0')
    def manual_session_force_cancel_test(self):
        """ check manual failing of repair sessions via a non-coordinator works if the --force flag is set """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False, 'num_tokens': 1, 'commitlog_sync_period_in_ms': 500})
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        # make data inconsistent between nodes
        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")

        for node in self.cluster.nodelist():
            out = node.nodetool('repair_admin')
            self.assertIn("no sessions", out.stdout)

        session_id = self._make_fake_session('ks', 'tbl')

        for node in self.cluster.nodelist():
            out = node.nodetool('repair_admin')
            lines = out.stdout.split('\n')
            self.assertGreater(len(lines), 1)
            line = lines[1]
            self.assertIn(str(session_id), line)
            self.assertIn("REPAIRING", line)

        node2.nodetool("repair_admin --cancel {} --force".format(session_id))

        for node in self.cluster.nodelist():
            out = node.nodetool('repair_admin --all')
            lines = out.stdout.split('\n')
            self.assertGreater(len(lines), 1)
            line = lines[1]
            self.assertIn(str(session_id), line)
            self.assertIn("FAILED", line)

    def sstable_marking_test(self):
        """
        * Launch a three node cluster
        * Stop node3
        * Write 10K rows with stress
        * Start node3
        * Issue an incremental repair, and wait for it to finish
        * Run sstablemetadata on every node, assert that all sstables are marked as repaired
        """
        cluster = self.cluster
        # hinted handoff can create SSTable that we don't need after node3 restarted
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        node3.stop(gently=True)

        node1.stress(['write', 'n=10K', 'no-warmup', '-schema', 'replication(factor=3)'])
        node1.flush()
        node2.flush()
        node3.start(wait_other_notice=True)
        if node3.get_cassandra_version() < '2.2':
            log_file = 'system.log'
        else:
            log_file = 'debug.log'
        node3.watch_log_for("Initializing keyspace1.standard1", filename=log_file)
        # wait for things to settle before starting repair
        time.sleep(1)
        self.repair(node3)

        if cluster.version() >= '4.0':
            # sstables are compacted out of pending repair by a compaction
            for node in cluster.nodelist():
                node.nodetool('compact keyspace1 standard1')

        for out in (node.run_sstablemetadata(keyspace='keyspace1').stdout for node in cluster.nodelist()):
            self.assertNotIn('Repaired at: 0', out)

    def multiple_repair_test(self):
        """
        * Launch a three node cluster
        * Create a keyspace with RF 3 and a table
        * Insert 49 rows
        * Stop node3
        * Insert 50 more rows
        * Restart node3
        * Issue an incremental repair on node3
        * Stop node2
        * Insert a final50 rows
        * Restart node2
        * Issue an incremental repair on node2
        * Replace node3 with a new node
        * Verify data integrity
        # TODO: Several more verifications of data need to be interspersed throughout the test. The final assertion is insufficient.
        @jira_ticket CASSANDRA-10644
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 3)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        debug("insert data")

        insert_c1c2(session, keys=range(1, 50), consistency=ConsistencyLevel.ALL)
        node1.flush()

        debug("bringing down node 3")
        node3.flush()
        node3.stop(gently=False)

        debug("inserting additional data into node 1 and 2")
        insert_c1c2(session, keys=range(50, 100), consistency=ConsistencyLevel.TWO)
        node1.flush()
        node2.flush()

        debug("restarting and repairing node 3")
        node3.start(wait_for_binary_proto=True)

        self.repair(node3)

        # wait stream handlers to be closed on windows
        # after session is finished (See CASSANDRA-10644)
        if is_win:
            time.sleep(2)

        debug("stopping node 2")
        node2.stop(gently=False)

        debug("inserting data in nodes 1 and 3")
        insert_c1c2(session, keys=range(100, 150), consistency=ConsistencyLevel.TWO)
        node1.flush()
        node3.flush()

        debug("start and repair node 2")
        node2.start(wait_for_binary_proto=True)

        self.repair(node2)

        debug("replace node and check data integrity")
        node3.stop(gently=False)
        node5 = Node('node5', cluster, True, ('127.0.0.5', 9160), ('127.0.0.5', 7000), '7500', '0', None, ('127.0.0.5', 9042))
        cluster.add(node5, False)
        node5.start(replace_address='127.0.0.3', wait_other_notice=True)

        assert_one(session, "SELECT COUNT(*) FROM ks.cf LIMIT 200", [149])

    def sstable_repairedset_test(self):
        """
        * Launch a two node cluster
        * Insert data with stress
        * Stop node2
        * Run sstablerepairedset against node2
        * Start node2
        * Run sstablemetadata on both nodes, pipe to a file
        * Verify the output of sstablemetadata shows no repairs have occurred
        * Stop node1
        * Insert more data with stress
        * Start node1
        * Issue an incremental repair
        * Run sstablemetadata on both nodes again, pipe to a new file
        * Verify repairs occurred and repairedAt was updated
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.populate(2).start()
        node1, node2 = cluster.nodelist()
        node1.stress(['write', 'n=10K', 'no-warmup', '-schema', 'replication(factor=2)', 'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)', '-rate', 'threads=50'])

        node1.flush()
        node2.flush()

        node2.stop(gently=False)

        node2.run_sstablerepairedset(keyspace='keyspace1')
        node2.start(wait_for_binary_proto=True)

        initialOut1 = node1.run_sstablemetadata(keyspace='keyspace1').stdout
        initialOut2 = node2.run_sstablemetadata(keyspace='keyspace1').stdout

        matches = findall('(?<=Repaired at:).*', '\n'.join([initialOut1, initialOut2]))
        debug("Repair timestamps are: {}".format(matches))

        uniquematches = set(matches)
        matchcount = Counter(matches)

        self.assertGreaterEqual(len(uniquematches), 2, uniquematches)

        self.assertGreaterEqual(max(matchcount), 1, matchcount)

        self.assertIn('Repaired at: 0', '\n'.join([initialOut1, initialOut2]))

        node1.stop()
        node2.stress(['write', 'n=15K', 'no-warmup', '-schema', 'replication(factor=2)'])
        node2.flush()
        node1.start(wait_for_binary_proto=True)

        self.repair(node1)

        if cluster.version() >= '4.0':
            # sstables are compacted out of pending repair by a compaction
            for node in cluster.nodelist():
                node.nodetool('compact keyspace1 standard1')

        finalOut1 = node1.run_sstablemetadata(keyspace='keyspace1').stdout
        finalOut2 = node2.run_sstablemetadata(keyspace='keyspace1').stdout

        matches = findall('(?<=Repaired at:).*', '\n'.join([finalOut1, finalOut2]))

        debug(matches)

        uniquematches = set(matches)
        matchcount = Counter(matches)

        self.assertGreaterEqual(len(uniquematches), 2)

        self.assertGreaterEqual(max(matchcount), 2)

        self.assertNotIn('Repaired at: 0', '\n'.join([finalOut1, finalOut2]))

    def compaction_test(self):
        """
        Test we can major compact after an incremental repair
        * Launch a three node cluster
        * Create a keyspace with RF 3 and a table
        * Stop node3
        * Insert 100 rows
        * Restart node3
        * Issue an incremental repair
        * Insert 50 more rows
        * Perform a major compaction on node3
        * Verify all data is present
        # TODO: I have no idea what this is testing. The assertions do not verify anything meaningful.
        # TODO: Fix all the string formatting
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 3)
        session.execute("create table tab(key int PRIMARY KEY, val int);")

        node3.stop()

        for x in range(0, 100):
            session.execute("insert into tab(key,val) values(" + str(x) + ",0)")
        node1.flush()

        node3.start(wait_for_binary_proto=True)

        self.repair(node3)
        for x in range(0, 150):
            session.execute("insert into tab(key,val) values(" + str(x) + ",1)")

        cluster.flush()

        node3.nodetool('compact')

        for x in range(0, 150):
            assert_one(session, "select val from tab where key =" + str(x), [1])

    @since("2.2", max_version='4')
    def multiple_full_repairs_lcs_test(self):
        """
        @jira_ticket CASSANDRA-11172 - repeated full repairs should not cause infinite loop in getNextBackgroundTask
        This test only makes sense when anti-compaction is executed after full repairs, which
        is not the case on 4.0
        """
        cluster = self.cluster
        cluster.populate(2).start()
        node1, node2 = cluster.nodelist()
        for x in xrange(0, 2):
            node1.stress(['write', 'n=100k', 'no-warmup', '-rate', 'threads=10', '-schema', 'compaction(strategy=LeveledCompactionStrategy,sstable_size_in_mb=10)', 'replication(factor=2)'])
            cluster.flush()
            cluster.wait_for_compactions()
            self.repair(node1, options=["keyspace1 standard1"], incremental=False, force_anti_compaction=True)

        # Make sure anti-compaction was executed during test
        self.assertTrue(node1.grep_log("Starting anticompaction"), "Did not run anti-compaction with --run-anti-compaction option.")

    @attr('long')
    @skip('hangs CI')
    def multiple_subsequent_repair_test(self):
        """
        @jira_ticket CASSANDRA-8366

        There is an issue with subsequent inc repairs increasing load size.
        So we perform several repairs and check that the expected amount of data exists.
        * Launch a three node cluster
        * Write 5M rows with stress
        * Wait for minor compactions to finish
        * Issue an incremental repair on each node, sequentially
        * Issue major compactions on each node
        * Sleep for a while so load size can be propagated between nodes
        * Verify the correct amount of data is on each node
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        debug("Inserting data with stress")
        node1.stress(['write', 'n=5M', 'no-warmup', '-rate', 'threads=10', '-schema', 'replication(factor=3)'])

        debug("Flushing nodes")
        cluster.flush()

        debug("Waiting compactions to finish")
        cluster.wait_for_compactions()

        debug("Repairing node1")
        self.repair(node1)
        debug("Repairing node2")
        self.repair(node2)
        debug("Repairing node3")
        self.repair(node3)

        # Using "print" instead of debug() here is on purpose.  The compactions
        # take a long time and don't print anything by default, which can result
        # in the test being timed out after 20 minutes.  These print statements
        # prevent it from being timed out.
        print "compacting node1"
        node1.compact()
        print "compacting node2"
        node2.compact()
        print "compacting node3"
        node3.compact()

        # wait some time to be sure the load size is propagated between nodes
        debug("Waiting for load size info to be propagated between nodes")
        time.sleep(45)

        load_size_in_kb = float(sum(map(lambda n: n.data_size(), [node1, node2, node3])))
        load_size = load_size_in_kb / 1024 / 1024
        debug("Total Load size: {}GB".format(load_size))

        # There is still some overhead, but it's lot better. We tolerate 25%.
        expected_load_size = 4.5  # In GB
        assert_almost_equal(load_size, expected_load_size, error=0.25)

    @attr('resource-intensive')
    def sstable_marking_test_not_intersecting_all_ranges(self):
        """
        @jira_ticket CASSANDRA-10299
        * Launch a four node cluster
        * Insert data with stress
        * Issue an incremental repair on each node sequentially
        * Assert no extra, unrepaired sstables are generated
        """
        cluster = self.cluster
        cluster.populate(4).start()
        node1, node2, node3, node4 = cluster.nodelist()

        debug("Inserting data with stress")
        node1.stress(['write', 'n=3', 'no-warmup', '-rate', 'threads=1', '-schema', 'replication(factor=3)'])

        debug("Flushing nodes")
        cluster.flush()

        debug("Repairing node 1")
        self.repair(node1)
        debug("Repairing node 2")
        self.repair(node2)
        debug("Repairing node 3")
        self.repair(node3)
        debug("Repairing node 4")
        self.repair(node4)

        if cluster.version() >= '4.0':
            # sstables are compacted out of pending repair by a compaction
            for node in cluster.nodelist():
                node.nodetool('compact keyspace1 standard1')

        for out in (node.run_sstablemetadata(keyspace='keyspace1').stdout for node in cluster.nodelist() if len(node.get_sstables('keyspace1', 'standard1')) > 0):
            self.assertNotIn('Repaired at: 0', out)

    @no_vnodes()
    @since('4.0')
    def move_test(self):
        """ Test repaired data remains in sync after a move """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False, 'commitlog_sync_period_in_ms': 500})
        cluster.populate(4, tokens=[0, 2**32, 2**48, -(2**32)]).start()
        node1, node2, node3, node4 = cluster.nodelist()

        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 2}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")

        # insert some data
        stmt = SimpleStatement("INSERT INTO ks.tbl (k,v) VALUES (%s, %s)")
        for i in range(1000):
            session.execute(stmt, (i, i))

        self.repair(node1, options=['ks'])

        for i in range(1000):
            v = i + 1000
            session.execute(stmt, (v, v))

        # everything should be in sync
        for node in cluster.nodelist():
            result = self.repair(node, options=['ks', '--validate'])
            self.assertIn("Repaired data is in sync", result.stdout)

        node2.nodetool('move {}'.format(2**16))

        # everything should still be in sync
        for node in cluster.nodelist():
            result = self.repair(node, options=['ks', '--validate'])
            self.assertIn("Repaired data is in sync", result.stdout)

    @no_vnodes()
    @since('4.0')
    def decommission_test(self):
        """ Test repaired data remains in sync after a decommission """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False, 'commitlog_sync_period_in_ms': 500})
        cluster.populate(4).start()
        node1, node2, node3, node4 = cluster.nodelist()

        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 2}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")

        # insert some data
        stmt = SimpleStatement("INSERT INTO ks.tbl (k,v) VALUES (%s, %s)")
        for i in range(1000):
            session.execute(stmt, (i, i))

        self.repair(node1, options=['ks'])

        for i in range(1000):
            v = i + 1000
            session.execute(stmt, (v, v))

        # everything should be in sync
        for node in cluster.nodelist():
            result = self.repair(node, options=['ks', '--validate'])
            self.assertIn("Repaired data is in sync", result.stdout)

        node2.nodetool('decommission')

        # everything should still be in sync
        for node in [node1, node3, node4]:
            result = self.repair(node, options=['ks', '--validate'])
            self.assertIn("Repaired data is in sync", result.stdout)

    @no_vnodes()
    @since('4.0')
    def bootstrap_test(self):
        """ Test repaired data remains in sync after a bootstrap """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False, 'commitlog_sync_period_in_ms': 500})
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 2}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")

        # insert some data
        stmt = SimpleStatement("INSERT INTO ks.tbl (k,v) VALUES (%s, %s)")
        for i in range(1000):
            session.execute(stmt, (i, i))

        self.repair(node1, options=['ks'])

        for i in range(1000):
            v = i + 1000
            session.execute(stmt, (v, v))

        # everything should be in sync
        for node in [node1, node2, node3]:
            result = self.repair(node, options=['ks', '--validate'])
            self.assertIn("Repaired data is in sync", result.stdout)

        node4 = new_node(self.cluster)
        node4.start(wait_for_binary_proto=True)

        self.assertEqual(len(self.cluster.nodelist()), 4)
        # everything should still be in sync
        for node in self.cluster.nodelist():
            result = self.repair(node, options=['ks', '--validate'])
            self.assertIn("Repaired data is in sync", result.stdout)

    @since('3.0')
    def do_not_ressurrect_on_compaction_race_test(self):
        """
        @jira_ticket APOLLO-688
        """
        self._incremental_repair_data_ressurection_regression_test()

    @since('3.0', max_version='4')
    def do_not_ressurrect_on_anticompaction_start_failure_test(self):
        """
        @jira_ticket APOLLO-688
        """
        self._incremental_repair_data_ressurection_regression_test(anticompaction_fail_phase='start')

    @since('3.0', max_version='4')
    def do_not_ressurrect_on_anticompaction_middle_failure_test(self):
        """
        @jira_ticket APOLLO-688
        """
        self._incremental_repair_data_ressurection_regression_test(anticompaction_fail_phase='middle')

    @since('3.0', max_version='4')
    def do_not_ressurrect_on_anticompaction_end_failure_test(self):
        """
        @jira_ticket APOLLO-688
        """
        self._incremental_repair_data_ressurection_regression_test(anticompaction_fail_phase='end')

    def _incremental_repair_data_ressurection_regression_test(self, anticompaction_fail_phase=None):
        """
        @jira_ticket APOLLO-596
        * Table replicated in node1 and node2 (RF=2)
        * 10 rows inserted on both nodes
        * Node2 dies
        * 10 rows are deleted, but tombstones only go to node1 since node2 is down
        * Node2 recovers
        * Repair is run
          * SSTables containing 10 original rows are compacted before anti-compaction on node2, so cannot be marked as repaired
          * Tombstones for these rows are transferred from node1 to node2
          * On node1, all SStables are successfully marked as repaired
        * Rows are purged on node1 after gc_grace_seconds=60
        * On the next repair we must ensure data is not ressurrected on node1
          * Before APOLLO-596, the rows could not be purged on node2 because they were in separate compactions bucket
          so in the next repair only the data rows without the tombstones were propagated to node1 which already purged
          these rows.
        """
        # we set a higher number of nodes when testing anti-compaction failure
        # to make sure there is anti-compaction, otherwise all SSTables will have
        # its repairedAt mutated without anti-compaction, since with N=2 and
        # RF=2 all nodes are responsible for all cluster data
        fail_during_anticompaction = anticompaction_fail_phase is not None
        nodes = 3 if fail_during_anticompaction else 2
        # Since there are more nodes, repair will take longer and thus we need
        # to increase gc_grace_seconds when failur_during_anti_compaction=true
        gc_grace_seconds = 90 if fail_during_anticompaction else 60

        cluster = self.cluster
        cluster.populate(nodes)
        node1, node2 = cluster.nodelist()[:2]

        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.set_batch_commitlog(enabled=True)

        if fail_during_anticompaction:
            self.ignore_log_patterns = ["Dummy failure"]

        debug("Setting up byteman on {}".format(node2.name))
        # set up byteman
        node2.byteman_port = '8100'
        node2.import_config_files()

        debug("Starting cluster")
        cluster.start()

        session = self.patient_exclusive_cql_connection(node1)
        # create keyspace with RF=2 to be able to be repaired
        create_ks(session, 'ks', 2)

        # create table with short gc_gs
        debug("Creating table with gc_grace_seconds={}".format(gc_grace_seconds))
        session.execute("""
            CREATE TABLE cf1 (
                key text,
                c1 text,
                c2 text,
                PRIMARY KEY (key, c1)
            )
            WITH gc_grace_seconds={}
            AND dclocal_read_repair_chance=0.0
            AND compaction = {{'class': 'SizeTieredCompactionStrategy', 'enabled': 'false',
                               'min_threshold': 2, 'max_threshold': 2}};
        """.format(gc_grace_seconds))

        debug("Populating table ks.cf2")

        # insert some data in multiple sstables
        for i in xrange(0, 10):
            for j in xrange(0, 100):
                query = SimpleStatement("INSERT INTO cf1 (key, c1, c2) VALUES ('k{}', 'v{}', 'value')".format(i, j), consistency_level=ConsistencyLevel.ONE)
                session.execute(query)
            cluster.flush()

        debug("Stopping node2")

        # take down node2, so that only node1 has gc-able data
        node2.stop(wait_other_notice=True)

        debug("Deleting data on node1")

        deletion_time = time.time()
        # Create SSTables with tombstones,  one with row tombstone and another with cell range tombstones
        for i in xrange(0, 5):
            query = SimpleStatement("DELETE FROM cf1 WHERE key='k{}'".format(i), consistency_level=ConsistencyLevel.ONE)
            session.execute(query)
        cluster.flush()
        for i in xrange(5, 10):
            for j in xrange(0, 100):
                query = SimpleStatement("DELETE FROM cf1 WHERE key='k{}' AND c1='v{}'".format(i, j), consistency_level=ConsistencyLevel.ONE)
                session.execute(query)
        cluster.flush()
        # Create another non-tombstoned SSTable with which should be marked as repaired by node2
        for i in xrange(11, 20):
            for j in xrange(0, 100):
                query = SimpleStatement("INSERT INTO cf1 (key, c1, c2) VALUES ('k{}', 'v{}', 'value')".format(i, j), consistency_level=ConsistencyLevel.ONE)
                session.execute(query)
        cluster.flush()

        debug("Starting node2 with byteman enabled")

        # bring up node2 with byteman installedand run incremental repair
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)

        debug("Install byteman rule on node2")
        if fail_during_anticompaction:
            anticompaction_fail_script = "anticompaction_fail_{}.btm".format(anticompaction_fail_phase)
            node2.byteman_submit(['./byteman/{}'.format(anticompaction_fail_script)])
        else:
            node2.byteman_submit(['./byteman/minor_compact_sstables_during_repair.btm'])

        mark = node2.mark_log()

        debug("Run repair on node1")
        self.repair(node1, options=['ks cf1'])

        if fail_during_anticompaction:
            self.assertTrue(node2.grep_log("Dummy failure", from_mark=mark))
        elif self.cluster.version() < "4.0":
            # Message is only print on 3.X
            self.assertTrue(node2.grep_log("because they potentially shadow rows compacted during repair",
                                           from_mark=mark, filename="debug.log"))

        # When testing anti-compaction failure, the RF is higher (3), so we need to repair node3
        # to ensure all dataset will be repaired
        if fail_during_anticompaction:
            debug("Run repair on node3")
            node3 = cluster.nodelist()[2]
            self.repair(node3, options=['ks cf1'])

        repair_time = time.time()

        # Wait for tombstones to expire
        time_until_expiration = gc_grace_seconds - (repair_time - deletion_time)
        self.assertGreater(time_until_expiration, 0, "Data must be repaired before gc_grace_seconds={} but took {} seconds to repair."
                                                     .format(gc_grace_seconds, repair_time - deletion_time))
        debug("Will sleep {} seconds to wait for data to expire.".format(time_until_expiration))
        time.sleep(time_until_expiration)

        # Run compaction so tombstones will be garbage collected
        node1.compact()
        node2.compact()
        if fail_during_anticompaction:
            node3 = cluster.nodelist()[2]
            node3.compact()

        # Check deleted data is not present
        for i in xrange(0, 10):
            assert_none(session, "SELECT * FROM cf1 WHERE key = 'k{}'".format(i), cl=ConsistencyLevel.ALL)
        # Check non-tombstoned data is present
        for i in xrange(11, 20):
            for j in xrange(0, 100):
                assert_all(session, "SELECT * FROM cf1 WHERE key = 'k{}' and c1 = 'v{}'".format(i, j),
                           [['k{}'.format(i), 'v{}'.format(j), 'value']], cl=ConsistencyLevel.ALL)

        debug("Run repair again")
        self.repair(node1, options=['ks cf1'])
        # When testing anti-compaction failure, the RF is higher (3), so we need to repair node3
        # to ensure all dataset will be repaired
        if fail_during_anticompaction:
            node3 = cluster.nodelist()[2]
            self.repair(node3, options=['ks cf1'])

        node1.compact()
        node2.compact()
        if fail_during_anticompaction:
            node3 = cluster.nodelist()[2]
            node3.compact()

        # Check deleted data is not present
        for i in xrange(0, 10):
            assert_none(session, "SELECT * FROM cf1 WHERE key = 'k{}'".format(i), cl=ConsistencyLevel.ALL)
        # Check non-tombstoned data is present
        for i in xrange(11, 20):
            for j in xrange(0, 100):
                assert_all(session, "SELECT * FROM cf1 WHERE key = 'k{}' and c1 = 'v{}'".format(i, j),
                           [['k{}'.format(i), 'v{}'.format(j), 'value']], cl=ConsistencyLevel.ALL)

    @since('3.0')
    def inc_repair_on_mv_or_cdc_tables_test(self):
        """
        On dse5.0 and dse5.1, incremental repair on MV tables should fallback to full repair and log a warning.
        On dse6.0, incremental repair on MV tables should fail.
        """
        cluster = self.cluster
        cluster.populate(3)
        opts = {'hinted_handoff_enabled': False}

        # cdc is only available on 3.10+
        if cluster.version() >= '3.10':
            opts['cdc_enabled'] = True
        elif cluster.version() >= '4.0':
            self.ignore_log_patterns = ["incremental repair is not supported on tables with materialized views or CDC-enabled"]

        cluster.set_configuration_options(values=opts)
        cluster.start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)

        debug("Creating keyspaces")
        session.execute("CREATE KEYSPACE mv_only_ks WITH replication={'class':'SimpleStrategy', 'replication_factor':3}")
        session.execute("CREATE KEYSPACE mixed_ks WITH replication={'class':'SimpleStrategy', 'replication_factor':3}")
        session.execute("CREATE KEYSPACE non_mv_ks WITH replication={'class':'SimpleStrategy', 'replication_factor':3}")

        debug("Creating tables and views")
        session.execute("CREATE TABLE mv_only_ks.t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute(("CREATE MATERIALIZED VIEW mv_only_ks.t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        session.execute("CREATE TABLE mixed_ks.t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute("CREATE TABLE mixed_ks.u (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute(("CREATE MATERIALIZED VIEW mixed_ks.t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))
        # cdc is only available on 3.10+
        if cluster.version() >= "3.10":
            session.execute("CREATE TABLE mixed_ks.cdc (id int PRIMARY KEY, v int, v2 text, v3 decimal) WITH cdc = true")

        session.execute("CREATE TABLE non_mv_ks.t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")

        node1, node2, node3 = self.cluster.nodelist()

        debug('Shutdown node2')
        node2.stop(wait_other_notice=True)

        debug('Inserting data')

        base_tables = ["mv_only_ks.t", "mixed_ks.t", "mixed_ks.u", "non_mv_ks.t"]
        mvs = ["mv_only_ks.t_by_v", "mixed_ks.t_by_v"]

        for i in xrange(1000):
            for base_table in base_tables:
                session.execute("INSERT INTO {base} (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0)".format(base=base_table, v=i))

        debug('Waiting for views to be inserted')
        self._replay_batchlogs()

        debug('Verify the data in the views with CL=ONE')
        for i in xrange(1000):
            for view in mvs:
                assert_one(
                    session,
                    "SELECT * FROM {} WHERE v = {}".format(view, i),
                    [i, i, 'a', 3.0]
                )

        debug('Verify the data in the views with CL=ALL. All should be unavailable.')
        for i in xrange(1000):
            for view in mvs:
                statement = SimpleStatement(
                    "SELECT * FROM {} WHERE v = {}".format(view, i),
                    consistency_level=ConsistencyLevel.ALL
                )

                assert_unavailable(
                    session.execute,
                    statement
                )

        debug('Start node2, and repair')
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        try:
            stdout, stderr, rc = self.repair(node1)
        except ToolError as e:
            if self.cluster.version() >= "4.0":
                self.assertIn("because incremental repair is not supported on tables with materialized views or CDC-enabled",
                              e.stderr)
                return  # Nothing else to validate here
            else:
                self.fail("Running incremental repair on tables with MVs or CD should fallback "
                          "to full repairs.")

        # cdc is only available on 3.10+
        mixed_ks_tables = '\[t\_by\_v, cdc, t\]' if cluster.version() >= '3.10' else '\[t\_by\_v, t\]'

        to_search = 'WARN: Incremental repair is not supported on tables with Materialized Views.* ' \
                    'Running full repairs on table\(s\) mixed_ks.{}.'.format(mixed_ks_tables)

        debug("Version: " + str(cluster.version()))
        debug("to search: " + str(to_search))

        debug(stdout)
        self.assertTrue(re.compile('WARN: Incremental repair is not supported on '
                                   'tables with Materialized Views.* Running full '
                                   'repairs on table\(s\) mv_only_ks.\[t\_by\_v, t\].').search(stdout))
        self.assertTrue(re.compile(to_search).search(stdout))

        debug('Verify the data in the MV with CL=ONE. All should be available now.')
        for i in xrange(1000):
            for view in mvs:
                assert_one(
                    session,
                    "SELECT * FROM {} WHERE v = {}".format(view, i),
                    [i, i, 'a', 3.0],
                    cl=ConsistencyLevel.ONE
                )

        self.assertTrue(node1.grep_log('repairing keyspace non_mv_ks .* incremental: true'), "Should have incrementally repaired non_mv_ks.")
        self.assertTrue(node1.grep_log('repairing keyspace mv_only_ks .* incremental: false'), "Should not have incrementally repaired mv_only_ks.")
        self.assertTrue(node1.grep_log('repairing keyspace mixed_ks .* incremental: false.*{}'.format(mixed_ks_tables)), "Should have split mixed_ks repair in full and incremental components.")
        self.assertTrue(node1.grep_log('repairing keyspace mixed_ks .* incremental: true.*\[u\]'), "Should have split mixed_ks repair in full and incremental components.")

    def repair(self, node, options=None, incremental=True, force_anti_compaction=False):
        if options is None:
            options = []

        args = ["repair"]
        args += get_repair_options(self.cluster.version(), incremental, force_anti_compaction)

        args = args + options
        cmd = ' '.join(args)
        debug("will run " + cmd)
        return node.nodetool(cmd)

    def _replay_batchlogs(self):
        debug("Replaying batchlog on all nodes")
        for node in self.cluster.nodelist():
            if node.is_running():
                node.nodetool("replaybatchlog")

    def wait_sstables_to_be_marked_repaired(self, node, num_sstables=3):
        mark = node.mark_log()
        node.nodetool('disableautocompaction keyspace1 standard1')
        node.nodetool('enableautocompaction keyspace1 standard1')
        for n in xrange(0, num_sstables):
            debug("Waiting for sstable {} to be marked repaired.".format(n))
            node.watch_log_for("Removing compaction strategy", from_mark=mark, timeout=10, filename="debug.log")
            mark = node.mark_log()

    @since('3.0')
    def mark_unrepaired_test(self):
        """
        * Launch a four node, two DC cluster
        * Start a normal repair
        * Assert every node anticompacts
        @jira_ticket APOLLO-692
        """
        cluster = self.cluster
        debug("Starting cluster..")
        cluster.populate(2).start()
        node1, node2 = cluster.nodelist()

        debug("Inserting data (2 sstables)..")
        node1.stress(['write', 'n=10', 'no-warmup', '-schema', 'replication(factor=2)', 'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)'])
        cluster.flush()
        node1.stress(['write', 'n=10', 'no-warmup', '-schema', 'replication(factor=2)', 'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)'])
        cluster.flush()

        debug("Repairing..")
        self.repair(node1, options=["keyspace1 standard1"])

        debug("Checking all sstables were marked as repaired..")
        for node in cluster.nodelist():
            if self.cluster.version() >= '4.0':  # Requires compaction to run to mark SSTables as repaired
                self.wait_sstables_to_be_marked_repaired(node)
            self.assertAllRepairedSSTables(node, 'keyspace1')

        stdout, _, _ = node1.nodetool("info")
        self.assertTrue("Percent Repaired       : 100.0%" in stdout)

        debug("Marking all sstables from keyspace1 as unrepaired without --force - should throw exception")
        with self.assertRaises(ToolError) as ctx:
            node1.nodetool("mark_unrepaired keyspace1")

        self.assertTrue("WARNING: This operation will mark all SSTables of keyspace keyspace1 as unrepaired, "
                        "potentially creating new compaction tasks. Only use this when no longer running incremental "
                        "repair on this node. Use --force option to confirm." in ctx.exception.stdout)

        debug("Marking all sstables from keyspace1 as unrepaired with --force on node1")
        stdout, _, _ = node1.nodetool("mark_unrepaired keyspace1 --force")
        debug(stdout)
        num_sstables = 6 if cluster.version() >= "3.1" else 2  # 6696
        self.assertTrue("Marked {} SSTable(s) as unrepaired.".format(num_sstables) in stdout)

        debug("Checking all sstables were marked as unrepaired")
        stdout, _, _ = node1.nodetool("info")
        self.assertTrue("Percent Repaired       : 0.0%" in stdout)

        debug("Major compact node2")
        node2.nodetool("compact keyspace1")

        self.assertNoRepairedSSTables(node1, 'keyspace1')
        self.assertAllRepairedSSTables(node2, 'keyspace1')

        debug("Marking all sstables from keyspace1 as unrepaired with --force on node2")
        stdout, _, _ = node2.nodetool("mark_unrepaired keyspace1 --force")
        debug(stdout)
        num_sstables = 3 if cluster.version() >= '3.1' else 1  # 6696
        self.assertTrue("Marked {} SSTable(s) as unrepaired.".format(num_sstables) in stdout)

        self.assertNoRepairedSSTables(node1, 'keyspace1')
        self.assertNoRepairedSSTables(node2, 'keyspace1')

        debug("Try to mark again as unrepaired - Should say there are no sstables to mark as unrepaired")
        stdout, _, _ = node1.nodetool("mark_unrepaired keyspace1 --force")
        self.assertTrue("No repaired SSTables to mark as unrepaired." in stdout)

        debug("Repairing again")
        self.repair(node1, options=["keyspace1 standard1"])

        debug("Checking all sstables were marked as repaired..")
        for node in cluster.nodelist():
            if self.cluster.version() >= '4':  # Requires compaction to run to mark SSTables as repaired
                self.wait_sstables_to_be_marked_repaired(node)
            self.assertAllRepairedSSTables(node, 'keyspace1')

    def test_consistent_repair_failure_at_anticompaction_doesnt_cause_hanging(self):
        """
        @jira_ticket APOLLO-814
        """
        cluster = self.cluster
        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})

        debug("Setting up byteman on node2")
        node2.byteman_port = '8100'
        node2.import_config_files()

        debug("Starting cluster")
        cluster.start()

        session1 = self.patient_exclusive_cql_connection(node1)
        create_ks(session1, 'ks', 2)

        debug("Creating table")
        session1.execute("""
            CREATE TABLE cf (
                key text,
                v text,
                PRIMARY KEY (key)
            )
            WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'};
        """)

        debug("Inserting row")
        query = SimpleStatement("INSERT INTO ks.cf (key, v) VALUES ('1', 'v1')", consistency_level=ConsistencyLevel.ONE)
        session1.execute(query)
        cluster.flush()

        debug("Installing byteman rule on node2 to kill the node")
        node2.byteman_submit(['./byteman/repair_die_after_anticompaction.btm'])

        debug("Running repair on node1")
        executor = ThreadPoolExecutor(max_workers=1)
        f = executor.submit(lambda: self.repair(node1, options=['ks cf']))

        debug("Waiting for repair to abort")
        self.assertIsNotNone(f.exception(60))

    def test_restarting_after_failure_doesnt_unrepair_sessions(self):
        """
        @jira_ticket APOLLO-821
        """
        cluster = self.cluster
        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})

        debug("Setting up byteman on node2")
        node2.byteman_port = '8100'
        node2.import_config_files()

        debug("Starting cluster")
        cluster.start()

        session1 = self.patient_exclusive_cql_connection(node1)
        create_ks(session1, 'ks', 2)

        debug("Creating table")
        session1.execute("""
            CREATE TABLE cf (
                key text,
                v text,
                PRIMARY KEY (key)
            )
            WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'};
        """)

        debug("Inserting row")
        query = SimpleStatement("INSERT INTO ks.cf (key, v) VALUES ('1', 'v1')", consistency_level=ConsistencyLevel.ONE)
        session1.execute(query)
        cluster.flush()

        preNode2DownMark = node1.mark_log()

        debug("Installing byteman rule on node2 to kill the node")
        node2.byteman_submit(['./byteman/repair_die_after_anticompaction.btm'])

        debug("Running repair on node1")
        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(lambda: self.repair(node1, options=['ks cf']))

        debug("Waiting for node2 to die")
        node1.watch_log_for("InetAddress /127.0.0.2 is now DOWN", from_mark=preNode2DownMark, timeout=60)
        node2.stop(gently=False)
        debug("Sleeping to avoid picking up repair messages left by node1")
        time.sleep(30)

        debug("Setting up byteman startup rule on node2")
        node2.byteman_startup_script = "./byteman/ars_slow_start.btm"
        node2.import_config_files()

        node2.start()

        debug("Asserting sstables are still marked as pending repair")
        self.assertAllPendingRepairSSTables(node2, 'ks')

    def test_commit_failure_aborts_repair(self):
        """
        @jira_ticket APOLLO-815
        """
        cluster = self.cluster
        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})

        debug("Setting up byteman on node2")
        node2.byteman_port = '8100'
        node2.import_config_files()

        debug("Starting cluster")
        node1.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=['-Dcassandra.finalize_commit_timeout_seconds=10'])
        node2.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=['-Dcassandra.finalize_commit_timeout_seconds=10'])

        session1 = self.patient_exclusive_cql_connection(node1)
        create_ks(session1, 'ks', 2)

        debug("Creating table")
        session1.execute("""
            CREATE TABLE cf (
                key text,
                v text,
                PRIMARY KEY (key)
            )
            WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'};
        """)

        debug("Inserting row")
        query = SimpleStatement("INSERT INTO ks.cf (key, v) VALUES ('1', 'v1')", consistency_level=ConsistencyLevel.ONE)
        session1.execute(query)
        cluster.flush()

        debug("Installing byteman rule on node2 to kill the node at commit")
        node2.byteman_submit(['./byteman/repair_die_at_commit.btm'])

        debug("Running repair on node1")
        executor = ThreadPoolExecutor(max_workers=1)
        f = executor.submit(lambda: self.repair(node1, options=['ks cf']))

        debug("Waiting for repair to abort")
        self.assertIsNotNone(f.exception(60))

    def test_consistent_repair_recovers_from_anticompaction_failure(self):
        self._test_consistent_repair_recovers_from_failure("after_anticompaction",
                                                           ConsistentState.FAILED, ConsistentState.PREPARING, ConsistentState.FAILED, ConsistentState.FAILED)

    def test_consistent_repair_recovers_from_commit_failure(self):
        self._test_consistent_repair_recovers_from_failure("at_commit",
                                                           ConsistentState.FINALIZED, ConsistentState.REPAIRING, ConsistentState.FINALIZED, ConsistentState.FINALIZED)

    def _test_consistent_repair_recovers_from_failure(self, stage, node1FailureState, node2FailureState, node1FinalState, node2FinalState):
        """
        @jira_ticket APOLLO-815
        """
        cluster = self.cluster
        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})

        debug("Setting up byteman on node2")
        node2.byteman_port = '8100'
        node2.import_config_files()

        debug("Starting cluster")
        cluster.start()

        session1 = self.patient_exclusive_cql_connection(node1)
        session2 = self.patient_exclusive_cql_connection(node2)
        create_ks(session1, 'ks', 2)

        debug("Creating table")
        session1.execute("""
            CREATE TABLE cf (
                key text,
                v text,
                PRIMARY KEY (key)
            )
            WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'};
        """)

        debug("Stopping node2 so first row version is inserted only on node 1")
        node2.stop(wait_other_notice=True)

        debug("Inserting first row version")
        query = SimpleStatement("INSERT INTO ks.cf (key, v) VALUES ('1', 'v1')", consistency_level=ConsistencyLevel.ONE)
        session1.execute(query)
        cluster.flush()

        debug("Stopping node1 and starting node2 to insert new row version on node2")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        session2 = self.patient_exclusive_cql_connection(node2)
        node1.stop()

        debug("Inserting new row version")
        query = SimpleStatement("INSERT INTO ks.cf (key, v) VALUES ('1', 'v2')", consistency_level=ConsistencyLevel.ONE)
        session2.execute(query)
        cluster.flush()

        debug("Restarting node1")
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        session1 = self.patient_exclusive_cql_connection(node1)

        debug("Asserting sstables are unrepaired")
        self.assertNoRepairedSSTables(node1, 'ks')
        self.assertNoRepairedSSTables(node2, 'ks')

        preNode2DownMark = node1.mark_log()

        rule = './byteman/repair_die_{}.btm'.format(stage)
        debug("Installing byteman rule {} on node2 to kill the node: this will leave the sstables in the pending bucket".format(rule))
        node2.byteman_submit([rule])

        debug("Running repair on node1")
        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(lambda: self.repair(node1, options=['ks cf']))

        debug("Waiting for node2 to die")
        node1.watch_log_for("InetAddress /127.0.0.2 is now DOWN", from_mark=preNode2DownMark, timeout=60)
        node2.stop(gently=False)
        debug("Sleeping to avoid picking up repair messages left by node1")
        time.sleep(30)

        debug("Restarting node2 which was killed during repair")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        debug("Asserting node2 sstables are marked as pending repair")
        # node1 sstables might have been marked as unrepaired by background compaction
        self.assertAllPendingRepairSSTables(node2, 'ks')

        repairSession = self.getLocalSessionId(node1)

        debug("Asserting failure session state for node1 is {} and for node2 is {}".format(node1FailureState, node2FailureState))
        self.checkLocalSessionState(node1, repairSession, node1FailureState)
        self.checkLocalSessionState(node2, repairSession, node2FailureState)

        debug("Re-running repair on node1 repairs all sstables")
        self.repair(node1, options=['ks cf'])
        self.assertAllRepairedSSTables(node1, 'ks')
        self.assertAllRepairedSSTables(node2, 'ks')

        debug("Asserting final session state for node1 is {} and for node2 is {}".format(node1FinalState, node2FinalState))
        self.checkLocalSessionState(node1, repairSession, node1FinalState)
        self.checkLocalSessionState(node2, repairSession, node2FinalState)

    def test_consistent_repair_cleanup_after_anticompaction_failure(self):
        self._test_consistent_repair_cleanup("after_anticompaction", False)

    def test_consistent_repair_cleanup_after_streaming_failure(self):
        self.ignore_log_patterns = (r'Stream failed')
        self._test_consistent_repair_cleanup("at_streaming", False)

    def test_consistent_repair_cleanup_after_commit_failure(self):
        self._test_consistent_repair_cleanup("at_commit", True)

    def _test_consistent_repair_cleanup(self, stage, finalized):
        """
        @jira_ticket APOLLO-816
        """
        cluster = self.cluster
        cluster.populate(3)
        node1, node2, node3 = cluster.nodelist()

        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})

        debug("Setting up byteman on node2")
        node2.byteman_port = '8100'
        node2.import_config_files()

        debug("Starting cluster")
        cluster.start()

        session1 = self.patient_exclusive_cql_connection(node1)
        session2 = self.patient_exclusive_cql_connection(node2)
        create_ks(session1, 'ks', 3)

        debug("Creating table")
        session1.execute("""
            CREATE TABLE cf (
                key text,
                v text,
                PRIMARY KEY (key)
            )
            WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'};
        """)

        debug("Stopping node2 so first row version is inserted only on node 1")
        node2.stop(wait_other_notice=True)

        debug("Inserting first row version")
        query = SimpleStatement("INSERT INTO ks.cf (key, v) VALUES ('1', 'v1')", consistency_level=ConsistencyLevel.ONE)
        session1.execute(query)
        cluster.flush()

        debug("Stopping node1 and starting node2 to insert new row version on node2")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        session2 = self.patient_exclusive_cql_connection(node2)
        node1.stop()

        debug("Inserting new row version")
        query = SimpleStatement("INSERT INTO ks.cf (key, v) VALUES ('1', 'v2')", consistency_level=ConsistencyLevel.ONE)
        session2.execute(query)
        cluster.flush()

        debug("Restarting node1")
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        session1 = self.patient_exclusive_cql_connection(node1)

        preNode2DownMark = node1.mark_log()

        rule = './byteman/repair_die_{}.btm'.format(stage)
        debug("Installing byteman rule {} on node2 to kill the node".format(rule))
        node2.byteman_submit([rule])

        debug("Running repair on node1")
        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(lambda: self.repair(node1, options=['ks cf']))

        debug("Waiting for node2 to die")
        node1.watch_log_for("InetAddress /127.0.0.2 is now DOWN", from_mark=preNode2DownMark, timeout=60)
        node2.stop(gently=False)
        debug("Sleeping to avoid picking up repair messages left by node1")
        time.sleep(30)

        debug("Restarting node2 which was killed during repair")
        node2.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=['-Dcassandra.repair_cleanup_interval_seconds=5', '-Dcassandra.repair_status_check_timeout_seconds=5'])

        debug("Waiting for the cleanup thread to run")
        preNode2CleanupMark = node2.mark_log()
        node2.watch_log_for("Running LocalSessions.cleanup", from_mark=preNode2CleanupMark, timeout=60, filename='debug.log')

        debug("Checking all nodes have the session marked as {}".format("finalized" if finalized else "failed"))
        for node in cluster.nodelist():
            session = self.patient_exclusive_cql_connection(node)
            results = list(session.execute("SELECT * FROM system.repairs"))
            self.assertEqual(len(results), 1)
            result = results[0]
            self.assertEqual(result.state, ConsistentState.FINALIZED if finalized else ConsistentState.FAILED)

        debug("Checking all nodes have their sstables {}".format("repaired" if finalized else "unrepaired"))
        for node in cluster.nodelist():
            node.nodetool('compact ks cf')
            if (finalized):
                self.assertAllRepairedSSTables(node, 'ks')
            else:
                self.assertNoRepairedSSTables(node, 'ks')

    def getLocalSessionId(self, node):
        session = self.patient_exclusive_cql_connection(node)
        results = list(session.execute("SELECT parent_id FROM system.repairs"))
        self.assertEqual(len(results), 1)
        return results[0].parent_id

    def checkLocalSessionState(self, node, id, state):
        session = self.patient_exclusive_cql_connection(node)
        results = list(session.execute("SELECT * FROM system.repairs WHERE parent_id=%s", [id]))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].state, state)

    @since('3.0', max_version='4')
    def non_anticompacted_metric_test(self):
        """
        @jira_ticket APOLLO-689
        - Run repair on table replicated on node1 and node2
        - Simulate race on node2 by running compaction during repair with byteman
        - Check that warning was logged on node2 about not all SSTables being anti-compacted
        - Check AntiCompactedBytes > 0 on node1 and NonAntiCompactedBytes > 0 on node2
        """
        cluster = self.cluster
        cluster.populate(2)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.set_batch_commitlog(enabled=True)
        node1, node2 = cluster.nodelist()

        debug("Setting up byteman on {}".format(node2.name))
        # set up byteman
        node2.byteman_port = '8100'
        node2.import_config_files()

        # allow metrics to be fetched
        remove_perf_disable_shared_mem(node1)
        remove_perf_disable_shared_mem(node2)

        debug("Starting nodes")
        cluster.start()
        node1, node2 = cluster.nodelist()

        debug("Creating tables")
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 2)
        session.execute("create table tab(key int PRIMARY KEY, val int) "
                        "WITH compaction = {'class': 'SizeTieredCompactionStrategy', "
                        "'enabled': 'false'};")

        debug("Stopping node2")
        node2.stop(wait_other_notice=True)

        debug("Insert 50 rows")
        for x in range(0, 50):
            session.execute("insert into tab(key,val) values(" + str(x) + ",0)")
        node1.flush()

        debug("Starting node2")
        node2.start(wait_for_binary_proto=True)

        debug("Stopping node1")
        node1.stop(wait_other_notice=True)

        debug("Insert 60 rows")
        session = self.patient_cql_connection(node2)
        session.execute('USE ks')
        for x in range(40, 100):
            session.execute("insert into tab(key,val) values(" + str(x) + ",1)")
        cluster.flush()

        debug("Starting node1")
        node1.start(wait_for_binary_proto=True)

        debug("Install byteman rule on node2")
        node2.byteman_submit(['./byteman/major_compact_sstables_during_repair.btm'])

        debug("Repairing node2")
        self.repair(node2)

        # Check that warning was logged on node2 but not on node1
        debug("Checking that node2 logged warning")
        self.assertTrue(node2.grep_log('could not be marked as repaired because'))
        self.assertFalse(node1.grep_log('could not be marked as repaired because'))

        debug("Checking anti-compacted metrics on node1 and node2")
        self.assertGreater(table_metric(node1, "ks", "tab", 'AntiCompactedBytes'), 0)
        self.assertEqual(table_metric(node1, "ks", "tab", 'NonAntiCompactedBytes'), 0)
        self.assertEqual(table_metric(node2, "ks", "tab", 'AntiCompactedBytes'), 0)
        self.assertGreater(table_metric(node2, "ks", "tab", 'NonAntiCompactedBytes'), 0)

        debug("Checking data")
        for x in range(0, 40):
            assert_one(session, "select val from tab where key =" + str(x), [0], cl=ConsistencyLevel.ALL)

        for x in range(40, 100):
            assert_one(session, "select val from tab where key =" + str(x), [1], cl=ConsistencyLevel.ALL)

    @since('4.0')
    def force_test(self):
        """
        forcing an incremental repair should incrementally repair any nodes
        that are up, but should not promote the sstables to repaired
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False, 'num_tokens': 1, 'commitlog_sync_period_in_ms': 500})
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")
        stmt = SimpleStatement("INSERT INTO ks.tbl (k,v) VALUES (%s, %s)")
        stmt.consistency_level = ConsistencyLevel.ALL
        for i in range(10):
            session.execute(stmt, (i, i))

        node2.stop()

        # repair should fail because node2 is down
        with self.assertRaises(ToolError):
            node1.repair(options=['ks'])

        # run with force flag
        node1.repair(options=['ks', '--force'])

        # ... and verify nothing was promoted to repaired
        self.assertNoRepairedSSTables(node1, 'ks')
        self.assertNoRepairedSSTables(node2, 'ks')

    @since('4.0')
    def hosts_test(self):
        """
        running an incremental repair with hosts specified should incrementally repair
        the given nodes, but should not promote the sstables to repaired
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False, 'num_tokens': 1, 'commitlog_sync_period_in_ms': 500})
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")
        stmt = SimpleStatement("INSERT INTO ks.tbl (k,v) VALUES (%s, %s)")
        stmt.consistency_level = ConsistencyLevel.ALL
        for i in range(10):
            session.execute(stmt, (i, i))

        # run with force flag
        node1.repair(options=['ks', '-hosts', ','.join([node1.address(), node2.address()])])

        # ... and verify nothing was promoted to repaired
        self.assertNoRepairedSSTables(node1, 'ks')
        self.assertNoRepairedSSTables(node2, 'ks')

    @since('4.0')
    def subrange_test(self):
        """
        running an incremental repair with hosts specified should incrementally repair
        the given nodes, but should not promote the sstables to repaired
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False,
                                                  'num_tokens': 1,
                                                  'commitlog_sync_period_in_ms': 500,
                                                  'partitioner': 'org.apache.cassandra.dht.Murmur3Partitioner'})
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")
        stmt = SimpleStatement("INSERT INTO ks.tbl (k,v) VALUES (%s, %s)")
        stmt.consistency_level = ConsistencyLevel.ALL
        for i in range(10):
            session.execute(stmt, (i, i))

        for node in cluster.nodelist():
            node.flush()
            self.assertNoRepairedSSTables(node, 'ks')

        # only repair the partition k=0
        token = Murmur3Token.from_key(str(bytearray([0, 0, 0, 0])))
        # import ipdb; ipdb.set_trace()
        # run with force flag
        node1.repair(options=['ks', '-st', str(token.value - 1), '-et', str(token.value)])

        # verify we have a mix of repaired and unrepaired sstables
        self.assertRepairedAndUnrepaired(node1, 'ks')
        self.assertRepairedAndUnrepaired(node2, 'ks')
        self.assertRepairedAndUnrepaired(node3, 'ks')
