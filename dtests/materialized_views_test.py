import collections
import re
import sys
import time
import traceback
from enum import Enum  # Remove when switching to py3
from functools import partial
from multiprocessing import Process, Queue
from unittest import skip, skipIf

from dse import ConsistencyLevel, InvalidRequest, WriteFailure
from dse.cluster import Cluster, NoHostAvailable
from dse.concurrent import execute_concurrent_with_args
from dse.query import SimpleStatement
from nose.plugins.attrib import attr
from nose.tools import assert_equal

from dtest import Tester, debug, supports_v5_protocol
from tools.assertions import (assert_all, assert_crc_check_chance_equal,
                              assert_invalid, assert_none, assert_one,
                              assert_unavailable)
from tools.data import create_ks, rows_to_list
from tools.decorators import since, since_dse
from tools.interceptors import Verb, delaying_interceptor
from tools.jmxutils import (JolokiaAgent, make_mbean,
                            remove_perf_disable_shared_mem)
from tools.misc import get_ip_from_node, new_node
from tools.preparation import jvm_args

# CASSANDRA-10978. Migration wait (in seconds) to use in bootstrapping tests. Needed to handle
# pathological case of flushing schema keyspace for multiple data directories. See CASSANDRA-6696
# for multiple data directory changes and CASSANDRA-10421 for compaction logging that must be
# written.
MIGRATION_WAIT = 5


@since('3.0')
class TestMaterializedViews(Tester):
    """
    Test materialized views implementation.
    @jira_ticket CASSANDRA-6477
    @since 3.0
    """

    def _rows_to_list(self, rows):
        new_list = [list(row) for row in rows]
        return new_list

    def is_legacy(self):
        return True

    def wrap_jvm_args(self, jvm_args=[]):
        if self.is_legacy():
            jvm_args.append("-Dcassandra.mv.create_legacy_schema=true")
        else:
            jvm_args.append("-Dcassandra.mv.allow_multiple_base_regular_columns_in_view_primary_keys=true")
        return jvm_args

    def prepare(self, user_table=False, rf=1, options=None, nodes=3,
                install_byteman=False, enable_batch_commitlog=False,
                interceptors=None, **kwargs):
        cluster = self.cluster
        cluster.populate([nodes, 0], install_byteman=install_byteman)
        if options:
            cluster.set_configuration_options(values=options)
        if enable_batch_commitlog:
            cluster.set_batch_commitlog(enabled=True)

        # We'll use JMX to control interceptors, and what requires this apparently
        if interceptors:
            for node in cluster.nodelist():
                remove_perf_disable_shared_mem(node)

        if self.is_legacy():
            debug("Creating legacy materialized view cluster")
        jvm_arguments = jvm_args(interceptors=interceptors)
        cluster.start(jvm_args=self.wrap_jvm_args(jvm_args=jvm_arguments))
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1, **kwargs)
        create_ks(session, 'ks', rf)

        if user_table:
            session.execute(
                ("CREATE TABLE users (username varchar, password varchar, gender varchar, "
                 "session_token varchar, state varchar, birth_year bigint, "
                 "PRIMARY KEY (username));")
            )

            # create a materialized view
            session.execute(("CREATE MATERIALIZED VIEW users_by_state AS "
                             "SELECT * FROM users WHERE STATE IS NOT NULL AND username IS NOT NULL "
                             "PRIMARY KEY (state, username)"))

        return session

    def update_view(self, session, query, flush, compact=False):
        session.execute(query)
        # self._replay_batchlogs()
        if flush:
            self.cluster.flush()
        if compact:
            self.cluster.compact()

    def _settle_nodes(self):
        debug("Settling all nodes")
        stage_match = re.compile("(?P<name>\S+)\s+(?P<active>\d+)\s+(?P<pending>\d+)\s+(?P<completed>\d+)\s+(?P<blocked>\d+)\s+(?P<alltimeblocked>\d+)")

        def _settled_stages(node):
            (stdout, stderr, rc) = node.nodetool("tpstats")
            lines = re.split("\n+", stdout)
            for line in lines:
                match = stage_match.match(line)
                if match is not None:
                    active = int(match.group('active'))
                    pending = int(match.group('pending'))
                    if active != 0 or pending != 0:
                        debug("%s - pool %s still has %d active and %d pending" % (node.name, match.group("name"), active, pending))
                        return False
            return True

        for node in self.cluster.nodelist():
            if node.is_running():
                node.nodetool("replaybatchlog")
                attempts = 50  # 100 milliseconds per attempt, so 5 seconds total
                while attempts > 0 and not _settled_stages(node):
                    time.sleep(0.1)
                    attempts -= 1

    def _build_progress_table(self):
        if self.cluster.version() >= '4':
            return 'system.view_builds_in_progress'
        else:
            return 'system.views_builds_in_progress'

    def _wait_for_view(self, ks, view, attempts=50, delay=0):
        debug("waiting for view")

        def _view_build_finished(node):
            s = self.patient_exclusive_cql_connection(node)
            query = "SELECT * FROM %s WHERE keyspace_name='%s' AND view_name='%s'" % \
                    (self._build_progress_table(), ks, view)
            built_views_query = "SELECT view_name FROM %s WHERE keyspace_name='%s' AND view_name='%s'" % \
                                ("system.built_views", ks, view)
            built_views_result = list(s.execute(built_views_query))
            result = list(s.execute(query))
            return len(built_views_result) == 1 and len(result) == 0

        if delay > 0:
            time.sleep(delay)

        total_delay = delay + attempts  # each attempt currently sleeps for one second
        for node in self.cluster.nodelist():
            if node.is_running():
                # 1 sec per attempt, so 50 seconds total by default
                while attempts > 0 and not _view_build_finished(node):
                    time.sleep(1)
                    attempts -= 1
                if attempts <= 0:
                    raise RuntimeError("View {}.{} build not finished after {} seconds.".format(ks, view, total_delay))

    def _wait_for_view_build_start(self, session, ks, view, wait_minutes=2):
        """Wait for the start of a MV build, ensuring that it has saved some progress"""
        start = time.time()
        while True:
            try:
                query = "SELECT COUNT(*) FROM %s WHERE keyspace_name='%s' AND view_name='%s'" % \
                        (self._build_progress_table(), ks, view)
                result = list(session.execute(query))
                self.assertEqual(result[0].count, 0)
            except AssertionError:
                break

            elapsed = (time.time() - start) / 60
            if elapsed > wait_minutes:
                self.fail("The MV build hasn't started in 2 minutes.")

    def _insert_data(self, session):
        # insert data
        insert_stmt = "INSERT INTO users (username, password, gender, state, birth_year) VALUES "
        session.execute(insert_stmt + "('user1', 'ch@ngem3a', 'f', 'TX', 1968);")
        session.execute(insert_stmt + "('user2', 'ch@ngem3b', 'm', 'CA', 1971);")
        session.execute(insert_stmt + "('user3', 'ch@ngem3c', 'f', 'FL', 1978);")
        session.execute(insert_stmt + "('user4', 'ch@ngem3d', 'm', 'TX', 1974);")
        self._settle_nodes()

    def _replay_batchlogs(self):
        for node in self.cluster.nodelist():
            if node.is_running():
                debug("Replaying batchlog on node {}".format(node.name))
                node.nodetool("replaybatchlog")
                # CASSANDRA-13069 - Ensure replayed mutations are removed from the batchlog
                node_session = self.patient_exclusive_cql_connection(node)
                result = list(node_session.execute("SELECT count(*) FROM system.batches;"))
                self.assertEqual(result[0].count, 0)

    def _assert_view_meta(self, session, views, exists=True, nodes=2):
        if exists:
            assert_one(session, "SELECT COUNT(*) FROM system.built_views", [views])
            if self.cluster.version() >= '3.11':
                assert_one(session, "SELECT COUNT(*) FROM system_distributed.view_build_status", [views * nodes])
        else:
            assert_none(session, "SELECT * FROM system.built_views")
            if self.cluster.version() >= '3.11':
                assert_none(session, "SELECT * FROM system_distributed.view_build_status")
        assert_none(session, "SELECT * FROM {}".format(self._build_progress_table()))

    def create_test(self):
        """Test the materialized view creation"""

        session = self.prepare(user_table=True)

        result = list(session.execute(("SELECT * FROM system_schema.views "
                                       "WHERE keyspace_name='ks' AND base_table_name='users' ALLOW FILTERING")))
        self.assertEqual(len(result), 1, "Expecting 1 materialized view, got" + str(result))

    def test_gcgs_validation(self):
        """Verify that it's not possible to create or set a too low gc_grace_seconds on MVs"""
        session = self.prepare(user_table=True)

        # Shouldn't be able to alter the gc_grace_seconds of the base table to 0
        assert_invalid(session,
                       "ALTER TABLE users WITH gc_grace_seconds = 0",
                       "Cannot alter gc_grace_seconds of the base table of a materialized view "
                       "to 0, since this value is used to TTL undelivered updates. Setting "
                       "gc_grace_seconds too low might cause undelivered updates to expire "
                       "before being replayed.")

        # But can alter the gc_grace_seconds of the bease table to a value != 0
        session.execute("ALTER TABLE users WITH gc_grace_seconds = 10")

        # Shouldn't be able to alter the gc_grace_seconds of the MV to 0
        assert_invalid(session,
                       "ALTER MATERIALIZED VIEW users_by_state WITH gc_grace_seconds = 0",
                       "Cannot alter gc_grace_seconds of a materialized view to 0, since "
                       "this value is used to TTL undelivered updates. Setting gc_grace_seconds "
                       "too low might cause undelivered updates to expire before being replayed.")

        # Now let's drop MV
        session.execute("DROP MATERIALIZED VIEW ks.users_by_state;")

        # Now we should be able to set the gc_grace_seconds of the base table to 0
        session.execute("ALTER TABLE users WITH gc_grace_seconds = 0")

        # Now we shouldn't be able to create a new MV on this table
        assert_invalid(session,
                       "CREATE MATERIALIZED VIEW users_by_state AS "
                       "SELECT * FROM users WHERE STATE IS NOT NULL AND username IS NOT NULL "
                       "PRIMARY KEY (state, username)",
                       "Cannot create materialized view 'users_by_state' for base table 'users' "
                       "with gc_grace_seconds of 0, since this value is used to TTL undelivered "
                       "updates. Setting gc_grace_seconds too low might cause undelivered updates"
                       " to expire before being replayed.")

    def insert_test(self):
        """Test basic insertions"""

        session = self.prepare(user_table=True)

        self._insert_data(session)

        result = list(session.execute("SELECT * FROM users;"))
        self.assertEqual(len(result), 4, "Expecting {} users, got {}".format(4, len(result)))

        result = list(session.execute("SELECT * FROM users_by_state WHERE state='TX';"))
        self.assertEqual(len(result), 2, "Expecting {} users, got {}".format(2, len(result)))

        result = list(session.execute("SELECT * FROM users_by_state WHERE state='CA';"))
        self.assertEqual(len(result), 1, "Expecting {} users, got {}".format(1, len(result)))

        result = list(session.execute("SELECT * FROM users_by_state WHERE state='MA';"))
        self.assertEqual(len(result), 0, "Expecting {} users, got {}".format(0, len(result)))

    def populate_mv_after_insert_test(self):
        """Test that a view is OK when created with existing data"""

        session = self.prepare(consistency_level=ConsistencyLevel.QUORUM)

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v) VALUES ({v}, {v})".format(v=i))

        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL "
                         "AND id IS NOT NULL PRIMARY KEY (v, id)"))

        debug("wait for view to build")
        self._wait_for_view("ks", "t_by_v")

        debug("wait that all batchlogs are replayed")
        self._replay_batchlogs()

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t_by_v WHERE v = {}".format(i), [i, i])

    @since('3.0', max_version='3.X')
    def populate_mv_after_insert_wide_rows_test(self):
        """Test that a view is OK when created with existing data with wide rows"""

        session = self.prepare(consistency_level=ConsistencyLevel.QUORUM)

        session.execute("CREATE TABLE t (id int, v int, PRIMARY KEY (id, v))")

        for i in xrange(5):
            for j in xrange(10000):
                session.execute("INSERT INTO t (id, v) VALUES ({}, {})".format(i, j))

        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL "
                         "AND id IS NOT NULL PRIMARY KEY (v, id)"))

        debug("wait for view to build")
        self._wait_for_view("ks", "t_by_v")

        debug("wait that all batchlogs are replayed")
        self._replay_batchlogs()
        for i in xrange(5):
            for j in xrange(10000):
                assert_one(session, "SELECT * FROM t_by_v WHERE id = {} AND v = {}".format(i, j), [j, i])

    def crc_check_chance_test(self):
        """Test that crc_check_chance parameter is properly populated after mv creation and update"""

        session = self.prepare()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL "
                         "AND id IS NOT NULL PRIMARY KEY (v, id) WITH crc_check_chance = 0.5"))

        assert_crc_check_chance_equal(session, "t_by_v", 0.5, view=True)

        session.execute("ALTER MATERIALIZED VIEW t_by_v WITH crc_check_chance = 0.3")

        assert_crc_check_chance_equal(session, "t_by_v", 0.3, view=True)

    def prepared_statement_test(self):
        """Test basic insertions with prepared statement"""

        session = self.prepare(user_table=True)

        insertPrepared = session.prepare(
            "INSERT INTO users (username, password, gender, state, birth_year) VALUES (?, ?, ?, ?, ?);"
        )
        selectPrepared = session.prepare(
            "SELECT state, password, session_token FROM users_by_state WHERE state=?;"
        )

        # insert data
        session.execute(insertPrepared.bind(('user1', 'ch@ngem3a', 'f', 'TX', 1968)))
        session.execute(insertPrepared.bind(('user2', 'ch@ngem3b', 'm', 'CA', 1971)))
        session.execute(insertPrepared.bind(('user3', 'ch@ngem3c', 'f', 'FL', 1978)))
        session.execute(insertPrepared.bind(('user4', 'ch@ngem3d', 'm', 'TX', 1974)))

        result = list(session.execute("SELECT * FROM users;"))
        self.assertEqual(len(result), 4, "Expecting {} users, got {}".format(4, len(result)))

        result = list(session.execute(selectPrepared.bind(['TX'])))
        self.assertEqual(len(result), 2, "Expecting {} users, got {}".format(2, len(result)))

        result = list(session.execute(selectPrepared.bind(['CA'])))
        self.assertEqual(len(result), 1, "Expecting {} users, got {}".format(1, len(result)))

        result = list(session.execute(selectPrepared.bind(['MA'])))
        self.assertEqual(len(result), 0, "Expecting {} users, got {}".format(0, len(result)))

    def immutable_test(self):
        """Test that a materialized view is immutable"""

        session = self.prepare(user_table=True)

        # cannot insert
        assert_invalid(session, "INSERT INTO users_by_state (state, username) VALUES ('TX', 'user1');",
                       "Cannot directly modify a materialized view")

        # cannot update
        assert_invalid(session, "UPDATE users_by_state SET session_token='XYZ' WHERE username='user1' AND state = 'TX';",
                       "Cannot directly modify a materialized view")

        # cannot delete a row
        assert_invalid(session, "DELETE from users_by_state where state='TX';",
                       "Cannot directly modify a materialized view")

        # cannot delete a cell
        assert_invalid(session, "DELETE session_token from users_by_state where state='TX';",
                       "Cannot directly modify a materialized view")

        # cannot alter a table
        assert_invalid(session, "ALTER TABLE users_by_state ADD first_name varchar",
                       "Cannot use ALTER TABLE on Materialized View")

    def drop_mv_test(self):
        """Test that we can drop a view properly"""

        session = self.prepare(user_table=True)

        # create another materialized view
        session.execute(("CREATE MATERIALIZED VIEW users_by_birth_year AS "
                         "SELECT * FROM users WHERE birth_year IS NOT NULL AND "
                         "username IS NOT NULL PRIMARY KEY (birth_year, username)"))

        result = list(session.execute(("SELECT * FROM system_schema.views "
                                       "WHERE keyspace_name='ks' AND base_table_name='users' ALLOW FILTERING")))
        self.assertEqual(len(result), 2, "Expecting {} materialized view, got {}".format(2, len(result)))

        session.execute("DROP MATERIALIZED VIEW ks.users_by_state;")

        result = list(session.execute(("SELECT * FROM system_schema.views "
                                       "WHERE keyspace_name='ks' AND base_table_name='users' ALLOW FILTERING")))
        self.assertEqual(len(result), 1, "Expecting {} materialized view, got {}".format(1, len(result)))

    def drop_column_test(self):
        """Test that we cannot drop a column if it is used by a MV"""

        session = self.prepare(user_table=True)

        result = list(session.execute(("SELECT * FROM system_schema.views "
                                       "WHERE keyspace_name='ks' AND base_table_name='users' ALLOW FILTERING")))
        self.assertEqual(len(result), 1, "Expecting {} materialized view, got {}".format(1, len(result)))

        if self.cluster.version() >= '4' and not self.is_legacy():
            assert_invalid(
                session,
                "ALTER TABLE ks.users DROP state;",
                "Cannot drop column state from base table ks.users because it is required on materialized view ks.users_by_state."
            )
        else:
            assert_invalid(
                session,
                "ALTER TABLE ks.users DROP state;",
                "Cannot drop column state on base table users with materialized views."
            )

    def drop_table_test(self):
        """Test that we cannot drop a table without deleting its MVs first"""

        session = self.prepare(user_table=True)

        result = list(session.execute(("SELECT * FROM system_schema.views "
                                       "WHERE keyspace_name='ks' AND base_table_name='users' ALLOW FILTERING")))
        self.assertEqual(
            len(result), 1,
            "Expecting {} materialized view, got {}".format(1, len(result))
        )

        assert_invalid(
            session,
            "DROP TABLE ks.users;",
            "Cannot drop table when materialized views still depend on it"
        )

        result = list(session.execute(("SELECT * FROM system_schema.views "
                                       "WHERE keyspace_name='ks' AND base_table_name='users' ALLOW FILTERING")))
        self.assertEqual(
            len(result), 1,
            "Expecting {} materialized view, got {}".format(1, len(result))
        )

        session.execute("DROP MATERIALIZED VIEW ks.users_by_state;")
        session.execute("DROP TABLE ks.users;")

        result = list(session.execute(("SELECT * FROM system_schema.views "
                                       "WHERE keyspace_name='ks' AND base_table_name='users' ALLOW FILTERING")))
        self.assertEqual(
            len(result), 0,
            "Expecting {} materialized view, got {}".format(1, len(result))
        )

    def clustering_column_test(self):
        """Test that we can use clustering columns as primary key for a materialized view"""

        session = self.prepare(consistency_level=ConsistencyLevel.QUORUM)

        session.execute(("CREATE TABLE users (username varchar, password varchar, gender varchar, "
                         "session_token varchar, state varchar, birth_year bigint, "
                         "PRIMARY KEY (username, state, birth_year));"))

        # create a materialized view that use a compound key
        session.execute(("CREATE MATERIALIZED VIEW users_by_state_birth_year "
                         "AS SELECT * FROM users WHERE state IS NOT NULL AND birth_year IS NOT NULL "
                         "AND username IS NOT NULL PRIMARY KEY (state, birth_year, username)"))

        session.cluster.control_connection.wait_for_schema_agreement()

        self._insert_data(session)

        result = list(session.execute("SELECT * FROM ks.users_by_state_birth_year WHERE state='TX'"))
        self.assertEqual(len(result), 2, "Expecting {} users, got {}".format(2, len(result)))

        result = list(session.execute("SELECT * FROM ks.users_by_state_birth_year WHERE state='TX' AND birth_year=1968"))
        self.assertEqual(len(result), 1, "Expecting {} users, got {}".format(1, len(result)))

    def _add_dc_after_mv_test(self, rf, alter_ks_add_dc=None):
        """
        @jira_ticket CASSANDRA-10978

        Add datacenter with configurable replication.
        """

        session = self.prepare(rf=rf, nodes=3)

        debug("Creating schema")
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        debug("Writing 1k to base")
        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v) VALUES ({id}, {v})".format(id=i, v=-i))

        debug("Reading 1k from view")
        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t_by_v WHERE v = {}".format(-i), [-i, i])

        debug("Reading 1k from base")
        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t WHERE id = {}".format(i), [i, -i])

        debug("Bootstrapping new node in another dc")
        node4 = new_node(self.cluster, data_center='dc2')
        node4.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=["-Dcassandra.migration_task_wait_in_seconds={}".format(MIGRATION_WAIT)]))

        debug("Bootstrapping new node in another dc")
        node5 = new_node(self.cluster, remote_debug_port='1414', data_center='dc2')
        node5.start(jvm_args=self.wrap_jvm_args(jvm_args=["-Dcassandra.migration_task_wait_in_seconds={}".format(MIGRATION_WAIT)]))

        if alter_ks_add_dc is not None:
            session.execute(alter_ks_add_dc)

            node4.nodetool("rebuild -ks ks -- dc1")
            node5.nodetool("rebuild -ks ks -- dc1")

        session2 = self.patient_exclusive_cql_connection(node4)

        debug("Verifying data from new node in view")
        for i in xrange(1000):
            assert_one(session2, "SELECT * FROM ks.t_by_v WHERE v = {}".format(-i), [-i, i])

        debug("Inserting 100 into base")
        for i in xrange(1000, 1100):
            session.execute("INSERT INTO t (id, v) VALUES ({id}, {v})".format(id=i, v=-i))

        debug("Verify 100 in view")
        for i in xrange(1000, 1100):
            assert_one(session, "SELECT * FROM t_by_v WHERE v = {}".format(-i), [-i, i])

    @attr('resource-intensive')
    def add_dc_after_mv_simple_replication_test(self):
        """
        @jira_ticket CASSANDRA-10634

        Test that materialized views work as expected when adding a datacenter with SimpleStrategy.
        """

        self._add_dc_after_mv_test(1)

    @attr('resource-intensive')
    def add_dc_after_mv_network_replication_test(self):
        """
        @jira_ticket CASSANDRA-10634

        Test that materialized views work as expected when adding a datacenter with NetworkTopologyStrategy.

        Since 4.0 (CASSANDRA-12681), Cassandra won't allow to create non-existing DC before adding nodes in new DC
        """
        alter_ks = "ALTER KEYSPACE ks WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'dc1':1, 'dc2':1};"
        self._add_dc_after_mv_test(rf={'dc1': 1}, alter_ks_add_dc=alter_ks)

    @attr('resource-intensive')
    def add_node_after_mv_test(self):
        """
        @jira_ticket CASSANDRA-10978

        Test that materialized views work as expected when adding a node.
        """

        session = self.prepare()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v) VALUES ({id}, {v})".format(id=i, v=-i))

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t_by_v WHERE v = {}".format(-i), [-i, i])

        node4 = new_node(self.cluster)
        node4.start(wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=["-Dcassandra.migration_task_wait_in_seconds={}".format(MIGRATION_WAIT)]))

        session2 = self.patient_exclusive_cql_connection(node4)

        """
        @jira_ticket CASSANDRA-12984

        Assert that MVs are marked as build after bootstrap. Otherwise newly streamed MVs will be built again
        """
        assert_one(session2, "SELECT count(*) FROM system.built_views WHERE keyspace_name = 'ks' AND view_name = 't_by_v'", [1])

        for i in xrange(1000):
            assert_one(session2, "SELECT * FROM ks.t_by_v WHERE v = {}".format(-i), [-i, i])

        for i in xrange(1000, 1100):
            session.execute("INSERT INTO t (id, v) VALUES ({id}, {v})".format(id=i, v=-i))

        for i in xrange(1000, 1100):
            assert_one(session, "SELECT * FROM t_by_v WHERE v = {}".format(-i), [-i, i])

    @attr('resource-intensive')
    def add_node_after_wide_mv_with_range_deletions_test(self):
        """
        @jira_ticket CASSANDRA-11670

        Test that materialized views work with wide materialized views as expected when adding a node.
        """

        session = self.prepare()

        session.execute("CREATE TABLE t (id int, v int, PRIMARY KEY (id, v)) WITH compaction = { 'class': 'SizeTieredCompactionStrategy', 'enabled': 'false' }")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        for i in xrange(10):
            for j in xrange(100):
                session.execute("INSERT INTO t (id, v) VALUES ({id}, {v})".format(id=i, v=j))

        self.cluster.flush()

        for i in xrange(10):
            for j in xrange(100):
                assert_one(session, "SELECT * FROM t WHERE id = {} and v = {}".format(i, j), [i, j])
                assert_one(session, "SELECT * FROM t_by_v WHERE id = {} and v = {}".format(i, j), [j, i])

        for i in xrange(10):
            for j in xrange(100):
                if j % 10 == 0:
                    session.execute("DELETE FROM t WHERE id = {} AND v >= {} and v < {}".format(i, j, j + 2))

        self.cluster.flush()

        for i in xrange(10):
            for j in xrange(100):
                if j % 10 == 0 or (j - 1) % 10 == 0:
                    assert_none(session, "SELECT * FROM t WHERE id = {} and v = {}".format(i, j))
                    assert_none(session, "SELECT * FROM t_by_v WHERE id = {} and v = {}".format(i, j))
                else:
                    assert_one(session, "SELECT * FROM t WHERE id = {} and v = {}".format(i, j), [i, j])
                    assert_one(session, "SELECT * FROM t_by_v WHERE id = {} and v = {}".format(i, j), [j, i])

        node4 = new_node(self.cluster)
        node4.set_configuration_options(values={'max_mutation_size_in_kb': 20})  # CASSANDRA-11670
        debug("Start join at {}".format(time.strftime("%H:%M:%S")))
        node4.start(wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=["-Dcassandra.migration_task_wait_in_seconds={}".format(MIGRATION_WAIT)]))

        session2 = self.patient_exclusive_cql_connection(node4)

        for i in xrange(10):
            for j in xrange(100):
                if j % 10 == 0 or (j - 1) % 10 == 0:
                    assert_none(session2, "SELECT * FROM ks.t WHERE id = {} and v = {}".format(i, j))
                    assert_none(session2, "SELECT * FROM ks.t_by_v WHERE id = {} and v = {}".format(i, j))
                else:
                    assert_one(session2, "SELECT * FROM ks.t WHERE id = {} and v = {}".format(i, j), [i, j])
                    assert_one(session2, "SELECT * FROM ks.t_by_v WHERE id = {} and v = {}".format(i, j), [j, i])

        for i in xrange(10):
            for j in xrange(100, 110):
                session.execute("INSERT INTO t (id, v) VALUES ({id}, {v})".format(id=i, v=j))

        for i in xrange(10):
            for j in xrange(110):
                if j < 100 and (j % 10 == 0 or (j - 1) % 10 == 0):
                    assert_none(session2, "SELECT * FROM ks.t WHERE id = {} and v = {}".format(i, j))
                    assert_none(session2, "SELECT * FROM ks.t_by_v WHERE id = {} and v = {}".format(i, j))
                else:
                    assert_one(session2, "SELECT * FROM ks.t WHERE id = {} and v = {}".format(i, j), [i, j])
                    assert_one(session2, "SELECT * FROM ks.t_by_v WHERE id = {} and v = {}".format(i, j), [j, i])

    @attr('resource-intensive')
    def add_node_after_very_wide_mv_test(self):
        """
        @jira_ticket CASSANDRA-11670

        Test that materialized views work with very wide materialized views as expected when adding a node.
        """

        session = self.prepare()

        session.execute("CREATE TABLE t (id int, v int, PRIMARY KEY (id, v))")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        for i in xrange(5):
            for j in xrange(5000):
                session.execute("INSERT INTO t (id, v) VALUES ({id}, {v})".format(id=i, v=j))

        self.cluster.flush()

        for i in xrange(5):
            for j in xrange(5000):
                assert_one(session, "SELECT * FROM t_by_v WHERE id = {} and v = {}".format(i, j), [j, i])

        node4 = new_node(self.cluster)
        node4.set_configuration_options(values={'max_mutation_size_in_kb': 20})  # CASSANDRA-11670
        debug("Start join at {}".format(time.strftime("%H:%M:%S")))
        node4.start(wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=["-Dcassandra.migration_task_wait_in_seconds={}".format(MIGRATION_WAIT)]))

        session2 = self.patient_exclusive_cql_connection(node4)

        for i in xrange(5):
            for j in xrange(5000):
                assert_one(session2, "SELECT * FROM ks.t_by_v WHERE id = {} and v = {}".format(i, j), [j, i])

        for i in xrange(5):
            for j in xrange(5100):
                session.execute("INSERT INTO t (id, v) VALUES ({id}, {v})".format(id=i, v=j))

        for i in xrange(5):
            for j in xrange(5100):
                assert_one(session, "SELECT * FROM t_by_v WHERE id = {} and v = {}".format(i, j), [j, i])

    @attr('resource-intensive')
    def add_write_survey_node_after_mv_test(self):
        """
        @jira_ticket CASSANDRA-10621
        @jira_ticket CASSANDRA-10978

        Test that materialized views work as expected when adding a node in write survey mode.
        """

        session = self.prepare()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v) VALUES ({id}, {v})".format(id=i, v=-i))

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t_by_v WHERE v = {}".format(-i), [-i, i])

        node4 = new_node(self.cluster)
        node4.start(wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=["-Dcassandra.write_survey=true", "-Dcassandra.migration_task_wait_in_seconds={}".format(MIGRATION_WAIT)]))

        for i in xrange(1000, 1100):
            session.execute("INSERT INTO t (id, v) VALUES ({id}, {v})".format(id=i, v=-i))

        for i in xrange(1100):
            assert_one(session, "SELECT * FROM t_by_v WHERE v = {}".format(-i), [-i, i])

    def allow_filtering_test(self):
        """Test that allow filtering works as usual for a materialized view"""

        session = self.prepare()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))
        session.execute(("CREATE MATERIALIZED VIEW t_by_v2 AS SELECT * FROM t "
                         "WHERE v2 IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v2, id)"))

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0)".format(v=i))

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t_by_v WHERE v = {v}".format(v=i), [i, i, 'a', 3.0])

        rows = list(session.execute("SELECT * FROM t_by_v2 WHERE v2 = 'a'"))
        self.assertEqual(len(rows), 1000, "Expected 1000 rows but got {}".format(len(rows)))

        assert_invalid(session, "SELECT * FROM t_by_v WHERE v = 1 AND v2 = 'a'")
        assert_invalid(session, "SELECT * FROM t_by_v2 WHERE v2 = 'a' AND v = 1")

        for i in xrange(1000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {} AND v3 = 3.0 ALLOW FILTERING".format(i),
                [i, i, 'a', 3.0]
            )
            assert_one(
                session,
                "SELECT * FROM t_by_v2 WHERE v2 = 'a' AND v = {} ALLOW FILTERING".format(i),
                ['a', i, i, 3.0]
            )

    @since('4.0')
    def timeout_on_mv_acquire_lock_test(self):
        """
        Test that base table update is dropped when there is a timeout during
        view lock acquisition and ViewLockAcquisitionTimeouts metric is accounted.

        @jira_ticket DB-1522
        """

        debug("Creating cluster")
        cluster = self.cluster
        cluster.populate(3)

        node1, node2, _ = cluster.nodelist()

        # Set a very low timeout on node2 so the base table write request will timeout
        node2.set_configuration_options(values={'write_request_timeout_in_ms': 100})

        # Add a delay longer then write_request_timeout_in_ms to cause the view lock acquisition to fail
        delaying_write = delaying_interceptor(500, Verb.WRITES.msg("WRITE"))

        # We'll use JMX to control interceptors, and what requires this apparently
        for node in cluster.nodelist():
            remove_perf_disable_shared_mem(node)

        cluster.start(jvm_args=self.wrap_jvm_args(jvm_args=jvm_args(interceptors=[delaying_write])))

        debug("Creating views")
        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, 'ks', 2)
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))
        session.execute(("CREATE MATERIALIZED VIEW t_by_v2 AS SELECT * FROM t "
                         "WHERE v2 IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v2, id)"))

        intercepted_count = 0
        with delaying_write.enable(node2) as interception:
            debug("Inserting data")
            for i in xrange(10):
                session.execute("INSERT INTO t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0)".format(v=i))

            intercepted_count = interception.intercepted_count()

        debug("Verifying view write lock timed out message was print")
        node2.watch_log_for('Could not acquire view lock in .* milliseconds for table t'
                            ' of keyspace ks.', filename='debug.log')

        debug("Verifying ViewLockAcquisitionTimeouts metric")
        dropped = self._get_view_lock_acquisition_timeouts(node2, "ks", "t")
        debug("ViewLockAcquisitionTimeouts is {}".format(dropped))
        debug("Interception count is {}".format(intercepted_count))
        self.assertEquals(dropped, intercepted_count)

    def _get_view_lock_acquisition_timeouts(self, node, ks, table):
        mbean = make_mbean('metrics', type="Table", keyspace=ks, scope=table, name='ViewLockAcquisitionTimeouts')
        with JolokiaAgent(node) as jmx:
            return jmx.read_attribute(mbean, 'Count')

    def secondary_index_test(self):
        """Test that secondary indexes cannot be created on a materialized view"""

        session = self.prepare()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))
        assert_invalid(session, "CREATE INDEX ON t_by_v (v2)",
                       "Secondary indexes are not supported on materialized views")

    def ttl_test(self):
        """
        Test that TTL works as expected for a materialized view
        @expected_result The TTL is propagated properly between tables.
        """

        session = self.prepare()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 int, v3 int)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v2 AS SELECT * FROM t "
                         "WHERE v2 IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v2, id)"))

        for i in xrange(100):
            session.execute("INSERT INTO t (id, v, v2, v3) VALUES ({v}, {v}, {v}, {v}) USING TTL 10".format(v=i))

        for i in xrange(100):
            assert_one(session, "SELECT * FROM t_by_v2 WHERE v2 = {}".format(i), [i, i, i, i])

        time.sleep(20)

        rows = list(session.execute("SELECT * FROM t_by_v2"))
        self.assertEqual(len(rows), 0, "Expected 0 rows but got {}".format(len(rows)))

    def query_all_new_column_test(self):
        """
        Test that a materialized view created with a 'SELECT *' works as expected when adding a new column
        @expected_result The new column is present in the view.
        """

        session = self.prepare(user_table=True)

        self._insert_data(session)

        assert_one(
            session,
            "SELECT * FROM users_by_state WHERE state = 'TX' AND username = 'user1'",
            ['TX', 'user1', 1968, 'f', 'ch@ngem3a', None]
        )

        session.execute("ALTER TABLE users ADD first_name varchar;")

        results = list(session.execute("SELECT * FROM users_by_state WHERE state = 'TX' AND username = 'user1'"))
        self.assertEqual(len(results), 1)
        self.assertTrue(hasattr(results[0], 'first_name'), 'Column "first_name" not found')
        assert_one(
            session,
            "SELECT * FROM users_by_state WHERE state = 'TX' AND username = 'user1'",
            ['TX', 'user1', 1968, None, 'f', 'ch@ngem3a', None]
        )

    def query_new_column_test(self):
        """
        Test that a materialized view created with 'SELECT <col1, ...>' works as expected when adding a new column
        @expected_result The new column is not present in the view.
        """

        session = self.prepare(user_table=True)

        session.execute(("CREATE MATERIALIZED VIEW users_by_state2 AS SELECT username FROM users "
                         "WHERE STATE IS NOT NULL AND USERNAME IS NOT NULL PRIMARY KEY (state, username)"))

        self._insert_data(session)

        assert_one(
            session,
            "SELECT * FROM users_by_state2 WHERE state = 'TX' AND username = 'user1'",
            ['TX', 'user1']
        )

        session.execute("ALTER TABLE users ADD first_name varchar;")

        results = list(session.execute("SELECT * FROM users_by_state2 WHERE state = 'TX' AND username = 'user1'"))
        self.assertEqual(len(results), 1)
        self.assertFalse(hasattr(results[0], 'first_name'), 'Column "first_name" found in view')
        assert_one(
            session,
            "SELECT * FROM users_by_state2 WHERE state = 'TX' AND username = 'user1'",
            ['TX', 'user1']
        )

    def rename_column_test(self):
        """
        Test that a materialized view created with a 'SELECT *' works as expected when renaming a column
        @expected_result The column is also renamed in the view.
        """

        session = self.prepare(user_table=True)

        self._insert_data(session)

        assert_one(
            session,
            "SELECT * FROM users_by_state WHERE state = 'TX' AND username = 'user1'",
            ['TX', 'user1', 1968, 'f', 'ch@ngem3a', None]
        )

        session.execute("ALTER TABLE users RENAME username TO user")

        results = list(session.execute("SELECT * FROM users_by_state WHERE state = 'TX' AND user = 'user1'"))
        self.assertEqual(len(results), 1)
        self.assertTrue(hasattr(results[0], 'user'), 'Column "user" not found')
        assert_one(
            session,
            "SELECT state, user, birth_year, gender FROM users_by_state WHERE state = 'TX' AND user = 'user1'",
            ['TX', 'user1', 1968, 'f']
        )

    def rename_column_atomicity_test(self):
        """
        Test that column renaming is atomically done between a table and its materialized views
        @jira_ticket CASSANDRA-12952
        """

        session = self.prepare(nodes=1, user_table=True, install_byteman=True)
        node = self.cluster.nodelist()[0]

        self._insert_data(session)

        assert_one(
            session,
            "SELECT * FROM users_by_state WHERE state = 'TX' AND username = 'user1'",
            ['TX', 'user1', 1968, 'f', 'ch@ngem3a', None]
        )

        # Rename a column with an injected byteman rule to kill the node after the first schema update
        self.allow_log_errors = True
        script_version = '4x' if self.cluster.version() >= '4' else '3x'
        node.byteman_submit(['./byteman/merge_schema_failure_{}.btm'.format(script_version)])
        with self.assertRaises(NoHostAvailable):
            session.execute("ALTER TABLE users RENAME username TO user")

        debug('Restarting node')
        node.stop()
        node.start(wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))
        session = self.patient_cql_connection(node, consistency_level=ConsistencyLevel.ONE)

        # Both the table and its view should have the new schema after restart
        assert_one(
            session,
            "SELECT * FROM ks.users WHERE state = 'TX' AND user = 'user1' ALLOW FILTERING",
            ['user1', 1968, 'f', 'ch@ngem3a', None, 'TX']
        )
        assert_one(
            session,
            "SELECT * FROM ks.users_by_state WHERE state = 'TX' AND user = 'user1'",
            ['TX', 'user1', 1968, 'f', 'ch@ngem3a', None]
        )

    def lwt_test(self):
        """Test that lightweight transaction behave properly with a materialized view"""

        session = self.prepare()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        debug("Inserting initial data using IF NOT EXISTS")
        for i in xrange(1000):
            session.execute(
                "INSERT INTO t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0) IF NOT EXISTS".format(v=i)
            )
        self._replay_batchlogs()

        debug("All rows should have been inserted")
        for i in xrange(1000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0]
            )

        debug("Tyring to UpInsert data with a different value using IF NOT EXISTS")
        for i in xrange(1000):
            v = i * 2
            session.execute(
                "INSERT INTO t (id, v, v2, v3) VALUES ({id}, {v}, 'a', 3.0) IF NOT EXISTS".format(id=i, v=v)
            )
        self._replay_batchlogs()

        debug("No rows should have changed")
        for i in xrange(1000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0]
            )

        debug("Update the 10 first rows with a different value")
        for i in xrange(1000):
            v = i + 2000
            session.execute(
                "UPDATE t SET v={v} WHERE id = {id} IF v < 10".format(id=i, v=v)
            )
        self._replay_batchlogs()

        debug("Verify that only the 10 first rows changed.")
        results = list(session.execute("SELECT * FROM t_by_v;"))
        self.assertEqual(len(results), 1000)
        for i in xrange(1000):
            v = i + 2000 if i < 10 else i
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(v),
                [v, i, 'a', 3.0]
            )

        debug("Deleting the first 10 rows")
        for i in xrange(1000):
            v = i + 2000
            session.execute(
                "DELETE FROM t WHERE id = {id} IF v = {v} ".format(id=i, v=v)
            )
        self._replay_batchlogs()

        debug("Verify that only the 10 first rows have been deleted.")
        results = list(session.execute("SELECT * FROM t_by_v;"))
        self.assertEqual(len(results), 990)
        for i in xrange(10, 1000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0]
            )

    def test_interrupt_build_process(self):
        """Test that an interrupted MV build process is resumed as it should"""

        options = {'hinted_handoff_enabled': False}
        if self.cluster.version() >= '4':
            options['concurrent_materialized_view_builders'] = 4

        session = self.prepare(options=options, install_byteman=True)
        node1, node2, node3 = self.cluster.nodelist()

        debug("Avoid premature MV build finalization with byteman")
        for node in self.cluster.nodelist():
            if self.cluster.version() >= '4':
                node.byteman_submit(['./byteman/4.0/skip_view_build_finalization.btm'])
                node.byteman_submit(['./byteman/4.0/skip_view_build_task_finalization.btm'])
            else:
                node.byteman_submit(['./byteman/pre4.0/skip_finish_view_build_status.btm'])
                node.byteman_submit(['./byteman/pre4.0/skip_view_build_update_distributed.btm'])

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")

        debug("Inserting initial data")
        for i in xrange(10000):
            session.execute(
                "INSERT INTO t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0) IF NOT EXISTS".format(v=i)
            )

        debug("Create a MV")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        debug("Wait and ensure the MV build has started. Waiting up to 2 minutes.")
        self._wait_for_view_build_start(session, 'ks', 't_by_v', wait_minutes=2)

        debug("Stop the cluster. Interrupt the MV build process.")
        self.cluster.stop()

        debug("Checking logs to verify that the view build tasks have been created")
        for node in self.cluster.nodelist():
            self.assertTrue(node.grep_log('Starting new view build', filename='debug.log'))
            self.assertFalse(node.grep_log('Resuming view build', filename='debug.log'))
            node.mark_log(filename='debug.log')

        debug("Restart the cluster")
        self.cluster.start(wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))
        session = self.patient_cql_connection(node1)
        session.execute("USE ks")

        debug("MV shouldn't be built yet.")
        self.assertNotEqual(len(list(session.execute("SELECT COUNT(*) FROM t_by_v"))), 10000)

        debug("Wait and ensure the MV build resumed. Waiting up to 2 minutes.")
        self._wait_for_view("ks", "t_by_v")

        debug("Verify all data")
        assert_one(session, "SELECT COUNT(*) FROM t_by_v", [10000])
        for i in xrange(10000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0],
                cl=ConsistencyLevel.ALL
            )

        debug("Checking logs to verify that some view build tasks have been resumed")
        for node in self.cluster.nodelist():
            self.assertTrue(node.grep_log('Resuming view build', filename='debug.log'))

    @since('4.0')
    def test_drop_while_building(self):
        """Test that a parallel MV build is interrupted when the view is removed"""

        session = self.prepare(options={'concurrent_materialized_view_builders': 4}, install_byteman=True)
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")

        debug("Inserting initial data")
        for i in xrange(10000):
            session.execute("INSERT INTO t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0) IF NOT EXISTS".format(v=i))

        debug("Slowing down MV build with byteman")
        for node in self.cluster.nodelist():
            node.byteman_submit(['./byteman/4.0/view_builder_task_sleep.btm'])

        debug("Create a MV")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        debug("Drop the MV while it is still building")
        session.execute("DROP MATERIALIZED VIEW t_by_v")

        debug("Verify that the build has been stopped before its finalization without errors")
        for node in self.cluster.nodelist():
            self.check_logs_for_errors()
            self.assertFalse(node.grep_log('Marking view', filename='debug.log'))
            self.assertTrue(node.grep_log('Stopping current view builder due to schema change', filename='debug.log'))

        debug("Verify that the view has been removed")
        failed = False
        try:
            session.execute("SELECT COUNT(*) FROM t_by_v")
        except InvalidRequest:
            failed = True
        self.assertTrue(failed, "The view shouldn't be queryable")
        self._assert_view_meta(session, views=1, exists=False)

        debug("Create the MV again")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        debug("Verify that the MV has been successfully created")
        # byteman slows down the view build by 50 millis per key and we have 10k keys with 3 nodes at RF=1 and 4
        # parallel runners, therefore 50 x 10,000 = 500,000 / 3x4 = 42,000 millis or 42 seconds
        self._wait_for_view('ks', 't_by_v', delay=42)
        assert_one(session, "SELECT COUNT(*) FROM t_by_v", [10000])

    @since('4.0')
    def test_drop_with_stopped_build(self):
        """Test that MV whose build has been stopped with `nodetool stop` can be dropped"""

        session = self.prepare(options={'concurrent_materialized_view_builders': 4}, install_byteman=True)
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        nodes = self.cluster.nodelist()

        debug("Inserting initial data")
        for i in xrange(10000):
            session.execute("INSERT INTO t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0) IF NOT EXISTS".format(v=i))

        debug("Slowing down MV build with byteman")
        for node in nodes:
            node.byteman_submit(['./byteman/4.0/view_builder_task_sleep.btm'])

        debug("Create a MV")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        debug("Stopping all running view build tasks with nodetool")
        for node in nodes:
            node.watch_log_for('Starting new view build for range', filename='debug.log', timeout=60)
            node.nodetool('stop VIEW_BUILD')

        debug("Checking logs to verify that some view build tasks have been stopped")
        for node in nodes:
            node.watch_log_for('Stopped build for view', filename='debug.log', timeout=60)
            node.watch_log_for('Compaction interrupted: View build', filename='system.log', timeout=60)
            self.check_logs_for_errors()

        debug("Drop the MV while it is still building")
        session.execute("DROP MATERIALIZED VIEW t_by_v")

        debug("Verify that the build has been stopped before its finalization without errors")
        for node in nodes:
            self.check_logs_for_errors()
            self.assertFalse(node.grep_log('Marking view', filename='debug.log'))
            self.assertTrue(node.grep_log('Stopping current view builder due to schema change', filename='debug.log'))

        debug("Verify that the view has been removed")
        failed = False
        try:
            session.execute("SELECT COUNT(*) FROM t_by_v")
        except InvalidRequest:
            failed = True
        self.assertTrue(failed, "The view shouldn't be queryable")

        debug("Create the MV again")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        debug("Verify that the MV has been successfully created")
        # byteman slows down the view build by 50 millis per key and we have 10k keys with 3 nodes at RF=1 and 4
        # parallel runners, therefore 50 x 10,000 = 500,000 / 3x4 = 42,000 millis or 42 seconds
        self._wait_for_view('ks', 't_by_v', delay=42)
        assert_one(session, "SELECT COUNT(*) FROM t_by_v", [10000])

    @since('4.0')
    def test_resume_stopped_build(self):
        """Test that MV builds stopped with `nodetool stop` are resumed after restart"""

        session = self.prepare(options={'concurrent_materialized_view_builders': 4}, install_byteman=True)
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        nodes = self.cluster.nodelist()

        debug("Inserting initial data")
        for i in xrange(10000):
            session.execute("INSERT INTO t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0) IF NOT EXISTS".format(v=i))

        debug("Slowing down MV build with byteman")
        for node in nodes:
            node.byteman_submit(['./byteman/4.0/view_builder_task_sleep.btm'])

        debug("Create a MV")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        debug("Stopping all running view build tasks with nodetool")
        for node in nodes:
            node.watch_log_for('Starting new view build for range', filename='debug.log', timeout=60)
            node.nodetool('stop VIEW_BUILD')

        debug("Checking logs to verify that some view build tasks have been stopped")
        for node in nodes:
            node.watch_log_for('Stopped build for view', filename='debug.log', timeout=60)
            node.watch_log_for('Compaction interrupted: View build', filename='system.log', timeout=60)
            node.watch_log_for('Interrupted build for view', filename='debug.log', timeout=60)
            self.assertFalse(node.grep_log('Marking view', filename='debug.log'))
            self.check_logs_for_errors()

        debug("Check that MV shouldn't be built yet.")
        self.assertNotEqual(len(list(session.execute("SELECT COUNT(*) FROM t_by_v"))), 10000)

        debug("Restart the cluster")
        self.cluster.stop()
        marks = [node.mark_log() for node in nodes]
        self.cluster.start(wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))
        session = self.patient_cql_connection(nodes[0])

        debug("Verify that the MV has been successfully created")
        # byteman slows down the view build by 50 millis per key and we have 10k keys with 3 nodes at RF=1 and 4
        # parallel runners, therefore 50 x 10,000 = 500,000 / 3x4 = 42,000 millis or 42 seconds
        self._wait_for_view('ks', 't_by_v', delay=42)
        assert_one(session, "SELECT COUNT(*) FROM ks.t_by_v", [10000])

        debug("Checking logs to verify that the view build has been resumed and completed after restart")
        for node, mark in zip(nodes, marks):
            self.assertTrue(node.grep_log('Resuming view build', filename='debug.log', from_mark=mark))
            self.assertTrue(node.grep_log('Marking view', filename='debug.log', from_mark=mark))
            self.check_logs_for_errors()

    @since('3.0')
    def test_no_base_column_in_view_pk_complex_timestamp_with_flush(self):
        self._test_no_base_column_in_view_pk_complex_timestamp(flush=True)

    @since('3.0')
    def test_no_base_column_in_view_pk_complex_timestamp_without_flush(self):
        self._test_no_base_column_in_view_pk_complex_timestamp(flush=False)

    def _test_no_base_column_in_view_pk_complex_timestamp(self, flush):
        """
        Able to shadow old view row if all columns in base are removed including unselected
        Able to recreate view row if at least one selected column alive

        @jira_ticket CASSANDRA-11500
        """
        session = self.prepare(rf=3, nodes=3, options={'hinted_handoff_enabled': False}, consistency_level=ConsistencyLevel.QUORUM)
        node1, node2, node3 = self.cluster.nodelist()

        session.execute('USE ks')
        session.execute("CREATE TABLE t (k int, c int, a int, b int, e int, f int, primary key(k, c))")
        session.execute(("CREATE MATERIALIZED VIEW mv AS SELECT k,c,a,b FROM t "
                         "WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, k)"))
        session.cluster.control_connection.wait_for_schema_agreement()

        # update unselected, view row should be alive
        self.update_view(session, "UPDATE t USING TIMESTAMP 1 SET e=1 WHERE k=1 AND c=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, None, None, 1, None])
        assert_one(session, "SELECT * FROM mv", [1, 1, None, None])

        # remove unselected, add selected column, view row should be alive
        self.update_view(session, "UPDATE t USING TIMESTAMP 2 SET e=null, b=1 WHERE k=1 AND c=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, None, 1, None, None])
        assert_one(session, "SELECT * FROM mv", [1, 1, None, 1])

        # remove selected column, view row is removed
        self.update_view(session, "UPDATE t USING TIMESTAMP 2 SET e=null, b=null WHERE k=1 AND c=1;", flush)
        assert_none(session, "SELECT * FROM t")
        assert_none(session, "SELECT * FROM mv")

        # update unselected with ts=3, view row should be alive
        self.update_view(session, "UPDATE t USING TIMESTAMP 3 SET f=1 WHERE k=1 AND c=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, None, None, None, 1])
        assert_one(session, "SELECT * FROM mv", [1, 1, None, None])

        # insert livenesssInfo, view row should be alive
        self.update_view(session, "INSERT INTO t(k,c) VALUES(1,1) USING TIMESTAMP 3", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, None, None, None, 1])
        assert_one(session, "SELECT * FROM mv", [1, 1, None, None])

        # remove unselected, view row should be alive because of base livenessInfo alive
        self.update_view(session, "UPDATE t USING TIMESTAMP 3 SET f=null WHERE k=1 AND c=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, None, None, None, None])
        assert_one(session, "SELECT * FROM mv", [1, 1, None, None])

        # add selected column, view row should be alive
        self.update_view(session, "UPDATE t USING TIMESTAMP 3 SET a=1 WHERE k=1 AND c=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, None, None, None])
        assert_one(session, "SELECT * FROM mv", [1, 1, 1, None])

        # update unselected, view row should be alive
        self.update_view(session, "UPDATE t USING TIMESTAMP 4 SET f=1 WHERE k=1 AND c=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, None, None, 1])
        assert_one(session, "SELECT * FROM mv", [1, 1, 1, None])

        # delete with ts=3, view row should be alive due to unselected@ts4
        self.update_view(session, "DELETE FROM t USING TIMESTAMP 3 WHERE k=1 AND c=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, None, None, None, 1])
        assert_one(session, "SELECT * FROM mv", [1, 1, None, None])

        # remove unselected, view row should be removed
        self.update_view(session, "UPDATE t USING TIMESTAMP 4 SET f=null WHERE k=1 AND c=1;", flush)
        assert_none(session, "SELECT * FROM t")
        assert_none(session, "SELECT * FROM mv")

        # add selected with ts=7, view row is alive
        self.update_view(session, "UPDATE t USING TIMESTAMP 7 SET b=1 WHERE k=1 AND c=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, None, 1, None, None])
        assert_one(session, "SELECT * FROM mv", [1, 1, None, 1])

        # remove selected with ts=7, view row is dead
        self.update_view(session, "UPDATE t USING TIMESTAMP 7 SET b=null WHERE k=1 AND c=1;", flush)
        assert_none(session, "SELECT * FROM t")
        assert_none(session, "SELECT * FROM mv")

        # add selected with ts=5, view row is alive (selected column should not affects each other)
        self.update_view(session, "UPDATE t USING TIMESTAMP 5 SET a=1 WHERE k=1 AND c=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, None, None, None])
        assert_one(session, "SELECT * FROM mv", [1, 1, 1, None])

        # add selected with ttl=5
        self.update_view(session, "UPDATE t USING TTL 15 SET a=1 WHERE k=1 AND c=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, None, None, None])
        assert_one(session, "SELECT * FROM mv", [1, 1, 1, None])

        time.sleep(15)

        # update unselected with ttl=10, view row should be alive
        self.update_view(session, "UPDATE t USING TTL 15 SET f=1 WHERE k=1 AND c=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, None, None, None, 1])
        assert_one(session, "SELECT * FROM mv", [1, 1, None, None])

        time.sleep(15)

        # view row still alive due to base livenessInfo
        assert_none(session, "SELECT * FROM t")
        assert_none(session, "SELECT * FROM mv")

    @since('3.0')
    def test_base_column_in_view_pk_complex_timestamp_with_flush(self):
        self._test_base_column_in_view_pk_complex_timestamp(flush=True)

    @since('3.0')
    def test_base_column_in_view_pk_complex_timestamp_without_flush(self):
        self._test_base_column_in_view_pk_complex_timestamp(flush=False)

    def _test_base_column_in_view_pk_complex_timestamp(self, flush):
        """
        Able to shadow old view row with column ts greater than pk's ts and re-insert the view row
        Able to shadow old view row with column ts smaller than pk's ts and re-insert the view row

        @jira_ticket CASSANDRA-11500
        """
        session = self.prepare(rf=3, nodes=3, options={'hinted_handoff_enabled': False}, consistency_level=ConsistencyLevel.QUORUM)
        node1, node2, node3 = self.cluster.nodelist()

        session.execute('USE ks')
        session.execute("CREATE TABLE t (k int PRIMARY KEY, a int, b int)")
        session.execute(("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t "
                         "WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (k, a)"))
        session.cluster.control_connection.wait_for_schema_agreement()

        # Set initial values TS=1
        self.update_view(session, "INSERT INTO t (k, a, b) VALUES (1, 1, 1) USING TIMESTAMP 1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1])
        assert_one(session, "SELECT * FROM mv", [1, 1, 1])

        # increase b ts to 10
        self.update_view(session, "UPDATE t USING TIMESTAMP 10 SET b = 2 WHERE k = 1;", flush)
        assert_one(session, "SELECT k,a,b,writetime(b) FROM t", [1, 1, 2, 10])
        assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 1, 2, 10])

        # switch entries. shadow a = 1, insert a = 2
        self.update_view(session, "UPDATE t USING TIMESTAMP 2 SET a = 2 WHERE k = 1;", flush)
        assert_one(session, "SELECT k,a,b,writetime(b) FROM t", [1, 2, 2, 10])
        assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 2, 2, 10])

        # switch entries. shadow a = 2, insert a = 1
        self.update_view(session, "UPDATE t USING TIMESTAMP 3 SET a = 1 WHERE k = 1;", flush)
        assert_one(session, "SELECT k,a,b,writetime(b) FROM t", [1, 1, 2, 10])
        assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 1, 2, 10])

        # switch entries. shadow a = 1, insert a = 2
        self.update_view(session, "UPDATE t USING TIMESTAMP 4 SET a = 2 WHERE k = 1;", flush, compact=True)
        assert_one(session, "SELECT k,a,b,writetime(b) FROM t", [1, 2, 2, 10])
        assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 2, 2, 10])

        # able to shadow view row even if base-column in view pk's ts is smaller than row timestamp
        # set row TS = 20, a@6, b@20
        self.update_view(session, "DELETE FROM t USING TIMESTAMP 5 where k = 1;", flush)
        assert_one(session, "SELECT k,a,b,writetime(b) FROM t", [1, None, 2, 10])
        assert_none(session, "SELECT k,a,b,writetime(b) FROM mv")
        self.update_view(session, "INSERT INTO t (k, a, b) VALUES (1, 1, 1) USING TIMESTAMP 6;", flush)
        assert_one(session, "SELECT k,a,b,writetime(b) FROM t", [1, 1, 2, 10])
        assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 1, 2, 10])
        self.update_view(session, "INSERT INTO t (k, b) VALUES (1, 1) USING TIMESTAMP 20;", flush)
        assert_one(session, "SELECT k,a,b,writetime(b) FROM t", [1, 1, 1, 20])
        assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 1, 1, 20])

        # switch entries. shadow a = 1, insert a = 2
        self.update_view(session, "UPDATE t USING TIMESTAMP 7 SET a = 2 WHERE k = 1;", flush)
        assert_one(session, "SELECT k,a,b,writetime(a),writetime(b) FROM t", [1, 2, 1, 7, 20])
        assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 2, 1, 20])

        # switch entries. shadow a = 2, insert a = 1
        self.update_view(session, "UPDATE t USING TIMESTAMP 8 SET a = 1 WHERE k = 1;", flush)
        assert_one(session, "SELECT k,a,b,writetime(a),writetime(b) FROM t", [1, 1, 1, 8, 20])
        assert_one(session, "SELECT k,a,b,writetime(b) FROM mv", [1, 1, 1, 20])

        # create another view row
        self.update_view(session, "INSERT INTO t (k, a, b) VALUES (2, 2, 2);", flush)
        assert_one(session, "SELECT k,a,b FROM t WHERE k = 2", [2, 2, 2])
        assert_one(session, "SELECT k,a,b FROM mv WHERE k = 2", [2, 2, 2])

        # stop node2, node3
        debug('Shutdown node2')
        node2.stop(wait_other_notice=True)
        debug('Shutdown node3')
        node3.stop(wait_other_notice=True)
        # shadow a = 1, create a = 2
        query = SimpleStatement("UPDATE t USING TIMESTAMP 9 SET a = 2 WHERE k = 1", consistency_level=ConsistencyLevel.ONE)
        self.update_view(session, query, flush)
        # shadow (a=2, k=2) after 3 second
        query = SimpleStatement("UPDATE t USING TTL 3 SET a = 2 WHERE k = 2", consistency_level=ConsistencyLevel.ONE)
        self.update_view(session, query, flush)

        debug('Starting node2')
        node2.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))
        debug('Starting node3')
        node3.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))

        # For k = 1 & a = 1, We should get a digest mismatch of tombstones and repaired
        query = SimpleStatement("SELECT * FROM mv WHERE k = 1 AND a = 1", consistency_level=ConsistencyLevel.ALL)
        result = session.execute(query, trace=True)
        self.check_trace_events(result.get_query_trace(), True)
        self.assertEqual(0, len(result.current_rows))

        # For k = 1 & a = 1, second time no digest mismatch
        result = session.execute(query, trace=True)
        self.check_trace_events(result.get_query_trace(), False)
        assert_none(session, "SELECT * FROM mv WHERE k = 1 AND a = 1")
        self.assertEqual(0, len(result.current_rows))

        # For k = 1 & a = 2, We should get a digest mismatch of data and repaired for a = 2
        query = SimpleStatement("SELECT * FROM mv WHERE k = 1 AND a = 2", consistency_level=ConsistencyLevel.ALL)
        result = session.execute(query, trace=True)
        self.check_trace_events(result.get_query_trace(), True)
        self.assertEqual(1, len(result.current_rows))

        # For k = 1 & a = 2, second time no digest mismatch
        result = session.execute(query, trace=True)
        self.check_trace_events(result.get_query_trace(), False)
        self.assertEqual(1, len(result.current_rows))
        assert_one(session, "SELECT k,a,b,writetime(b) FROM mv WHERE k = 1", [1, 2, 1, 20])

        time.sleep(3)
        # For k = 2 & a = 2, We should get a digest mismatch of expired and repaired
        query = SimpleStatement("SELECT * FROM mv WHERE k = 2 AND a = 2", consistency_level=ConsistencyLevel.ALL)
        result = session.execute(query, trace=True)
        self.check_trace_events(result.get_query_trace(), True)
        debug(result.current_rows)
        self.assertEqual(0, len(result.current_rows))

        # For k = 2 & a = 2, second time no digest mismatch
        result = session.execute(query, trace=True)
        self.check_trace_events(result.get_query_trace(), False)
        self.assertEqual(0, len(result.current_rows))

    @since('3.0')
    def test_expired_liveness_with_limit_rf1_nodes1(self):
        self._test_expired_liveness_with_limit(rf=1, nodes=1)

    @since('3.0')
    def test_expired_liveness_with_limit_rf1_nodes3(self):
        self._test_expired_liveness_with_limit(rf=1, nodes=3)

    @since('3.0')
    def test_expired_liveness_with_limit_rf3(self):
        self._test_expired_liveness_with_limit(rf=3, nodes=3)

    def _test_expired_liveness_with_limit(self, rf, nodes):
        """
        Test MV with expired liveness limit is properly handled

        @jira_ticket CASSANDRA-13883
        """
        session = self.prepare(rf=rf, nodes=nodes, options={'hinted_handoff_enabled': False}, consistency_level=ConsistencyLevel.QUORUM)
        node1 = self.cluster.nodelist()[0]

        session.execute('USE ks')
        session.execute("CREATE TABLE t (k int PRIMARY KEY, a int, b int)")
        session.execute(("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t "
                         "WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (k, a)"))
        session.cluster.control_connection.wait_for_schema_agreement()

        for k in xrange(100):
            session.execute("INSERT INTO t (k, a, b) VALUES ({}, {}, {})".format(k, k, k))

        # generate view row with expired liveness except for row 50 and 99
        for k in xrange(100):
            if k == 50 or k == 99:
                continue
            session.execute("DELETE a FROM t where k = {};".format(k))

        # there should be 2 live data
        assert_one(session, "SELECT k,a,b FROM mv limit 1", [50, 50, 50])
        assert_all(session, "SELECT k,a,b FROM mv limit 2", [[50, 50, 50], [99, 99, 99]])
        assert_all(session, "SELECT k,a,b FROM mv", [[50, 50, 50], [99, 99, 99]])

        # verify IN
        keys = xrange(100)
        assert_one(session, "SELECT k,a,b FROM mv WHERE k in ({}) limit 1".format(', '.join(str(x) for x in keys)),
                   [50, 50, 50])
        assert_all(session, "SELECT k,a,b FROM mv WHERE k in ({}) limit 2".format(', '.join(str(x) for x in keys)),
                   [[50, 50, 50], [99, 99, 99]])
        assert_all(session, "SELECT k,a,b FROM mv WHERE k in ({})".format(', '.join(str(x) for x in keys)),
                   [[50, 50, 50], [99, 99, 99]])

        # verify fetch size
        session.default_fetch_size = 1
        assert_one(session, "SELECT k,a,b FROM mv limit 1", [50, 50, 50])
        assert_all(session, "SELECT k,a,b FROM mv limit 2", [[50, 50, 50], [99, 99, 99]])
        assert_all(session, "SELECT k,a,b FROM mv", [[50, 50, 50], [99, 99, 99]])

    @since('3.0')
    def test_base_column_in_view_pk_commutative_tombstone_with_flush(self):
        self._test_base_column_in_view_pk_commutative_tombstone_(flush=True)

    @since('3.0')
    def test_base_column_in_view_pk_commutative_tombstone_without_flush(self):
        self._test_base_column_in_view_pk_commutative_tombstone_(flush=False)

    def _test_base_column_in_view_pk_commutative_tombstone_(self, flush):
        """
        view row deletion should be commutative with newer view livenessInfo, otherwise deleted columns may be resurrected.
        @jira_ticket CASSANDRA-13409
        """
        session = self.prepare(rf=3, nodes=3, options={'hinted_handoff_enabled': False}, consistency_level=ConsistencyLevel.QUORUM)
        node1 = self.cluster.nodelist()[0]

        session.execute('USE ks')
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v,id)"))
        session.cluster.control_connection.wait_for_schema_agreement()
        for node in self.cluster.nodelist():
            node.nodetool("disableautocompaction")

        # sstable 1, Set initial values TS=1
        self.update_view(session, "INSERT INTO t (id, v, v2, v3) VALUES (1, 1, 'a', 3.0) USING TIMESTAMP 1", flush)
        assert_one(session, "SELECT * FROM t_by_v", [1, 1, 'a', 3.0])

        # sstable 2, change v's value and TS=2, tombstones v=1 and adds v=0 record
        self.update_view(session, "DELETE FROM t USING TIMESTAMP 2 WHERE id = 1;", flush)
        assert_none(session, "SELECT * FROM t_by_v")
        assert_none(session, "SELECT * FROM t")

        # sstable 3, tombstones of mv created by base deletion should remain.
        self.update_view(session, "INSERT INTO t (id, v) VALUES (1, 1) USING TIMESTAMP 3", flush)
        assert_one(session, "SELECT * FROM t_by_v", [1, 1, None, None])
        assert_one(session, "SELECT * FROM t", [1, 1, None, None])

        # sstable 4, shadow view row (id=1, v=1), insert (id=1, v=2, ts=4)
        self.update_view(session, "UPDATE t USING TIMESTAMP 4 set v = 2 WHERE id = 1;", flush)
        assert_one(session, "SELECT * FROM t_by_v", [2, 1, None, None])
        assert_one(session, "SELECT * FROM t", [1, 2, None, None])

        # sstable 5, shadow view row (id=1, v=2), insert (id=1, v=1 ts=5)
        self.update_view(session, "UPDATE t USING TIMESTAMP 5 set v = 1 WHERE id = 1;", flush)
        assert_one(session, "SELECT * FROM t_by_v", [1, 1, None, None])
        assert_one(session, "SELECT * FROM t", [1, 1, None, None])  # data deleted by row-tombstone@2 should not resurrect

        if flush:
            self.cluster.compact()
            assert_one(session, "SELECT * FROM t_by_v", [1, 1, None, None])
            assert_one(session, "SELECT * FROM t", [1, 1, None, None])  # data deleted by row-tombstone@2 should not resurrect

        # shadow view row (id=1, v=1)
        self.update_view(session, "UPDATE t USING TIMESTAMP 5 set v = null WHERE id = 1;", flush)
        assert_none(session, "SELECT * FROM t_by_v")
        assert_one(session, "SELECT * FROM t", [1, None, None, None])

    def view_tombstone_test(self):
        """
        Test that a materialized views properly tombstone

        @jira_ticket CASSANDRA-10261
        @jira_ticket CASSANDRA-10910
        """

        self.prepare(rf=3, options={'hinted_handoff_enabled': False})
        node1, node2, node3 = self.cluster.nodelist()

        session = self.patient_exclusive_cql_connection(node1)
        session.max_trace_wait = 120
        session.execute('USE ks')

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v,id)"))

        session.cluster.control_connection.wait_for_schema_agreement()

        # Set initial values TS=0, verify
        session.execute(SimpleStatement("INSERT INTO t (id, v, v2, v3) VALUES (1, 1, 'a', 3.0) USING TIMESTAMP 0",
                                        consistency_level=ConsistencyLevel.ALL))
        self._replay_batchlogs()
        assert_one(
            session,
            "SELECT * FROM t_by_v WHERE v = 1",
            [1, 1, 'a', 3.0]
        )

        session.execute(SimpleStatement("INSERT INTO t (id, v2) VALUES (1, 'b') USING TIMESTAMP 1",
                                        consistency_level=ConsistencyLevel.ALL))
        self._replay_batchlogs()

        assert_one(
            session,
            "SELECT * FROM t_by_v WHERE v = 1",
            [1, 1, 'b', 3.0]
        )

        # change v's value and TS=3, tombstones v=1 and adds v=0 record
        session.execute(SimpleStatement("UPDATE t USING TIMESTAMP 3 SET v = 0 WHERE id = 1",
                                        consistency_level=ConsistencyLevel.ALL))
        self._replay_batchlogs()

        assert_none(session, "SELECT * FROM t_by_v WHERE v = 1")

        debug('Shutdown node2')
        node2.stop(wait_other_notice=True)

        session.execute(SimpleStatement("UPDATE t USING TIMESTAMP 4 SET v = 1 WHERE id = 1",
                                        consistency_level=ConsistencyLevel.QUORUM))
        self._replay_batchlogs()

        assert_one(
            session,
            "SELECT * FROM t_by_v WHERE v = 1",
            [1, 1, 'b', 3.0]
        )

        node2.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))

        # We should get a digest mismatch
        query = SimpleStatement("SELECT * FROM t_by_v WHERE v = 1",
                                consistency_level=ConsistencyLevel.ALL)

        result = session.execute(query, trace=True)
        self.check_trace_events(result.get_query_trace(), True)

        # We should not get a digest mismatch the second time
        query = SimpleStatement("SELECT * FROM t_by_v WHERE v = 1", consistency_level=ConsistencyLevel.ALL)

        result = session.execute(query, trace=True)
        self.check_trace_events(result.get_query_trace(), False)

        # Verify values one last time
        assert_one(
            session,
            "SELECT * FROM t_by_v WHERE v = 1",
            [1, 1, 'b', 3.0],
            cl=ConsistencyLevel.ALL
        )

    def check_trace_events(self, trace, expect_digest):
        # we should see multiple requests get enqueued prior to index scan
        # execution happening

        # Look for messages like:
        #  4.0+        Digest mismatch: Mismatch for key DecoratedKey
        # <4.0         Digest mismatch: org.apache.cassandra.service.DigestMismatchException: Mismatch for key DecoratedKey
        regex = r"Digest mismatch: ([a-zA-Z.]+:\s)?Mismatch for key DecoratedKey"
        for event in trace.events:
            desc = event.description
            match = re.match(regex, desc)
            if match:
                if expect_digest:
                    break
                else:
                    self.fail("Encountered digest mismatch when we shouldn't")
        else:
            if expect_digest:
                self.fail("Didn't find digest mismatch")

    def simple_repair_test_by_base(self):
        self._simple_repair_test(repair_base=True)

    def simple_repair_test_by_view(self):
        self._simple_repair_test(repair_view=True)

    def _simple_repair_test(self, repair_base=False, repair_view=False):
        """
        Test that a materialized view are consistent after a simple repair.
        """

        session = self.prepare(rf=3, options={'hinted_handoff_enabled': False})
        node1, node2, node3 = self.cluster.nodelist()

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        session.cluster.control_connection.wait_for_schema_agreement()

        debug('Shutdown node2')
        node2.stop(wait_other_notice=True)

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0)".format(v=i))

        self._replay_batchlogs()

        debug('Verify the data in the MV with CL=ONE')
        for i in xrange(1000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0]
            )

        debug('Verify the data in the MV with CL=ALL. All should be unavailable.')
        for i in xrange(1000):
            statement = SimpleStatement(
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                consistency_level=ConsistencyLevel.ALL
            )

            assert_unavailable(
                session.execute,
                statement
            )

        debug('Start node2, and repair')
        node2.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))
        if repair_base:
            node1.nodetool("repair ks t")
        if repair_view:
            node1.nodetool("repair ks t_by_v")

        debug('Verify the data in the MV with CL=ALL. All should be available now and no digest mismatch')
        for i in xrange(1000):
            query = SimpleStatement(
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                consistency_level=ConsistencyLevel.ALL
            )
            result = session.execute(query, trace=True)
            self.check_trace_events(result.get_query_trace(), False)
            self.assertEquals(self._rows_to_list(result.current_rows), [[i, i, 'a', 3.0]])

    def base_replica_repair_test(self):
        self._base_replica_repair_test()

    @since('3.0', max_version='3.x')
    def base_replica_repair_with_contention_test(self):
        """
        Test repair does not fail when there is MV lock contention
        @jira_ticket CASSANDRA-12905
        """
        self._base_replica_repair_test(fail_mv_lock=True)

    def _base_replica_repair_test(self, fail_mv_lock=False):
        """
        Test that a materialized view are consistent after the repair of the base replica.
        """

        self.prepare(rf=3)
        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_exclusive_cql_connection(node1)
        session.execute('USE ks')

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        session.cluster.control_connection.wait_for_schema_agreement()

        debug('Write initial data')
        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0)".format(v=i))

        self._replay_batchlogs()

        debug('Verify the data in the MV with CL=ALL')
        for i in xrange(1000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0],
                cl=ConsistencyLevel.ALL
            )

        debug('Shutdown node1')
        node1.stop(wait_other_notice=True)
        debug('Delete node1 data')
        node1.clear(clear_all=True)

        jvm_args = []
        if fail_mv_lock:
            jvm_args = ['-Dcassandra.allow_unsafe_replace=true', '-Dcassandra.replace_address={}'.format(node1.address()), '-Dcassandra.test.fail_mv_locks_count=1000']
            # this should not make Keyspace.apply throw WTE on failure to acquire lock
            node1.set_configuration_options(values={'write_request_timeout_in_ms': 100})

        debug('Restarting node1 with jvm_args={}'.format(jvm_args))
        node1.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=jvm_args))

        debug('Shutdown node2 and node3')
        node2.stop(wait_other_notice=True)
        node3.stop(wait_other_notice=True)

        session = self.patient_exclusive_cql_connection(node1)
        session.execute('USE ks')

        debug('Verify that there is no data on node1')
        for i in xrange(1000):
            assert_none(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i)
            )

        debug('Restarting node2 and node3')
        node2.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))
        node3.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))

        # Just repair the base replica
        debug('Starting repair on node1')
        node1.nodetool("repair ks t")

        debug('Verify data with cl=ALL')
        for i in xrange(1000):
            assert_one(
                session,
                "SELECT * FROM t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0]
            )

    @attr("resource-intensive")
    def complex_repair_test(self):
        """
        Test that a materialized view are consistent after a more complex repair.
        """

        session = self.prepare(rf=5, options={'hinted_handoff_enabled': False}, nodes=5)
        node1, node2, node3, node4, node5 = self.cluster.nodelist()

        # we create the base table with gc_grace_seconds=5 so batchlog will expire after 5 seconds
        session.execute("CREATE TABLE ks.t (id int PRIMARY KEY, v int, v2 text, v3 decimal)"
                        "WITH gc_grace_seconds = 5")
        session.execute(("CREATE MATERIALIZED VIEW ks.t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        session.cluster.control_connection.wait_for_schema_agreement()

        debug('Shutdown node2 and node3')
        node2.stop()
        node3.stop(wait_other_notice=True)

        debug('Write initial data to node1 (will be replicated to node4 and node5)')
        for i in xrange(1000):
            session.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0)".format(v=i))

        debug('Verify the data in the MV on node1 with CL=ONE')
        for i in xrange(1000):
            assert_one(
                session,
                "SELECT * FROM ks.t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0]
            )

        debug('Close connection to node1')
        session.cluster.shutdown()
        debug('Shutdown node1, node4 and node5')
        node1.stop()
        node4.stop()
        node5.stop()

        debug('Start nodes 2 and 3')
        node2.start(self.wrap_jvm_args(jvm_args=[]))
        node3.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))

        session2 = self.patient_cql_connection(node2)

        debug('Verify the data in the MV on node2 with CL=ONE. No rows should be found.')
        for i in xrange(1000):
            assert_none(
                session2,
                "SELECT * FROM ks.t_by_v WHERE v = {}".format(i)
            )

        debug('Write new data in node2 and node3 that overlap those in node1, node4 and node5')
        for i in xrange(1000):
            # we write i*2 as value, instead of i
            session2.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0)".format(v=i * 2))

        debug('Verify the new data in the MV on node2 with CL=ONE')
        for i in xrange(1000):
            v = i * 2
            assert_one(
                session2,
                "SELECT * FROM ks.t_by_v WHERE v = {}".format(v),
                [v, v, 'a', 3.0]
            )

        debug('Wait for batchlogs to expire from node2 and node3')
        time.sleep(5)

        debug('Start remaining nodes')
        node1.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))
        node4.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))
        node5.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))

        session = self.patient_cql_connection(node1)

        debug('Read data from MV at QUORUM (old data should be returned)')
        for i in xrange(1000):
            assert_one(
                session,
                "SELECT * FROM ks.t_by_v WHERE v = {}".format(i),
                [i, i, 'a', 3.0],
                cl=ConsistencyLevel.QUORUM
            )

        debug('Run global repair on node1')
        node1.repair()

        debug('Read data from MV at quorum (new data should be returned after repair)')
        for i in xrange(1000):
            v = i * 2
            assert_one(
                session,
                "SELECT * FROM ks.t_by_v WHERE v = {}".format(v),
                [v, v, 'a', 3.0],
                cl=ConsistencyLevel.QUORUM
            )

    @attr('resource-intensive')
    def throttled_partition_update_test(self):
        """
        @jira_ticket: CASSANDRA-13299, test break up large partition when repairing base with mv.

        Provide a configuable batch size(cassandra.mv.mutation.row.count=100) to trottle number
        of rows to be applied in one mutation
        """

        session = self.prepare(rf=5, options={'hinted_handoff_enabled': False}, nodes=5)
        node1, node2, node3, node4, node5 = self.cluster.nodelist()

        for node in self.cluster.nodelist():
            node.nodetool("disableautocompaction")

        session.execute("CREATE TABLE ks.t (pk int, ck1 int, ck2 int, v1 int, v2 int, PRIMARY KEY(pk, ck1, ck2))")
        session.execute(("CREATE MATERIALIZED VIEW ks.t_by_v AS SELECT * FROM t "
                         "WHERE pk IS NOT NULL AND ck1 IS NOT NULL AND ck2 IS NOT NULL "
                         "PRIMARY KEY (pk, ck2, ck1)"))

        session.cluster.control_connection.wait_for_schema_agreement()

        debug('Shutdown node2 and node3')
        node2.stop(wait_other_notice=True)
        node3.stop(wait_other_notice=True)

        size = 50
        range_deletion_ts = 30
        partition_deletion_ts = 10

        for ck1 in xrange(size):
            for ck2 in xrange(size):
                session.execute("INSERT INTO ks.t (pk, ck1, ck2, v1, v2)"
                                " VALUES (1, {}, {}, {}, {}) USING TIMESTAMP {}".format(ck1, ck2, ck1, ck2, ck1))

        self._replay_batchlogs()

        for ck1 in xrange(size):
            for ck2 in xrange(size):
                assert_one(session, "SELECT pk,ck1,ck2,v1,v2 FROM ks.t WHERE pk=1 AND ck1={} AND ck2={}".format(ck1, ck2),
                           [1, ck1, ck2, ck1, ck2])
                assert_one(session, "SELECT pk,ck1,ck2,v1,v2 FROM ks.t_by_v WHERE pk=1 AND ck1={} AND ck2={}".format(ck1, ck2),
                           [1, ck1, ck2, ck1, ck2])

        debug('Shutdown node4 and node5')
        node4.stop(wait_other_notice=True)
        node5.stop(wait_other_notice=True)

        for ck1 in xrange(size):
            for ck2 in xrange(size):
                if ck1 % 2 == 0:  # range tombstone
                    session.execute("DELETE FROM ks.t USING TIMESTAMP 50 WHERE pk=1 AND ck1={}".format(ck1))
                elif ck1 == ck2:  # row tombstone
                    session.execute("DELETE FROM ks.t USING TIMESTAMP 60 WHERE pk=1 AND ck1={} AND ck2={}".format(ck1, ck2))
                elif ck1 == ck2 - 1:  # cell tombstone
                    session.execute("DELETE v2 FROM ks.t USING TIMESTAMP 70 WHERE pk=1 AND ck1={} AND ck2={}".format(ck1, ck2))

        # range deletion
        session.execute("DELETE FROM ks.t USING TIMESTAMP {} WHERE pk=1 and ck1 < 30 and ck1 > 20".format(range_deletion_ts))
        session.execute("DELETE FROM ks.t USING TIMESTAMP {} WHERE pk=1 and ck1 = 20 and ck2 < 10".format(range_deletion_ts))

        # partition deletion for ck1 <= partition_deletion_ts
        session.execute("DELETE FROM ks.t USING TIMESTAMP {} WHERE pk=1".format(partition_deletion_ts))
        self._replay_batchlogs()

        # start nodes with different batch size
        debug('Starting nodes')
        node2.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=["-Dcassandra.repair.mutation_repair_rows_per_batch={}".format(2)]))
        node3.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=["-Dcassandra.repair.mutation_repair_rows_per_batch={}".format(5)]))
        node4.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=["-Dcassandra.repair.mutation_repair_rows_per_batch={}".format(50)]))
        node5.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=["-Dcassandra.repair.mutation_repair_rows_per_batch={}".format(5000)]))
        self._replay_batchlogs()

        debug('repairing base table')
        node1.nodetool("repair ks t")
        self._replay_batchlogs()

        debug('stop cluster')
        self.cluster.stop()

        debug('rolling restart to check repaired data on each node')
        for node in self.cluster.nodelist():
            debug('starting {}'.format(node.name))
            node.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))
            session = self.patient_cql_connection(node, consistency_level=ConsistencyLevel.ONE)
            for ck1 in xrange(size):
                for ck2 in xrange(size):
                    if (
                        ck1 <= partition_deletion_ts or  # partition deletion
                        ck1 == ck2 or ck1 % 2 == 0 or  # row deletion or range tombstone
                        (ck1 > 20 and ck1 < 30) or (ck1 == 20 and ck2 < 10)  # range tombstone
                    ):
                        assert_none(session, "SELECT pk,ck1,ck2,v1,v2 FROM ks.t_by_v WHERE pk=1 AND "
                                             "ck1={} AND ck2={}".format(ck1, ck2))
                        assert_none(session, "SELECT pk,ck1,ck2,v1,v2 FROM ks.t WHERE pk=1 AND "
                                             "ck1={} AND ck2={}".format(ck1, ck2))
                    elif ck1 == ck2 - 1:  # cell tombstone
                        assert_one(session, "SELECT pk,ck1,ck2,v1,v2 FROM ks.t_by_v WHERE pk=1 AND "
                                            "ck1={} AND ck2={}".format(ck1, ck2), [1, ck1, ck2, ck1, None])
                        assert_one(session, "SELECT pk,ck1,ck2,v1,v2 FROM ks.t WHERE pk=1 AND "
                                            "ck1={} AND ck2={}".format(ck1, ck2), [1, ck1, ck2, ck1, None])
                    else:
                        assert_one(session, "SELECT pk,ck1,ck2,v1,v2 FROM ks.t_by_v WHERE pk=1 AND "
                                            "ck1={} AND ck2={}".format(ck1, ck2), [1, ck1, ck2, ck1, ck2])
                        assert_one(session, "SELECT pk,ck1,ck2,v1,v2 FROM ks.t WHERE pk=1 AND "
                                            "ck1={} AND ck2={}".format(ck1, ck2), [1, ck1, ck2, ck1, ck2])
            debug('stopping {}'.format(node.name))
            node.stop(wait_other_notice=True, wait_for_binary_proto=True)

    @attr('resource-intensive')
    def really_complex_repair_test(self):
        """
        Test that a materialized view are consistent after a more complex repair.
        """

        session = self.prepare(rf=5, options={'hinted_handoff_enabled': False}, nodes=5)
        node1, node2, node3, node4, node5 = self.cluster.nodelist()

        # we create the base table with gc_grace_seconds=5 so batchlog will expire after 5 seconds
        session.execute("CREATE TABLE ks.t (id int, v int, v2 text, v3 decimal, PRIMARY KEY(id, v, v2))"
                        "WITH gc_grace_seconds = 1")
        session.execute(("CREATE MATERIALIZED VIEW ks.t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL AND v IS NOT NULL AND "
                         "v2 IS NOT NULL PRIMARY KEY (v2, v, id)"))

        session.cluster.control_connection.wait_for_schema_agreement()

        debug('Shutdown node2 and node3')
        node2.stop(wait_other_notice=True)
        node3.stop(wait_other_notice=True)

        session.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (1, 1, 'a', 3.0)")
        session.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (2, 2, 'a', 3.0)")
        self._replay_batchlogs()
        debug('Verify the data in the MV on node1 with CL=ONE')
        assert_all(session, "SELECT * FROM ks.t_by_v WHERE v2 = 'a'", [['a', 1, 1, 3.0], ['a', 2, 2, 3.0]])

        session.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (1, 1, 'b', 3.0)")
        session.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (2, 2, 'b', 3.0)")
        self._replay_batchlogs()
        debug('Verify the data in the MV on node1 with CL=ONE')
        assert_all(session, "SELECT * FROM ks.t_by_v WHERE v2 = 'b'", [['b', 1, 1, 3.0], ['b', 2, 2, 3.0]])

        session.shutdown()

        debug('Shutdown node1, node4 and node5')
        node1.stop()
        node4.stop()
        node5.stop()

        debug('Start nodes 2 and 3')
        node2.start(jvm_args=self.wrap_jvm_args(jvm_args=[]))
        node3.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))

        session2 = self.patient_cql_connection(node2)
        session2.execute('USE ks')

        debug('Verify the data in the MV on node2 with CL=ONE. No rows should be found.')
        assert_none(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'a'")

        debug('Write new data in node2 that overlap those in node1')
        session2.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (1, 1, 'c', 3.0)")
        session2.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (2, 2, 'c', 3.0)")
        self._replay_batchlogs()
        assert_all(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'c'", [['c', 1, 1, 3.0], ['c', 2, 2, 3.0]])

        session2.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (1, 1, 'd', 3.0)")
        session2.execute("INSERT INTO ks.t (id, v, v2, v3) VALUES (2, 2, 'd', 3.0)")
        self._replay_batchlogs()
        assert_all(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'd'", [['d', 1, 1, 3.0], ['d', 2, 2, 3.0]])

        debug("Composite delete of everything")
        session2.execute("DELETE FROM ks.t WHERE id = 1 and v = 1")
        session2.execute("DELETE FROM ks.t WHERE id = 2 and v = 2")
        self._replay_batchlogs()
        assert_none(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'c'")
        assert_none(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'd'")

        debug('Wait for batchlogs to expire from node2 and node3')
        time.sleep(5)

        debug('Start remaining nodes')
        node1.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))
        node4.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))
        node5.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))

        # at this point the data isn't repaired so we have an inconsistency.
        # this value should return None
        assert_all(
            session2,
            "SELECT * FROM ks.t_by_v WHERE v2 = 'a'", [['a', 1, 1, 3.0], ['a', 2, 2, 3.0]],
            cl=ConsistencyLevel.QUORUM
        )

        debug('Run global repair on node1')
        node1.repair()

        assert_none(session2, "SELECT * FROM ks.t_by_v WHERE v2 = 'a'", cl=ConsistencyLevel.QUORUM)

    def complex_mv_select_statements_test(self):
        """
        Test complex MV select statements
        @jira_ticket CASSANDRA-9664
        """

        cluster = self.cluster
        cluster.populate(3).start(jmv_args=self.wrap_jvm_args(jvm_args=[]))
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1, consistency_level=ConsistencyLevel.QUORUM)

        debug("Creating keyspace")
        session.execute("CREATE KEYSPACE mvtest WITH replication = "
                        "{'class': 'SimpleStrategy', 'replication_factor': '3'}")
        session.execute('USE mvtest')

        mv_primary_keys = ["((a, b), c)",
                           "((b, a), c)",
                           "(a, b, c)",
                           "(c, b, a)",
                           "((c, a), b)"]

        for mv_primary_key in mv_primary_keys:

            session.execute("CREATE TABLE test (a int, b int, c int, d int, PRIMARY KEY (a, b, c))")

            insert_stmt = session.prepare("INSERT INTO test (a, b, c, d) VALUES (?, ?, ?, ?)")
            update_stmt = session.prepare("UPDATE test SET d = ? WHERE a = ? AND b = ? AND c = ?")
            delete_stmt1 = session.prepare("DELETE FROM test WHERE a = ? AND b = ? AND c = ?")
            delete_stmt2 = session.prepare("DELETE FROM test WHERE a = ?")

            session.cluster.control_connection.wait_for_schema_agreement()

            rows = [(0, 0, 0, 0),
                    (0, 0, 1, 0),
                    (0, 1, 0, 0),
                    (0, 1, 1, 0),
                    (1, 0, 0, 0),
                    (1, 0, 1, 0),
                    (1, 1, -1, 0),
                    (1, 1, 0, 0),
                    (1, 1, 1, 0)]

            for row in rows:
                session.execute(insert_stmt, row)

            debug("Testing MV primary key: {}".format(mv_primary_key))

            session.execute("CREATE MATERIALIZED VIEW mv AS SELECT * FROM test WHERE "
                            "a = 1 AND b IS NOT NULL AND c = 1 PRIMARY KEY {}".format(mv_primary_key))
            time.sleep(3)

            assert_all(
                session, "SELECT a, b, c, d FROM mv",
                [[1, 0, 1, 0], [1, 1, 1, 0]],
                ignore_order=True,
                cl=ConsistencyLevel.QUORUM
            )

            # insert new rows that does not match the filter
            session.execute(insert_stmt, (0, 0, 1, 0))
            session.execute(insert_stmt, (1, 1, 0, 0))
            assert_all(
                session, "SELECT a, b, c, d FROM mv",
                [[1, 0, 1, 0], [1, 1, 1, 0]],
                ignore_order=True,
                cl=ConsistencyLevel.QUORUM
            )

            # insert new row that does match the filter
            session.execute(insert_stmt, (1, 2, 1, 0))
            assert_all(
                session, "SELECT a, b, c, d FROM mv",
                [[1, 0, 1, 0], [1, 1, 1, 0], [1, 2, 1, 0]],
                ignore_order=True,
                cl=ConsistencyLevel.QUORUM
            )

            # update rows that does not match the filter
            session.execute(update_stmt, (1, 1, -1, 0))
            session.execute(update_stmt, (0, 1, 1, 0))
            assert_all(
                session, "SELECT a, b, c, d FROM mv",
                [[1, 0, 1, 0], [1, 1, 1, 0], [1, 2, 1, 0]],
                ignore_order=True,
                cl=ConsistencyLevel.QUORUM
            )

            # update a row that does match the filter
            session.execute(update_stmt, (2, 1, 1, 1))
            assert_all(
                session, "SELECT a, b, c, d FROM mv",
                [[1, 0, 1, 0], [1, 1, 1, 2], [1, 2, 1, 0]],
                ignore_order=True,
                cl=ConsistencyLevel.QUORUM
            )

            # delete rows that does not match the filter
            session.execute(delete_stmt1, (1, 1, -1))
            session.execute(delete_stmt1, (2, 0, 1))
            session.execute(delete_stmt2, (0,))
            assert_all(
                session, "SELECT a, b, c, d FROM mv",
                [[1, 0, 1, 0], [1, 1, 1, 2], [1, 2, 1, 0]],
                ignore_order=True,
                cl=ConsistencyLevel.QUORUM
            )

            # delete a row that does match the filter
            session.execute(delete_stmt1, (1, 1, 1))
            assert_all(
                session, "SELECT a, b, c, d FROM mv",
                [[1, 0, 1, 0], [1, 2, 1, 0]],
                ignore_order=True,
                cl=ConsistencyLevel.QUORUM
            )

            # delete a partition that matches the filter
            session.execute(delete_stmt2, (1,))
            assert_all(session, "SELECT a, b, c, d FROM mv", [], cl=ConsistencyLevel.QUORUM)

            # Cleanup
            session.execute("DROP MATERIALIZED VIEW mv")
            session.execute("DROP TABLE test")

    def propagate_view_creation_over_non_existing_table(self):
        """
        The internal addition of a view over a non existing table should be ignored
        @jira_ticket CASSANDRA-13737
        """

        cluster = self.cluster
        cluster.populate(3)
        cluster.start(self.wrap_jvm_args(jvm_args=[]))
        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1, consistency_level=ConsistencyLevel.QUORUM)
        create_ks(session, 'ks', 3)

        session.execute('CREATE TABLE users (username varchar PRIMARY KEY, state varchar)')

        # create a materialized view only in nodes 1 and 2
        node3.stop(wait_other_notice=True)
        session.execute(('CREATE MATERIALIZED VIEW users_by_state AS '
                         'SELECT * FROM users WHERE state IS NOT NULL AND username IS NOT NULL '
                         'PRIMARY KEY (state, username)'))

        # drop the base table only in node 3
        node1.stop(wait_other_notice=True)
        node2.stop(wait_other_notice=True)
        node3.start(wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))
        session = self.patient_cql_connection(node3, consistency_level=ConsistencyLevel.QUORUM)
        session.execute('DROP TABLE ks.users')

        # restart the cluster
        cluster.stop()
        cluster.start(self.wrap_jvm_args(jvm_args=[]))

        # node3 should have received and ignored the creation of the MV over the dropped table
        self.assertTrue(node3.grep_log('Not adding view users_by_state because the base table'))

    def base_view_consistency_on_failure_after_mv_apply_test(self):
        self._test_base_view_consistency_on_crash("after")

    def base_view_consistency_on_failure_before_mv_apply_test(self):
        self._test_base_view_consistency_on_crash("before")

    def _test_base_view_consistency_on_crash(self, fail_phase):
        """
         * Fails base table write before or after applying views
         * Restart node and replay commit and batchlog
         * Check that base and views are present

         @jira_ticket CASSANDRA-13069
        """

        self.ignore_log_patterns = [r'Dummy failure']
        self.prepare(rf=1, install_byteman=True, options={'hinted_handoff_enabled': False}, enable_batch_commitlog=True)
        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_exclusive_cql_connection(node1)
        session.execute('USE ks')

        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t "
                         "WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))

        session.cluster.control_connection.wait_for_schema_agreement()

        debug('Make node1 fail {} view writes'.format(fail_phase))
        prefix = "4.0" if self.cluster.version() >= '4.0' else "pre4.0"
        node1.byteman_submit(['./byteman/{}/fail_{}_view_write.btm'.format(prefix, fail_phase)])

        debug('Write 1000 rows - all node1 writes should fail')

        failed = False
        for i in xrange(1, 1000):
            try:
                session.execute("INSERT INTO t (id, v, v2, v3) VALUES ({v}, {v}, 'a', 3.0) USING TIMESTAMP {v}".format(v=i))
            except WriteFailure:
                failed = True

        self.assertTrue(failed, "Should fail at least once.")
        self.assertTrue(node1.grep_log("Dummy failure"), "Should throw Dummy failure")

        missing_entries = 0
        session = self.patient_exclusive_cql_connection(node1)
        session.execute('USE ks')
        for i in xrange(1, 1000):
            view_entry = rows_to_list(session.execute(SimpleStatement("SELECT * FROM t_by_v WHERE id = {} AND v = {}".format(i, i),
                                                                      consistency_level=ConsistencyLevel.ONE)))
            base_entry = rows_to_list(session.execute(SimpleStatement("SELECT * FROM t WHERE id = {}".format(i),
                                                                      consistency_level=ConsistencyLevel.ONE)))

            if not base_entry:
                missing_entries += 1
            if not view_entry:
                missing_entries += 1

        debug("Missing entries {}".format(missing_entries))
        self.assertTrue(missing_entries > 0, )

        debug('Stopping node1 to simulate crash')
        node1.stop(gently=False, wait_other_notice=True)
        # Set batchlog.replay_timeout_in_ms=1 so we can ensure batchlog will be replayed below
        node1.start(jvm_args=self.wrap_jvm_args(jvm_args=["-Dcassandra.batchlog.replay_timeout_in_ms=1"]))

        debug('Replay batchlogs')
        time.sleep(0.001)  # Wait batchlog.replay_timeout_in_ms=1 (ms)
        self._replay_batchlogs()

        debug('Verify that both the base table entry and view are present after commit and batchlog replay')
        session = self.patient_exclusive_cql_connection(node1)
        session.execute('USE ks')
        for i in xrange(1, 1000):
            view_entry = rows_to_list(session.execute(SimpleStatement("SELECT * FROM t_by_v WHERE id = {} AND v = {}".format(i, i),
                                                                      consistency_level=ConsistencyLevel.ONE)))
            base_entry = rows_to_list(session.execute(SimpleStatement("SELECT * FROM t WHERE id = {}".format(i),
                                                                      consistency_level=ConsistencyLevel.ONE)))

            self.assertTrue(base_entry, "Both base {} and view entry {} should exist.".format(base_entry, view_entry))
            self.assertTrue(view_entry, "Both base {} and view entry {} should exist.".format(base_entry, view_entry))


@since('3.0')
class TestNewMaterializedViews(TestMaterializedViews):
    """
    Test materialized views implementation with new hidden columns.
    @jira_ticket DB-1060
    @since_dse 6.7
    """

    def is_legacy(self):
        """
        Overwrite method in parent class
        """
        return False

    def test_view_metadata_cleanup(self):
        """
        @jira_ticket: DB-2348 drop keyspace or view should clear built_views and view_build_status
        """
        session = self.prepare(rf=2, nodes=2)

        def populate_data(session, rows):
            debug("populate base data")
            for v in xrange(rows):
                session.execute("INSERT INTO t(k,c,a,b,e,f) VALUES({v},{v},{v},{v},{v},{v})".format(v=v))

        def verify_data(session, rows, views):
            debug("verify view data")
            for v in xrange(rows):
                for view in xrange(views):
                    assert_one(session, "SELECT * FROM mv{} WHERE k={v} AND c={v}".format(view, v=v), [v, v, v, v, v, v])

        def create_keyspace(session, ks="ks1", rf=2):
            create_ks(session, ks, rf)

        def create_table(session):
            debug("create base table")
            session.execute("CREATE TABLE t (k int, c int, a int, b int, e int, f int, primary key(k, c))")

        def create_views(session, views, keyspace="ks1"):
            debug("create view")
            for view in xrange(views):
                session.execute("CREATE MATERIALIZED VIEW mv{} AS SELECT * FROM t "
                                "WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c,k)".format(view))
            for view in xrange(views):
                self._wait_for_view(keyspace, "mv{}".format(view))

        def drop_keyspace(session, keyspace="ks1"):
            debug("drop keyspace {}".format(keyspace))
            session.execute("DROP KEYSPACE IF EXISTS {}".format(keyspace))

        def drop_views(session, views):
            debug("drop all views")
            for view in xrange(views):
                session.execute("DROP MATERIALIZED VIEW IF EXISTS mv{}".format(view))

        rows = 100
        views = 5

        create_keyspace(session)
        create_table(session)
        populate_data(session, rows)
        create_views(session, views)
        verify_data(session, rows, views)

        self._assert_view_meta(session, views)
        drop_keyspace(session)
        self._assert_view_meta(session, views, exists=False)

        create_keyspace(session)
        create_table(session)
        populate_data(session, rows)
        create_views(session, views)
        verify_data(session, rows, views)

        self._assert_view_meta(session, views)
        drop_views(session, views)
        self._assert_view_meta(session, views, exists=False)

    @since_dse('6.7')
    def test_hidden_columns_data_loss(self):
        """
        test hidden column table data loss, we should error out instead of considering the view as legacy.
        """
        self.ignore_log_patterns = (r'Expect view mv1 to be new but got legacy schema ')

        session = self.prepare(rf=1, nodes=1)
        session.execute('USE ks')
        session.execute("CREATE TABLE t (k int, c int, a int, b int, e int, f int, primary key(k, c))")

        # unselected hidden columbs
        session.execute(("CREATE MATERIALIZED VIEW mv1 AS SELECT k,c,a,b FROM t "
                         "WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k, c)"))

        session.execute("TRUNCATE system_schema.hidden_columns")

        debug("restart cluster, expect error on view schema creation")
        self.cluster.stop()
        self.cluster.start(wait_for_binary_proto=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))
        for node in self.cluster.nodelist():
            self.assertFalse(node.is_running())

    @since_dse('6.7')
    def test_query_hidden_column(self):
        """
        hidden column should not be exposed to client
        """
        session = self.prepare(rf=1, nodes=1)
        session.execute('USE ks')
        session.execute("CREATE TABLE t (k int, c int, a int, b int, e int, f int, primary key(k, c))")
        # unselected hidden columbs
        session.execute(("CREATE MATERIALIZED VIEW mv1 AS SELECT k,c,a,b FROM t "
                         "WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k, c)"))

        # strict-liveness hidden columbs
        session.execute(("CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM t "
                         "WHERE k IS NOT NULL AND c IS NOT NULL AND a IS NOT NULL PRIMARY KEY (k, c, a)"))

        session.execute("INSERT INTO t (k,c,a,b,e,f) VALUES(1,1,1,1,1,1)")

        with self.assertRaisesRegexp(InvalidRequest, 'Undefined column name ".a"'):
            session.execute("SELECT k,c,\".a\" FROM mv2")

        with self.assertRaisesRegexp(InvalidRequest, 'Undefined column name ".e"'):
            session.execute("SELECT k,c,\".e\" FROM mv1")

        with self.assertRaisesRegexp(InvalidRequest, 'Undefined column name ".a"'):
            session.execute("SELECT k,c,a,\".a\" FROM mv1")

        with self.assertRaisesRegexp(InvalidRequest, 'Undefined column name ".a"'):
            session.execute("SELECT JSON \".a\" FROM mv2")

        with self.assertRaisesRegexp(InvalidRequest, 'Undefined column name ".a"'):
            session.execute("SELECT JSON \".a\" FROM mv1")

        with self.assertRaisesRegexp(InvalidRequest, 'Undefined column name ".a"'):
            session.execute("SELECT toJson(\".a\") FROM mv1")

        assert_one(session, "SELECT JSON * FROM mv1", ['{"k": 1, "c": 1, "a": 1, "b": 1}'])
        assert_one(session, "SELECT JSON * FROM mv2", ['{"k": 1, "c": 1, "a": 1, "b": 1, "e": 1, "f": 1}'])

        with self.assertRaisesRegexp(InvalidRequest, 'Undefined column name ".a"'):
            session.prepare("SELECT \".a\" FROM mv1 WHERE k = ?")

        def export_as_string(session, view):
            return session.cluster.metadata.keyspaces['ks'].views[view].export_as_string()

        debug("Export as cql : {}".format(export_as_string(session, 'mv1')))
        self.assertTrue('\".e\"' not in export_as_string(session, 'mv1'),
                        "Found hidden column '.e' in driver export_as_string(): {}".format(export_as_string(session, 'mv1')))
        self.assertTrue('\".a\"' not in export_as_string(session, 'mv2'),
                        "Found hidden column '.a' in driver export_as_string(): {}".format(export_as_string(session, 'mv1')))

    @since_dse('6.7')
    def test_multiple_filtered_column_in_view_with_flush(self):
        self._test_multiple_filtered_column_in_view(flush=True)

    @since_dse('6.7')
    def test_multiple_filtered_column_in_view_without_flush(self):
        self._test_multiple_filtered_column_in_view(flush=False)

    def _test_multiple_filtered_column_in_view(self, flush):
        session = self.prepare(rf=3, nodes=3, options={'hinted_handoff_enabled': False}, consistency_level=ConsistencyLevel.ALL)
        node1, node2, node3 = self.cluster.nodelist()
        for node in self.cluster.nodelist():
            node.nodetool("disableautocompaction")

        session.execute('USE ks')
        session.execute("CREATE TABLE t (k int, c int, a int, b int, d int, e int, primary key(k,c))")
        session.execute(("CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM t WHERE k IS NOT NULL AND c IS NOT NULL"
                         " AND a=1 AND b=1 PRIMARY KEY (k,c)"))
        session.execute(("CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM t WHERE k IS NOT NULL AND c IS NOT NULL"
                         " AND a=1 AND b=1 AND d IS NOT NULL PRIMARY KEY (k,c,d)"))
        session.execute(("CREATE MATERIALIZED VIEW mv3 AS SELECT k,c FROM t WHERE k IS NOT NULL AND c IS NOT NULL"
                         " AND a=1 AND b=1 PRIMARY KEY (k,c)"))
        session.cluster.control_connection.wait_for_schema_agreement()

        debug("Test timestamp")
        # update unselected column 'e' with ts 1, view row is not generated
        self.update_view(session, "UPDATE t USING TIMESTAMP 1 SET e=1 WHERE k=1 AND c=1", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, None, None, None, 1])
        assert_none(session, "SELECT * FROM mv1")
        assert_none(session, "SELECT k FROM mv1")
        assert_none(session, "SELECT a,b FROM mv1")
        assert_none(session, "SELECT d,e FROM mv1")
        assert_none(session, "SELECT a,e FROM mv1")
        assert_none(session, "SELECT * FROM mv2")
        assert_none(session, "SELECT k FROM mv2")
        assert_none(session, "SELECT d FROM mv2")
        assert_none(session, "SELECT e FROM mv2")
        assert_none(session, "SELECT a,b FROM mv2")
        assert_none(session, "SELECT * FROM mv3")
        assert_none(session, "SELECT k FROM mv3")
        assert_none(session, "SELECT c FROM mv3")

        # update filtered column 'a' with ts 1, view row is not generated, missing 'b'
        self.update_view(session, "UPDATE t USING TIMESTAMP 1 SET a=1 WHERE k=1 AND c=1", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, None, None, 1])
        assert_none(session, "SELECT * FROM mv1")
        assert_none(session, "SELECT k FROM mv1")
        assert_none(session, "SELECT a,b FROM mv1")
        assert_none(session, "SELECT d,e FROM mv1")
        assert_none(session, "SELECT a,e FROM mv1")
        assert_none(session, "SELECT * FROM mv2")
        assert_none(session, "SELECT k FROM mv2")
        assert_none(session, "SELECT d FROM mv2")
        assert_none(session, "SELECT e FROM mv2")
        assert_none(session, "SELECT a,b FROM mv2")
        assert_none(session, "SELECT * FROM mv3")
        assert_none(session, "SELECT k FROM mv3")
        assert_none(session, "SELECT c FROM mv3")

        # update filtered column 'b'=2 with ts 1, view row is not generated, mismatched 'b'
        self.update_view(session, "UPDATE t USING TIMESTAMP 1 SET b=2 WHERE k=1 AND c=1", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, 2, None, 1])
        assert_none(session, "SELECT * FROM mv1")
        assert_none(session, "SELECT k FROM mv1")
        assert_none(session, "SELECT a,b FROM mv1")
        assert_none(session, "SELECT d,e FROM mv1")
        assert_none(session, "SELECT a,e FROM mv1")
        assert_none(session, "SELECT * FROM mv2")
        assert_none(session, "SELECT k FROM mv2")
        assert_none(session, "SELECT d FROM mv2")
        assert_none(session, "SELECT e FROM mv2")
        assert_none(session, "SELECT a,b FROM mv2")
        assert_none(session, "SELECT * FROM mv3")
        assert_none(session, "SELECT k FROM mv3")
        assert_none(session, "SELECT c FROM mv3")

        # update filtered column 'b'=1 with ts 2, view row is generated,
        self.update_view(session, "UPDATE t USING TIMESTAMP 2 SET b=1 WHERE k=1 AND c=1", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, 1, None, 1])
        assert_one(session, "SELECT * FROM mv1", [1, 1, 1, 1, None, 1])
        assert_one(session, "SELECT k FROM mv1", [1])
        assert_one(session, "SELECT a,b FROM mv1", [1, 1])
        assert_one(session, "SELECT d,e FROM mv1", [None, 1])
        assert_one(session, "SELECT a,e FROM mv1", [1, 1])
        assert_none(session, "SELECT * FROM mv2")  # missing key 'd'
        assert_none(session, "SELECT k FROM mv2")
        assert_none(session, "SELECT d FROM mv2")
        assert_none(session, "SELECT e FROM mv2")
        assert_none(session, "SELECT a,b FROM mv2")
        assert_one(session, "SELECT * FROM mv3", [1, 1])
        assert_one(session, "SELECT k FROM mv3", [1])
        assert_one(session, "SELECT c FROM mv3", [1])

        # delete filtered column 'b' with ts 2, update d=1, view row is not generated,
        self.update_view(session, "UPDATE t USING TIMESTAMP 2 SET b=null, d=1 WHERE k=1 AND c=1", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, None, 1, 1])
        assert_none(session, "SELECT * FROM mv1")
        assert_none(session, "SELECT k FROM mv1")
        assert_none(session, "SELECT a,b FROM mv1")
        assert_none(session, "SELECT d,e FROM mv1")
        assert_none(session, "SELECT a,e FROM mv1")
        assert_none(session, "SELECT * FROM mv2")
        assert_none(session, "SELECT k FROM mv2")
        assert_none(session, "SELECT d FROM mv2")
        assert_none(session, "SELECT e FROM mv2")
        assert_none(session, "SELECT a,b FROM mv2")
        assert_none(session, "SELECT * FROM mv3")
        assert_none(session, "SELECT k FROM mv3")
        assert_none(session, "SELECT c FROM mv3")

        # update filtered column 'b'=1 with ts 3, view row is generated,
        self.update_view(session, "UPDATE t USING TIMESTAMP 3 SET b=1 WHERE k=1 AND c=1", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, 1, 1, 1])
        assert_one(session, "SELECT k,c,a,b,d,e FROM mv1", [1, 1, 1, 1, 1, 1])
        assert_one(session, "SELECT k FROM mv1", [1])
        assert_one(session, "SELECT a,b FROM mv1", [1, 1])
        assert_one(session, "SELECT d,e FROM mv1", [1, 1])
        assert_one(session, "SELECT a,e FROM mv1", [1, 1])
        assert_one(session, "SELECT k,c,a,b,d,e FROM mv2", [1, 1, 1, 1, 1, 1])
        assert_one(session, "SELECT k FROM mv2", [1])
        assert_one(session, "SELECT d FROM mv2", [1])
        assert_one(session, "SELECT e FROM mv2", [1])
        assert_one(session, "SELECT a,b FROM mv2", [1, 1])
        assert_one(session, "SELECT * FROM mv3", [1, 1])
        assert_one(session, "SELECT k FROM mv3", [1])
        assert_one(session, "SELECT c FROM mv3", [1])

        # update column 'd'=2 with ts 3, switching rows
        self.update_view(session, "UPDATE t USING TIMESTAMP 3 SET d=2 WHERE k=1 AND c=1", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, 1, 2, 1])
        assert_one(session, "SELECT k,c,a,b,d,e FROM mv1", [1, 1, 1, 1, 2, 1])
        assert_one(session, "SELECT k FROM mv1", [1])
        assert_one(session, "SELECT a,b FROM mv1", [1, 1])
        assert_one(session, "SELECT d,e FROM mv1", [2, 1])
        assert_one(session, "SELECT a,e FROM mv1", [1, 1])
        assert_one(session, "SELECT k,c,a,b,d,e FROM mv2", [1, 1, 1, 1, 2, 1])
        assert_one(session, "SELECT k FROM mv2", [1])
        assert_one(session, "SELECT d FROM mv2", [2])
        assert_one(session, "SELECT e FROM mv2", [1])
        assert_one(session, "SELECT a,b FROM mv2", [1, 1])
        assert_one(session, "SELECT * FROM mv3", [1, 1])
        assert_one(session, "SELECT k FROM mv3", [1])
        assert_one(session, "SELECT c FROM mv3", [1])

        # update column 'd'=1 with ts 4 and 'a'=null, rows are shadowed
        self.update_view(session, "UPDATE t USING TIMESTAMP 4 SET d=1,a=null WHERE k=1 AND c=1", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, None, 1, 1, 1])
        assert_none(session, "SELECT k,c,a,b,d,e FROM mv1")
        assert_none(session, "SELECT k FROM mv1")
        assert_none(session, "SELECT a,b FROM mv1")
        assert_none(session, "SELECT d,e FROM mv1")
        assert_none(session, "SELECT a,e FROM mv1")
        assert_none(session, "SELECT k,c,a,b,d,e FROM mv2")
        assert_none(session, "SELECT k FROM mv2")
        assert_none(session, "SELECT d FROM mv2")
        assert_none(session, "SELECT e FROM mv2")
        assert_none(session, "SELECT a,b FROM mv2")
        assert_none(session, "SELECT * FROM mv3")
        assert_none(session, "SELECT k FROM mv3")
        assert_none(session, "SELECT c FROM mv3")

        # update column 'a'=1 with ts 5 , rows are shadowed
        self.update_view(session, "UPDATE t USING TIMESTAMP 5 SET a=1 WHERE k=1 AND c=1", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, 1, 1, 1])
        assert_one(session, "SELECT k,c,a,b,d,e FROM mv1", [1, 1, 1, 1, 1, 1])
        assert_one(session, "SELECT k FROM mv1", [1])
        assert_one(session, "SELECT a,b FROM mv1", [1, 1])
        assert_one(session, "SELECT d,e FROM mv1", [1, 1])
        assert_one(session, "SELECT a,e FROM mv1", [1, 1])
        assert_one(session, "SELECT k,c,a,b,d,e FROM mv2", [1, 1, 1, 1, 1, 1])
        assert_one(session, "SELECT k FROM mv2", [1])
        assert_one(session, "SELECT d FROM mv2", [1])
        assert_one(session, "SELECT e FROM mv2", [1])
        assert_one(session, "SELECT a,b FROM mv2", [1, 1])
        assert_one(session, "SELECT * FROM mv3", [1, 1])
        assert_one(session, "SELECT k FROM mv3", [1])
        assert_one(session, "SELECT c FROM mv3", [1])

        # update column 'b'=null with ts 3 and 'a'=null, rows are shadowed
        self.update_view(session, "UPDATE t USING TIMESTAMP 3 SET b=null WHERE k=1 AND c=1", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, None, 1, 1])
        assert_none(session, "SELECT k,c,a,b,d,e FROM mv1")
        assert_none(session, "SELECT k FROM mv1")
        assert_none(session, "SELECT a,b FROM mv1")
        assert_none(session, "SELECT d,e FROM mv1")
        assert_none(session, "SELECT a,e FROM mv1")
        assert_none(session, "SELECT k,c,a,b,d,e FROM mv2")
        assert_none(session, "SELECT k FROM mv2")
        assert_none(session, "SELECT d FROM mv2")
        assert_none(session, "SELECT e FROM mv2")
        assert_none(session, "SELECT a,b FROM mv2")
        assert_none(session, "SELECT * FROM mv3")
        assert_none(session, "SELECT k FROM mv3")
        assert_none(session, "SELECT c FROM mv3")

        # update column 'b'=1 with ts 4, rows are alive
        self.update_view(session, "UPDATE t USING TIMESTAMP 4 SET b=1 WHERE k=1 AND c=1", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, 1, 1, 1])
        assert_one(session, "SELECT k,c,a,b,d,e FROM mv1", [1, 1, 1, 1, 1, 1])
        assert_one(session, "SELECT k FROM mv1", [1])
        assert_one(session, "SELECT a,b FROM mv1", [1, 1])
        assert_one(session, "SELECT d,e FROM mv1", [1, 1])
        assert_one(session, "SELECT a,e FROM mv1", [1, 1])
        assert_one(session, "SELECT k,c,a,b,d,e FROM mv2", [1, 1, 1, 1, 1, 1])
        assert_one(session, "SELECT k FROM mv2", [1])
        assert_one(session, "SELECT d FROM mv2", [1])
        assert_one(session, "SELECT e FROM mv2", [1])
        assert_one(session, "SELECT a,b FROM mv2", [1, 1])
        assert_one(session, "SELECT * FROM mv3", [1, 1])
        assert_one(session, "SELECT k FROM mv3", [1])
        assert_one(session, "SELECT c FROM mv3", [1])

        # cannot drop filtered colummn
        assert_invalid(session,
                       "ALTER TABLE t DROP a",
                       "Cannot drop column a from base table ks.t because it is required on materialized view ks.mv1.")

        # cannot drop strict-liveness colummn
        assert_invalid(session,
                       "ALTER TABLE t DROP d",
                       "Cannot drop column d from base table ks.t because it is required on materialized view ks.mv2.")

        # drop column e, doesn't affect row liveness
        session.execute("ALTER TABLE t DROP e")
        assert_one(session, "SELECT * FROM t", [1, 1, 1, 1, 1])
        assert_one(session, "SELECT * FROM mv1", [1, 1, 1, 1, 1])
        assert_one(session, "SELECT k FROM mv1", [1])
        assert_one(session, "SELECT a,b FROM mv1", [1, 1])
        assert_one(session, "SELECT d FROM mv1", [1])
        assert_one(session, "SELECT a FROM mv1", [1])
        assert_one(session, "SELECT * FROM mv2", [1, 1, 1, 1, 1])
        assert_one(session, "SELECT k FROM mv2", [1])
        assert_one(session, "SELECT d FROM mv2", [1])
        assert_one(session, "SELECT a,b FROM mv2", [1, 1])
        assert_one(session, "SELECT * FROM mv3", [1, 1])
        assert_one(session, "SELECT k FROM mv3", [1])
        assert_one(session, "SELECT c FROM mv3", [1])

        # update column 'd'=null with ts 4
        self.update_view(session, "UPDATE t USING TIMESTAMP 4 SET d=null WHERE k=1 AND c=1", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, 1, None])
        assert_one(session, "SELECT k,c,a,b,d FROM mv1", [1, 1, 1, 1, None])
        assert_one(session, "SELECT k FROM mv1", [1])
        assert_one(session, "SELECT a,b FROM mv1", [1, 1])
        assert_one(session, "SELECT d FROM mv1", [None])
        assert_one(session, "SELECT a FROM mv1", [1])
        assert_none(session, "SELECT k,c,a,b,d FROM mv2")
        assert_none(session, "SELECT k FROM mv2")
        assert_none(session, "SELECT d FROM mv2")
        assert_none(session, "SELECT a,b FROM mv2")
        assert_one(session, "SELECT * FROM mv3", [1, 1])
        assert_one(session, "SELECT k FROM mv3", [1])
        assert_one(session, "SELECT c FROM mv3", [1])

    @since_dse('6.7')
    def test_multiple_unselected_column_in_view_with_flush(self):
        self._test_multiple_unselected_column_in_view(flush=True)

    @since_dse('6.7')
    def test_multiple_unselected_column_in_view_without_flush(self):
        self._test_multiple_unselected_column_in_view(flush=False)

    def _test_multiple_unselected_column_in_view(self, flush):
        session = self.prepare(rf=3, nodes=3, options={'hinted_handoff_enabled': False}, consistency_level=ConsistencyLevel.ALL)
        node1, node2, node3 = self.cluster.nodelist()
        for node in self.cluster.nodelist():
            node.nodetool("disableautocompaction")

        session.execute('USE ks')
        session.execute("CREATE TABLE t (k int, c int, a int, b int, d int, e int, primary key(k,c))")
        session.execute(("CREATE MATERIALIZED VIEW mv AS SELECT k,a,b FROM t "
                         "WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k,c)"))
        session.cluster.control_connection.wait_for_schema_agreement()

        debug("Test timestamp")
        # update unselected column 'd' with ts 5, view row is generated
        self.update_view(session, "UPDATE t USING TIMESTAMP 5 SET d=1 WHERE k=1 AND c=1", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, None, None, 1, None])
        assert_one(session, "SELECT * FROM mv", [1, 1, None, None])
        assert_one(session, "SELECT k,c FROM mv", [1, 1])
        assert_one(session, "SELECT a FROM mv", [None])
        assert_one(session, "SELECT c,b FROM mv", [1, None])

        # delete unselected column 'd' with ts 5, view row is shadowed
        self.update_view(session, "UPDATE t USING TIMESTAMP 5 SET d=null WHERE k=1 AND c=1", flush)
        assert_none(session, "SELECT * FROM t")
        assert_none(session, "SELECT * FROM mv")
        assert_none(session, "SELECT k,c FROM mv")
        assert_none(session, "SELECT a FROM mv")
        assert_none(session, "SELECT c,b FROM mv")

        # update unselected column 'e' with ts 1, view row is generated
        self.update_view(session, "UPDATE t USING TIMESTAMP 1 SET e=1 WHERE k=1 AND c=1", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, None, None, None, 1])
        assert_one(session, "SELECT * FROM mv", [1, 1, None, None])
        assert_one(session, "SELECT k,c FROM mv", [1, 1])
        assert_one(session, "SELECT a FROM mv", [None])
        assert_one(session, "SELECT c,b FROM mv", [1, None])

        # delete unselected column 'e' with ts 1, view row is shadowed
        self.update_view(session, "UPDATE t USING TIMESTAMP 1 SET e=null WHERE k=1 AND c=1", flush)
        assert_none(session, "SELECT * FROM t")
        assert_none(session, "SELECT * FROM mv")
        assert_none(session, "SELECT k,c FROM mv")
        assert_none(session, "SELECT a FROM mv")
        assert_none(session, "SELECT c,b FROM mv")

        # update selected column 'a' with ts 1, view row is generated
        self.update_view(session, "UPDATE t USING TIMESTAMP 1 SET a=1 WHERE k=1 AND c=1", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, None, None, None])
        assert_one(session, "SELECT * FROM mv", [1, 1, 1, None])
        assert_one(session, "SELECT k,c FROM mv", [1, 1])
        assert_one(session, "SELECT a FROM mv", [1])
        assert_one(session, "SELECT c,b FROM mv", [1, None])

        # delete selected column 'a' with ts 5, update unselected column 'e', view row is generated
        self.update_view(session, "UPDATE t USING TIMESTAMP 5 SET a=null, e=1 WHERE k=1 AND c=1", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, None, None, None, 1])
        assert_one(session, "SELECT * FROM mv", [1, 1, None, None])
        assert_one(session, "SELECT k,c FROM mv", [1, 1])
        assert_one(session, "SELECT a FROM mv", [None])
        assert_one(session, "SELECT c,b FROM mv", [1, None])

        # row deletion with ts5, view row is shadowed
        self.update_view(session, "DELETE FROM t USING TIMESTAMP 5 WHERE k=1 AND c=1", flush)
        assert_none(session, "SELECT * FROM t")
        assert_none(session, "SELECT * FROM mv")
        assert_none(session, "SELECT k,c FROM mv")
        assert_none(session, "SELECT a FROM mv")
        assert_none(session, "SELECT c,b FROM mv")

        if flush:
            self.cluster.compact()
            assert_none(session, "SELECT * FROM t")
            assert_none(session, "SELECT * FROM mv")
            assert_none(session, "SELECT k,c FROM mv")
            assert_none(session, "SELECT a FROM mv")
            assert_none(session, "SELECT c,b FROM mv")

        # add unselected column 'u'
        session.execute('ALTER TABLE t ADD u int')
        assert_none(session, "SELECT * FROM t")
        assert_none(session, "SELECT * FROM mv")
        assert_none(session, "SELECT k,c FROM mv")
        assert_none(session, "SELECT a FROM mv")
        assert_none(session, "SELECT c,b FROM mv")

        # update selected column 'u' with ts 6, view row is generated
        self.update_view(session, "UPDATE t USING TIMESTAMP 6 SET u=1 WHERE k=1 AND c=1", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, None, None, None, None, 1])
        assert_one(session, "SELECT * FROM mv", [1, 1, None, None])
        assert_one(session, "SELECT k,c FROM mv", [1, 1])
        assert_one(session, "SELECT a FROM mv", [None])
        assert_one(session, "SELECT c,b FROM mv", [1, None])

        # add selected column 'b'
        session.execute('ALTER TABLE t DROP b')
        assert_one(session, "SELECT * FROM t", [1, 1, None, None, None, 1])
        assert_one(session, "SELECT * FROM mv", [1, 1, None])
        assert_one(session, "SELECT k,c FROM mv", [1, 1])
        assert_one(session, "SELECT a FROM mv", [None])
        assert_one(session, "SELECT c,a FROM mv", [1, None])

        # drop unselected column 'u'
        session.execute('ALTER TABLE t DROP u')
        assert_none(session, "SELECT * FROM t")
        assert_none(session, "SELECT * FROM mv")
        assert_none(session, "SELECT k,c FROM mv")
        assert_none(session, "SELECT a FROM mv")
        assert_none(session, "SELECT c,a FROM mv")

        # update unselected column 'e' with ts 6, view row is generated
        self.update_view(session, "UPDATE t USING TIMESTAMP 6 SET e=666 WHERE k=1 AND c=1", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, None, None, 666])
        assert_one(session, "SELECT * FROM mv", [1, 1, None])
        assert_one(session, "SELECT k,c FROM mv", [1, 1])
        assert_one(session, "SELECT a FROM mv", [None])
        assert_one(session, "SELECT c,a FROM mv", [1, None])

    @since_dse('6.7')
    def test_multiple_base_column_in_view_pk_with_flush(self):
        self._test_multiple_base_column_in_view_pk(flush=True)

    @since_dse('6.7')
    def test_multiple_base_column_in_view_pk_without_flush(self):
        self._test_multiple_base_column_in_view_pk(flush=False)

    def _test_multiple_base_column_in_view_pk(self, flush):
        session = self.prepare(rf=3, nodes=3, options={'hinted_handoff_enabled': False}, consistency_level=ConsistencyLevel.ALL)
        node1, node2, node3 = self.cluster.nodelist()
        for node in self.cluster.nodelist():
            node.nodetool("disableautocompaction")

        session.execute('USE ks')
        session.execute("CREATE TABLE t (k int, a int, b int, c int, d int, e int, f int, primary key(k))")
        session.execute(("CREATE MATERIALIZED VIEW mv AS SELECT k,a,b,c,d FROM t "
                         "WHERE k IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (k,a,b)"))
        session.cluster.control_connection.wait_for_schema_agreement()

        debug("Test timestamp")
        # update unselected column 'e', view row is not generated becuase strict-liveness columns are required
        self.update_view(session, "UPDATE t USING TIMESTAMP 1 SET e=1 WHERE k=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, None, None, None, None, 1, None])
        assert_none(session, "SELECT * FROM mv")
        assert_none(session, "SELECT k FROM mv")
        assert_none(session, "SELECT a,b FROM mv")
        assert_none(session, "SELECT c,d FROM mv")
        assert_none(session, "SELECT k,a,c FROM mv")

        # update one of view key 'a', view row is not generated
        self.update_view(session, "UPDATE t USING TIMESTAMP 1 SET a=1 WHERE k=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, None, None, None, 1, None])
        assert_none(session, "SELECT * FROM mv")
        assert_none(session, "SELECT k FROM mv")
        assert_none(session, "SELECT a,b FROM mv")
        assert_none(session, "SELECT c,d FROM mv")
        assert_none(session, "SELECT k,a,c FROM mv")

        # update another view key 'b', view row is generated
        self.update_view(session, "UPDATE t USING TIMESTAMP 1 SET b=1 WHERE k=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, None, None, 1, None])
        assert_one(session, "SELECT * FROM mv", [1, 1, 1, None, None])
        assert_one(session, "SELECT k FROM mv", [1])
        assert_one(session, "SELECT a,b FROM mv", [1, 1])
        assert_one(session, "SELECT c,d FROM mv", [None, None])
        assert_one(session, "SELECT k,a,c FROM mv", [1, 1, None])

        # delete one of view key 'b' with ts 1, view row should be shadowed
        self.update_view(session, "UPDATE t USING TIMESTAMP 1 SET b=null WHERE k=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, None, None, None, 1, None])
        assert_none(session, "SELECT * FROM mv")
        assert_none(session, "SELECT k FROM mv")
        assert_none(session, "SELECT a,b FROM mv")
        assert_none(session, "SELECT c,d FROM mv")
        assert_none(session, "SELECT k,a,c FROM mv")

        # update view key 'b' back to live with ts 2, view row should be alive
        self.update_view(session, "UPDATE t USING TIMESTAMP 2 SET b=1 WHERE k=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, None, None, 1, None])
        assert_one(session, "SELECT * FROM mv", [1, 1, 1, None, None])
        assert_one(session, "SELECT k FROM mv", [1])
        assert_one(session, "SELECT a,b FROM mv", [1, 1])
        assert_one(session, "SELECT c,d FROM mv", [None, None])
        assert_one(session, "SELECT k,a,c FROM mv", [1, 1, None])

        # delete one of view key 'a' with ts 1, view row should be shadowed
        self.update_view(session, "UPDATE t USING TIMESTAMP 1 SET a=null WHERE k=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, None, 1, None, None, 1, None])
        assert_none(session, "SELECT * FROM mv")
        assert_none(session, "SELECT k FROM mv")
        assert_none(session, "SELECT a,b FROM mv")
        assert_none(session, "SELECT c,d FROM mv")
        assert_none(session, "SELECT k,a,c FROM mv")

        # update view key 'a' back to live with ts 2, view row should be alive
        self.update_view(session, "UPDATE t USING TIMESTAMP 2 SET a=1 WHERE k=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, None, None, 1, None])
        assert_one(session, "SELECT * FROM mv", [1, 1, 1, None, None])
        assert_one(session, "SELECT k FROM mv", [1])
        assert_one(session, "SELECT a,b FROM mv", [1, 1])
        assert_one(session, "SELECT c,d FROM mv", [None, None])
        assert_one(session, "SELECT k,a,c FROM mv", [1, 1, None])

        # row deletion with ts 1, view row should be alive
        self.update_view(session, "DELETE FROM t USING TIMESTAMP 1 WHERE k=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, None, None, None, None])
        assert_one(session, "SELECT * FROM mv", [1, 1, 1, None, None])
        assert_one(session, "SELECT k FROM mv", [1])
        assert_one(session, "SELECT a,b FROM mv", [1, 1])
        assert_one(session, "SELECT c,d FROM mv", [None, None])
        assert_one(session, "SELECT k,a,c FROM mv", [1, 1, None])

        # row deletion with ts 2, view row should be alive
        self.update_view(session, "DELETE FROM t USING TIMESTAMP 2 WHERE k=1;", flush)
        assert_none(session, "SELECT * FROM t")
        assert_none(session, "SELECT * FROM mv")
        assert_none(session, "SELECT k FROM mv")
        assert_none(session, "SELECT a,b FROM mv")
        assert_none(session, "SELECT c,d FROM mv")
        assert_none(session, "SELECT k,a,c FROM mv")

        # insert row with ts 3
        self.update_view(session, "INSERT INTO T(k,a,b,c,e) VALUES(1,1,1,1,1) USING TIMESTAMP 3", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, 1, None, 1, None])
        assert_one(session, "SELECT * FROM mv", [1, 1, 1, 1, None])
        assert_one(session, "SELECT k FROM mv", [1])
        assert_one(session, "SELECT a,b FROM mv", [1, 1])
        assert_one(session, "SELECT c,d FROM mv", [1, None])
        assert_one(session, "SELECT k,a,c FROM mv", [1, 1, 1])

        # update 'a' to 2 with ts 4, switching rows
        self.update_view(session, "UPDATE t USING TIMESTAMP 4 SET a=2, c=2 WHERE k=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 2, 1, 2, None, 1, None])
        assert_one(session, "SELECT * FROM mv", [1, 2, 1, 2, None])
        assert_one(session, "SELECT k FROM mv", [1])
        assert_one(session, "SELECT a,b FROM mv", [2, 1])
        assert_one(session, "SELECT c,d FROM mv", [2, None])
        assert_one(session, "SELECT k,a,c FROM mv", [1, 2, 2])

        # update 'b' to 2 with ts 4, switching rows
        self.update_view(session, "UPDATE t USING TIMESTAMP 4 SET b=2, e=2 WHERE k=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 2, 2, 2, None, 2, None])
        assert_one(session, "SELECT * FROM mv", [1, 2, 2, 2, None])
        assert_one(session, "SELECT k FROM mv", [1])
        assert_one(session, "SELECT a,b FROM mv", [2, 2])
        assert_one(session, "SELECT c,d FROM mv", [2, None])
        assert_one(session, "SELECT k,a,c FROM mv", [1, 2, 2])

        # update 'a' to 1 with ts 6, switching rows
        self.update_view(session, "UPDATE t USING TIMESTAMP 6 SET a=1, d=1 WHERE k=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 2, 2, 1, 2, None])
        assert_one(session, "SELECT * FROM mv", [1, 1, 2, 2, 1])
        assert_one(session, "SELECT k FROM mv", [1])
        assert_one(session, "SELECT a,b FROM mv", [1, 2])
        assert_one(session, "SELECT c,d FROM mv", [2, 1])
        assert_one(session, "SELECT k,a,c FROM mv", [1, 1, 2])

        # update 'b' to 1 with ts 5, switching rows
        self.update_view(session, "UPDATE t USING TIMESTAMP 5 SET b=1, c=1, e=1 WHERE k=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, 1, 1, 1, None])
        assert_one(session, "SELECT * FROM mv", [1, 1, 1, 1, 1])
        assert_one(session, "SELECT k FROM mv", [1])
        assert_one(session, "SELECT a,b FROM mv", [1, 1])
        assert_one(session, "SELECT c,d FROM mv", [1, 1])
        assert_one(session, "SELECT k,a,c FROM mv", [1, 1, 1])

        if flush:
            self.cluster.compact()
            assert_one(session, "SELECT * FROM t", [1, 1, 1, 1, 1, 1, None])
            assert_one(session, "SELECT * FROM mv", [1, 1, 1, 1, 1])
            assert_one(session, "SELECT k FROM mv", [1])
            assert_one(session, "SELECT a,b FROM mv", [1, 1])
            assert_one(session, "SELECT c,d FROM mv", [1, 1])
            assert_one(session, "SELECT k,a,c FROM mv", [1, 1, 1])

        debug("Test TTL")
        # update 'b' to 2 with ttl 10, switching rows
        self.update_view(session, "UPDATE t USING TTL 15 SET b=2 WHERE k=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 2, 1, 1, 1, None])
        assert_one(session, "SELECT * FROM mv", [1, 1, 2, 1, 1])
        assert_one(session, "SELECT k FROM mv", [1])
        assert_one(session, "SELECT a,b FROM mv", [1, 2])
        assert_one(session, "SELECT c,d FROM mv", [1, 1])
        assert_one(session, "SELECT k,a,c FROM mv", [1, 1, 1])

        time.sleep(15)

        # view row should be removed due to expired strict-liveness column 'b'
        assert_one(session, "SELECT * FROM t", [1, 1, None, 1, 1, 1, None])
        assert_none(session, "SELECT * FROM mv")
        assert_none(session, "SELECT k FROM mv")
        assert_none(session, "SELECT a,b FROM mv")
        assert_none(session, "SELECT c,d FROM mv")
        assert_none(session, "SELECT k,a,c FROM mv")

        # update 'b' to 1 with ttl 30, switching rows
        self.update_view(session, "UPDATE t USING TTL 30 SET b=1 WHERE k=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, 1, 1, 1, None])
        assert_one(session, "SELECT * FROM mv", [1, 1, 1, 1, 1])
        assert_one(session, "SELECT k FROM mv", [1])
        assert_one(session, "SELECT a,b FROM mv", [1, 1])
        assert_one(session, "SELECT c,d FROM mv", [1, 1])
        assert_one(session, "SELECT k,a,c FROM mv", [1, 1, 1])

        # update 'a' to 2 with ttl 10, switching rows
        self.update_view(session, "UPDATE t USING TTL 10 SET a=2 WHERE k=1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 2, 1, 1, 1, 1, None])
        assert_one(session, "SELECT * FROM mv", [1, 2, 1, 1, 1])
        assert_one(session, "SELECT k FROM mv", [1])
        assert_one(session, "SELECT a,b FROM mv", [2, 1])
        assert_one(session, "SELECT c,d FROM mv", [1, 1])
        assert_one(session, "SELECT k,a,c FROM mv", [1, 2, 1])

        time.sleep(10)

        # view row should be removed due to expired strict-liveness column 'a'
        assert_one(session, "SELECT * FROM t", [1, None, 1, 1, 1, 1, None])
        assert_none(session, "SELECT * FROM mv")
        assert_none(session, "SELECT k FROM mv")
        assert_none(session, "SELECT a,b FROM mv")
        assert_none(session, "SELECT c,d FROM mv")
        assert_none(session, "SELECT k,a,c FROM mv")

        # clear data
        session.execute("TRUNCATE t")
        assert_none(session, "SELECT * FROM t")
        assert_none(session, "SELECT * FROM mv")
        assert_none(session, "SELECT k FROM mv")
        assert_none(session, "SELECT a,b FROM mv")
        assert_none(session, "SELECT c,d FROM mv")
        assert_none(session, "SELECT k,a,c FROM mv")

        debug("Test node down")
        # Prepare some data, and stop node, then switch row, check if view data will converge
        self.update_view(session, "INSERT INTO T(k,a,b,c,d,e,f) VALUES(1,1,1,1,1,1,1) USING TIMESTAMP 10", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, 1, 1, 1, 1])
        assert_one(session, "SELECT * FROM mv", [1, 1, 1, 1, 1])
        assert_one(session, "SELECT k FROM mv", [1])
        assert_one(session, "SELECT a,b FROM mv", [1, 1])
        assert_one(session, "SELECT c,d FROM mv", [1, 1])
        assert_one(session, "SELECT k,a,c FROM mv", [1, 1, 1])

        debug("Populating node 3")
        # prepare updated data on node 3
        node1.stop(wait_for_binary_proto=True, wait_other_notice=True)
        node2.stop(wait_for_binary_proto=True, wait_other_notice=True)
        query = SimpleStatement("UPDATE t USING TIMESTAMP 11 SET a=2 WHERE k=1;", consistency_level=ConsistencyLevel.ONE)
        self.update_view(session, query, flush)
        node1.start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))
        node2.start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))

        debug("Populating node 1")
        # prepare updated data on node 1
        node2.stop(wait_for_binary_proto=True, wait_other_notice=True)
        node3.stop(wait_for_binary_proto=True, wait_other_notice=True)
        query = SimpleStatement("UPDATE t USING TIMESTAMP 11 SET b=2 WHERE k=1;", consistency_level=ConsistencyLevel.ONE)
        self.update_view(session, query, flush)
        node2.start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))
        node3.start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))

        debug("Populating node 2")
        # prepare updated data on node 2
        node1.stop(wait_for_binary_proto=True, wait_other_notice=True)
        node3.stop(wait_for_binary_proto=True, wait_other_notice=True)
        query = SimpleStatement("UPDATE t USING TIMESTAMP 12 SET a=1 WHERE k=1;", consistency_level=ConsistencyLevel.ONE)
        self.update_view(session, query, flush)
        node1.start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))
        node3.start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=self.wrap_jvm_args(jvm_args=[]))

        debug("Verifying digest")
        # expect digest mismatch
        query = SimpleStatement("SELECT * FROM mv where k=1", consistency_level=ConsistencyLevel.ALL)
        result = session.execute(query, trace=True)
        self.check_trace_events(result.get_query_trace(), True)
        debug(result.current_rows)
        self.assertEqual([[1, 1, 2, 1, 1], [1, 2, 1, 1, 1]], rows_to_list(result))  # row(k=1,a=1,b=1) is shadowed

        # expect no digest mismatch
        query = SimpleStatement("SELECT * FROM mv where k=1", consistency_level=ConsistencyLevel.ALL)
        result = session.execute(query, trace=True)
        self.check_trace_events(result.get_query_trace(), False)
        debug(result.current_rows)
        self.assertEqual([[1, 1, 2, 1, 1], [1, 2, 1, 1, 1]], rows_to_list(result))  # row(k=1,a=1,b=1) is shadowed

        # read repair base to propogate view tombstones and fix view
        assert_one(session, "SELECT * FROM t", [1, 1, 2, 1, 1, 1, 1])

        # expect digest mismatch
        query = SimpleStatement("SELECT * FROM mv where k=1", consistency_level=ConsistencyLevel.ALL)
        result = session.execute(query, trace=True)
        self.check_trace_events(result.get_query_trace(), True)  # tombstone(k=1,a=2,b=1) needas to be propogated
        debug(result.current_rows)
        self.assertEqual([[1, 1, 2, 1, 1]], rows_to_list(result))

        # expect no digest mismatch
        query = SimpleStatement("SELECT * FROM mv where k=1", consistency_level=ConsistencyLevel.ALL)
        result = session.execute(query, trace=True)
        self.check_trace_events(result.get_query_trace(), False)
        debug(result.current_rows)
        self.assertEqual([[1, 1, 2, 1, 1]], rows_to_list(result))

        assert_one(session, "SELECT k FROM mv", [1])
        assert_one(session, "SELECT a,b FROM mv", [1, 2])
        assert_one(session, "SELECT c,d FROM mv", [1, 1])
        assert_one(session, "SELECT k,a,c FROM mv", [1, 1, 1])

        # drop unselected column 'f', no changes to view
        session.execute('ALTER TABLE t DROP f')
        assert_one(session, "SELECT * FROM t", [1, 1, 2, 1, 1, 1])
        assert_one(session, "SELECT * FROM mv", [1, 1, 2, 1, 1])
        assert_one(session, "SELECT k FROM mv", [1])
        assert_one(session, "SELECT a,b FROM mv", [1, 2])
        assert_one(session, "SELECT c,d FROM mv", [1, 1])
        assert_one(session, "SELECT k,a,c FROM mv", [1, 1, 1])

        # drop selected column 'd', view column is dropped
        session.execute('ALTER TABLE t DROP d')
        assert_one(session, "SELECT * FROM t", [1, 1, 2, 1, 1])
        assert_one(session, "SELECT * FROM mv", [1, 1, 2, 1])
        assert_one(session, "SELECT k FROM mv", [1])
        assert_one(session, "SELECT a,b FROM mv", [1, 2])
        assert_one(session, "SELECT c FROM mv", [1])
        assert_one(session, "SELECT k,a,c FROM mv", [1, 1, 1])

        # delete strict-liveness column 'b', view row is shadowed
        session.execute("UPDATE t USING TIMESTAMP 11 SET b=null WHERE k=1;")
        assert_one(session, "SELECT * FROM t", [1, 1, None, 1, 1])
        assert_none(session, "SELECT * FROM mv")
        assert_none(session, "SELECT k FROM mv")
        assert_none(session, "SELECT a,b FROM mv")
        assert_none(session, "SELECT c FROM mv")
        assert_none(session, "SELECT k,a,c FROM mv")

    @since_dse('6.7')
    def test_multiple_filtered_column_and_key_column_in_view_with_flush(self):
        self._test_multiple_filtered_column_and_key_column_in_view(flush=False)

    @since_dse('6.7')
    def test_multiple_filtered_column_and_key_column_in_view_without_flush(self):
        self._test_multiple_filtered_column_and_key_column_in_view(flush=False)

    def _test_multiple_filtered_column_and_key_column_in_view(self, flush):
        session = self.prepare(rf=3, nodes=3, options={'hinted_handoff_enabled': False}, consistency_level=ConsistencyLevel.ALL)
        node1, node2, node3 = self.cluster.nodelist()
        for node in self.cluster.nodelist():
            node.nodetool("disableautocompaction")

        # 2 filtered regular columns and one of them is unselected, 2 regular column as view pk.
        session.execute('USE ks')
        session.execute("CREATE TABLE t (k int, c int, a int, b int, d int, e int, f int, primary key(k,c))")
        session.execute(("CREATE MATERIALIZED VIEW mv AS SELECT e,f FROM t WHERE k IS NOT NULL AND c=1"
                         " AND a IS NOT NULL AND b IS NOT NULL AND d=1 AND e=1 PRIMARY KEY (k,c,a,b)"))
        session.cluster.control_connection.wait_for_schema_agreement()

        # Set initial values TS=1
        self.update_view(session, "INSERT INTO t (k, c, a, b, d, e, f) VALUES (1, 1, 1, 1, 1, 1, 1) USING TIMESTAMP 1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1, 1, 1, 1, 1])
        assert_one(session, "SELECT * FROM mv", [1, 1, 1, 1, 1, 1])
        assert_one(session, "SELECT k FROM mv", [1])
        assert_one(session, "SELECT c,a FROM mv", [1, 1])
        assert_one(session, "SELECT b,e,f FROM mv", [1, 1, 1])
        assert_one(session, "SELECT f FROM mv", [1])
        assert_one(session, "SELECT k,f FROM mv", [1, 1])
        assert_one(session, "SELECT e FROM mv", [1])

        # swith row by updating regular column in view pk a=2@2
        self.update_view(session, "UPDATE t USING TIMESTAMP 2 SET a = 2 WHERE k = 1 and c = 1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 2, 1, 1, 1, 1])
        assert_one(session, "SELECT * FROM mv", [1, 1, 2, 1, 1, 1])
        assert_one(session, "SELECT k FROM mv", [1])
        assert_one(session, "SELECT c,a FROM mv", [1, 2])
        assert_one(session, "SELECT b,e,f FROM mv", [1, 1, 1])
        assert_one(session, "SELECT f FROM mv", [1])
        assert_one(session, "SELECT k,f FROM mv", [1, 1])
        assert_one(session, "SELECT e FROM mv", [1])

        # shadow row by updating unselected filter d=2@2
        self.update_view(session, "UPDATE t USING TIMESTAMP 2 SET d = 2 WHERE k = 1 and c = 1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 2, 1, 2, 1, 1])
        assert_none(session, "SELECT * FROM mv")
        assert_none(session, "SELECT k FROM mv")
        assert_none(session, "SELECT c,a FROM mv")
        assert_none(session, "SELECT b,e,f FROM mv")
        assert_none(session, "SELECT f FROM mv")
        assert_none(session, "SELECT k,f FROM mv")
        assert_none(session, "SELECT e FROM mv")

        # switch row by updating regular column in view pk b=2@2
        self.update_view(session, "UPDATE t USING TIMESTAMP 2 SET b = 2 WHERE k = 1 and c = 1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 2, 2, 2, 1, 1])
        assert_none(session, "SELECT * FROM mv")
        assert_none(session, "SELECT k FROM mv")
        assert_none(session, "SELECT c,a FROM mv")
        assert_none(session, "SELECT b,e,f FROM mv")
        assert_none(session, "SELECT f FROM mv")
        assert_none(session, "SELECT k,f FROM mv")
        assert_none(session, "SELECT e FROM mv")

        # bring row back to live by updating unselected filter d=1@3
        self.update_view(session, "UPDATE t USING TIMESTAMP 3 SET d = 1 WHERE k = 1 and c = 1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 2, 2, 1, 1, 1])
        assert_one(session, "SELECT * FROM mv", [1, 1, 2, 2, 1, 1])
        assert_one(session, "SELECT k FROM mv", [1])
        assert_one(session, "SELECT c,a FROM mv", [1, 2])
        assert_one(session, "SELECT b,e,f FROM mv", [2, 1, 1])
        assert_one(session, "SELECT f FROM mv", [1])
        assert_one(session, "SELECT k,f FROM mv", [1, 1])
        assert_one(session, "SELECT e FROM mv", [1])

        # shadow row by deleting filter e@1
        self.update_view(session, "DELETE e FROM t USING TIMESTAMP 1 WHERE k = 1 and c = 1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 2, 2, 1, None, 1])
        assert_none(session, "SELECT * FROM mv")
        assert_none(session, "SELECT k FROM mv")
        assert_none(session, "SELECT c,a FROM mv")
        assert_none(session, "SELECT b,e,f FROM mv")
        assert_none(session, "SELECT f FROM mv")
        assert_none(session, "SELECT k,f FROM mv")
        assert_none(session, "SELECT e FROM mv")

        # bring row back to live by updating filter e=1@2
        self.update_view(session, "UPDATE t USING TIMESTAMP 2 SET e = 1 WHERE k = 1 and c = 1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 2, 2, 1, 1, 1])
        assert_one(session, "SELECT * FROM mv", [1, 1, 2, 2, 1, 1])
        assert_one(session, "SELECT k FROM mv", [1])
        assert_one(session, "SELECT c,a FROM mv", [1, 2])
        assert_one(session, "SELECT b,e,f FROM mv", [2, 1, 1])
        assert_one(session, "SELECT f FROM mv", [1])
        assert_one(session, "SELECT k,f FROM mv", [1, 1])
        assert_one(session, "SELECT e FROM mv", [1])

        # shadow row by using ttl=10 on unselected filter d=1
        self.update_view(session, "UPDATE t USING TTL 10 SET d = 1 WHERE k = 1 and c = 1;", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 2, 2, 1, 1, 1])
        assert_one(session, "SELECT * FROM mv", [1, 1, 2, 2, 1, 1])
        assert_one(session, "SELECT k FROM mv", [1])
        assert_one(session, "SELECT c,a FROM mv", [1, 2])
        assert_one(session, "SELECT b,e,f FROM mv", [2, 1, 1])
        assert_one(session, "SELECT f FROM mv", [1])
        assert_one(session, "SELECT k,f FROM mv", [1, 1])
        assert_one(session, "SELECT e FROM mv", [1])

        time.sleep(10)

        # unselected filter d expired
        assert_one(session, "SELECT * FROM t", [1, 1, 2, 2, None, 1, 1])
        assert_none(session, "SELECT * FROM mv")
        assert_none(session, "SELECT k FROM mv")
        assert_none(session, "SELECT c,a FROM mv")
        assert_none(session, "SELECT b,e,f FROM mv")
        assert_none(session, "SELECT f FROM mv")
        assert_none(session, "SELECT k,f FROM mv")
        assert_none(session, "SELECT e FROM mv")

        if flush:
            self.cluster.compact()
            assert_one(session, "SELECT * FROM t", [1, 1, 2, 2, None, 1, 1])
            assert_none(session, "SELECT * FROM mv")
            assert_none(session, "SELECT k FROM mv")
            assert_none(session, "SELECT c,a FROM mv")
            assert_none(session, "SELECT b,e,f FROM mv")
            assert_none(session, "SELECT f FROM mv")
            assert_none(session, "SELECT k,f FROM mv")
            assert_none(session, "SELECT e FROM mv")

    @since_dse('6.7')
    def test_mv_with_default_ttl_with_flush(self):
        self._test_mv_with_default_ttl(True)

    @since_dse('6.7')
    def test_mv_with_default_ttl_without_flush(self):
        self._test_mv_with_default_ttl(False)

    def _test_mv_with_default_ttl(self, flush):
        """
        Verify mv with default_time_to_live can be deleted properly using expired livenessInfo
        FIXME this would fail on legacy view, SEE CASSANDRA-14393
        @jira_ticket CASSANDRA-14071
        """
        session = self.prepare(rf=3, nodes=3, options={'hinted_handoff_enabled': False}, consistency_level=ConsistencyLevel.QUORUM)
        node1 = self.cluster.nodelist()[0]
        session.execute('USE ks')

        debug("MV with same key and unselected columns")
        session.execute("CREATE TABLE t2 (k int, a int, b int, c int, primary key(k, a)) with default_time_to_live=600")
        session.execute(("CREATE MATERIALIZED VIEW mv2 AS SELECT k,a,b FROM t2 "
                         "WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (a, k)"))
        session.cluster.control_connection.wait_for_schema_agreement()

        self.update_view(session, "UPDATE t2 SET c=1 WHERE k=1 AND a=1;", flush)
        assert_one(session, "SELECT k,a,b,c FROM t2", [1, 1, None, 1])
        assert_one(session, "SELECT k,a,b FROM mv2", [1, 1, None])

        self.update_view(session, "UPDATE t2 SET c=null WHERE k=1 AND a=1;", flush)
        assert_none(session, "SELECT k,a,b,c FROM t2")
        assert_none(session, "SELECT k,a,b FROM mv2")

        self.update_view(session, "UPDATE t2 SET c=2 WHERE k=1 AND a=1;", flush)
        assert_one(session, "SELECT k,a,b,c FROM t2", [1, 1, None, 2])
        assert_one(session, "SELECT k,a,b FROM mv2", [1, 1, None])

        self.update_view(session, "DELETE c FROM t2 WHERE k=1 AND a=1;", flush)
        assert_none(session, "SELECT k,a,b,c FROM t2")
        assert_none(session, "SELECT k,a,b FROM mv2")

        if flush:
            self.cluster.compact()
            assert_none(session, "SELECT * FROM t2")
            assert_none(session, "SELECT * FROM mv2")

        # test with user-provided ttl
        self.update_view(session, "INSERT INTO t2(k,a,b,c) VALUES(2,2,2,2) USING TTL 5", flush)
        self.update_view(session, "UPDATE t2 USING TTL 100 SET c=1 WHERE k=2 AND a=2;", flush)
        self.update_view(session, "UPDATE t2 USING TTL 50 SET c=2 WHERE k=2 AND a=2;", flush)
        self.update_view(session, "DELETE c FROM t2 WHERE k=2 AND a=2;", flush)

        time.sleep(5)

        assert_none(session, "SELECT k,a,b,c FROM t2")
        assert_none(session, "SELECT k,a,b FROM mv2")

        if flush:
            self.cluster.compact()
            assert_none(session, "SELECT * FROM t2")
            assert_none(session, "SELECT * FROM mv2")

        debug("MV with extra key")
        session.execute("CREATE TABLE t (k int PRIMARY KEY, a int, b int) with default_time_to_live=600")
        session.execute(("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t "
                         "WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (k, a)"))
        session.cluster.control_connection.wait_for_schema_agreement()

        self.update_view(session, "INSERT INTO t (k, a, b) VALUES (1, 1, 1);", flush)
        assert_one(session, "SELECT * FROM t", [1, 1, 1])
        assert_one(session, "SELECT * FROM mv", [1, 1, 1])

        self.update_view(session, "INSERT INTO t (k, a, b) VALUES (1, 2, 1);", flush)
        assert_one(session, "SELECT * FROM t", [1, 2, 1])
        assert_one(session, "SELECT * FROM mv", [1, 2, 1])

        self.update_view(session, "INSERT INTO t (k, a, b) VALUES (1, 3, 1);", flush)
        assert_one(session, "SELECT * FROM t", [1, 3, 1])
        assert_one(session, "SELECT * FROM mv", [1, 3, 1])

        if flush:
            self.cluster.compact()
            assert_one(session, "SELECT * FROM t", [1, 3, 1])
            assert_one(session, "SELECT * FROM mv", [1, 3, 1])

        # user provided ttl
        self.update_view(session, "UPDATE t USING TTL 50 SET a = 4 WHERE k = 1", flush)
        assert_one(session, "SELECT * FROM t", [1, 4, 1])
        assert_one(session, "SELECT * FROM mv", [1, 4, 1])

        self.update_view(session, "UPDATE t USING TTL 40 SET a = 5 WHERE k = 1", flush)
        assert_one(session, "SELECT * FROM t", [1, 5, 1])
        assert_one(session, "SELECT * FROM mv", [1, 5, 1])

        self.update_view(session, "UPDATE t USING TTL 30 SET a = 6 WHERE k = 1", flush)
        assert_one(session, "SELECT * FROM t", [1, 6, 1])
        assert_one(session, "SELECT * FROM mv", [1, 6, 1])

        if flush:
            self.cluster.compact()
            assert_one(session, "SELECT * FROM t", [1, 6, 1])
            assert_one(session, "SELECT * FROM mv", [1, 6, 1])


# For read verification
class MutationPresence(Enum):
    match = 1
    extra = 2
    missing = 3
    excluded = 4
    unknown = 5


class MM(object):
    mp = None

    def out(self):
        pass


class Match(MM):

    def __init__(self):
        self.mp = MutationPresence.match

    def out(self):
        return None


class Extra(MM):
    expecting = None
    value = None
    row = None

    def __init__(self, expecting, value, row):
        self.mp = MutationPresence.extra
        self.expecting = expecting
        self.value = value
        self.row = row

    def out(self):
        return "Extra. Expected {} instead of {}; row: {}".format(self.expecting, self.value, self.row)


class Missing(MM):
    value = None
    row = None

    def __init__(self, value, row):
        self.mp = MutationPresence.missing
        self.value = value
        self.row = row

    def out(self):
        return "Missing. At {}".format(self.row)


class Excluded(MM):

    def __init__(self):
        self.mp = MutationPresence.excluded

    def out(self):
        return None


class Unknown(MM):

    def __init__(self):
        self.mp = MutationPresence.unknown

    def out(self):
        return None


readConsistency = ConsistencyLevel.QUORUM
writeConsistency = ConsistencyLevel.QUORUM
SimpleRow = collections.namedtuple('SimpleRow', 'a b c d')


def row_generate(i, num_partitions):
    return SimpleRow(a=i % num_partitions, b=(i % 400) / num_partitions, c=i, d=i)


# Create a threaded session and execute queries from a Queue
def thread_session(ip, queue, start, end, rows, num_partitions):

    def execute_query(session, select_gi, i):
        row = row_generate(i, num_partitions)
        if (row.a, row.b) in rows:
            base = rows[(row.a, row.b)]
        else:
            base = -1
        gi = list(session.execute(select_gi, [row.c, row.a]))
        if base == i and len(gi) == 1:
            return Match()
        elif base != i and len(gi) == 1:
            return Extra(base, i, (gi[0][0], gi[0][1], gi[0][2], gi[0][3]))
        elif base == i and len(gi) == 0:
            return Missing(base, i)
        elif base != i and len(gi) == 0:
            return Excluded()
        else:
            return Unknown()

    try:
        cluster = Cluster([ip])
        session = cluster.connect()
        select_gi = session.prepare("SELECT * FROM mvtest.mv1 WHERE c = ? AND a = ?")
        select_gi.consistency_level = readConsistency

        for i in range(start, end):
            ret = execute_query(session, select_gi, i)
            queue.put_nowait(ret)
    except Exception as e:
        print str(e)
        queue.close()


@since('3.0')
@skipIf(sys.platform == 'win32', 'Bug in python on Windows: https://bugs.python.org/issue10128')
class TestMaterializedViewsConsistency(Tester):

    def prepare(self, user_table=False):
        cluster = self.cluster
        cluster.populate(3).start()
        node2 = cluster.nodelist()[1]

        # Keep the status of async requests
        self.exception_type = collections.Counter()
        self.num_request_done = 0
        self.counts = {}
        for mp in MutationPresence:
            self.counts[mp] = 0
        self.rows = {}
        self.update_stats_every = 100

        debug("Set to talk to node 2")
        self.session = self.patient_cql_connection(node2)

        return self.session

    def _print_write_status(self, row):
        output = "\r{}".format(row)
        for key in self.exception_type.keys():
            output = "{} ({}: {})".format(output, key, self.exception_type[key])
        sys.stdout.write(output)
        sys.stdout.flush()

    def _print_read_status(self, row):
        if self.counts[MutationPresence.unknown] == 0:
            sys.stdout.write(
                "\rOn {}; match: {}; extra: {}; missing: {}".format(
                    row,
                    self.counts[MutationPresence.match],
                    self.counts[MutationPresence.extra],
                    self.counts[MutationPresence.missing])
            )
        else:
            sys.stdout.write(
                "\rOn {}; match: {}; extra: {}; missing: {}; WTF: {}".format(
                    row,
                    self.counts[MutationPresence.match],
                    self.counts[MutationPresence.extra],
                    self.counts[MutationPresence.missing],
                    self.counts[MutationPresence.unkown])
            )
        sys.stdout.flush()

    def _do_row(self, insert_stmt, i, num_partitions):

        # Error callback for async requests
        def handle_errors(row, exc):
            self.num_request_done += 1
            try:
                name = type(exc).__name__
                self.exception_type[name] += 1
            except Exception as e:
                print traceback.format_exception_only(type(e), e)

        # Success callback for async requests
        def success_callback(row):
            self.num_request_done += 1

        if i % self.update_stats_every == 0:
            self._print_write_status(i)

        row = row_generate(i, num_partitions)

        async = self.session.execute_async(insert_stmt, row)
        errors = partial(handle_errors, row)
        async.add_callbacks(success_callback, errors)

    def _populate_rows(self):
        statement = SimpleStatement(
            "SELECT a, b, c FROM mvtest.test1",
            consistency_level=readConsistency
        )
        data = self.session.execute(statement)
        for row in data:
            self.rows[(row.a, row.b)] = row.c

    @skip('awaiting CASSANDRA-11290')
    def single_partition_consistent_reads_after_write_test(self):
        """
        Tests consistency of multiple writes to a single partition

        @jira_ticket CASSANDRA-10981
        """
        self._consistent_reads_after_write_test(1)

    @skip('hangs CI - CSTAR-905')
    def multi_partition_consistent_reads_after_write_test(self):
        """
        Tests consistency of multiple writes to a multiple partitions

        @jira_ticket CASSANDRA-10981
        """
        self._consistent_reads_after_write_test(20)

    def _consistent_reads_after_write_test(self, num_partitions):

        session = self.prepare()
        node1, node2, node3 = self.cluster.nodelist()

        # Test config
        lower = 0
        upper = 100000
        processes = 4
        queues = [None] * processes
        eachProcess = (upper - lower) / processes

        debug("Creating schema")
        session.execute(
            ("CREATE KEYSPACE IF NOT EXISTS mvtest WITH replication = "
             "{'class': 'SimpleStrategy', 'replication_factor': '3'}")
        )
        session.execute(
            "CREATE TABLE mvtest.test1 (a int, b int, c int, d int, PRIMARY KEY (a,b))"
        )
        session.cluster.control_connection.wait_for_schema_agreement()

        insert1 = session.prepare("INSERT INTO mvtest.test1 (a,b,c,d) VALUES (?,?,?,?)")
        insert1.consistency_level = writeConsistency

        debug("Writing data to base table")
        for i in range(upper / 10):
            self._do_row(insert1, i, num_partitions)

        debug("Creating materialized view")
        session.execute(
            ('CREATE MATERIALIZED VIEW mvtest.mv1 AS '
             'SELECT a,b,c,d FROM mvtest.test1 WHERE a IS NOT NULL AND b IS NOT NULL AND '
             'c IS NOT NULL PRIMARY KEY (c,a,b)')
        )
        session.cluster.control_connection.wait_for_schema_agreement()

        debug("Writing more data to base table")
        for i in range(upper / 10, upper):
            self._do_row(insert1, i, num_partitions)

        # Wait that all requests are done
        while self.num_request_done < upper:
            time.sleep(1)

        debug("Making sure all batchlogs are replayed on node1")
        node1.nodetool("replaybatchlog")
        debug("Making sure all batchlogs are replayed on node2")
        node2.nodetool("replaybatchlog")
        debug("Making sure all batchlogs are replayed on node3")
        node3.nodetool("replaybatchlog")

        debug("Finished writes, now verifying reads")
        self._populate_rows()

        for i in range(processes):
            start = lower + (eachProcess * i)
            if i == processes - 1:
                end = upper
            else:
                end = lower + (eachProcess * (i + 1))
            q = Queue()
            node_ip = get_ip_from_node(node2)
            p = Process(target=thread_session, args=(node_ip, q, start, end, self.rows, num_partitions))
            p.start()
            queues[i] = q

        for i in range(lower, upper):
            if i % 100 == 0:
                self._print_read_status(i)
            mm = queues[i % processes].get()
            if not mm.out() is None:
                sys.stdout.write("\r{}\n" .format(mm.out()))
            self.counts[mm.mp] += 1

        self._print_read_status(upper)
        sys.stdout.write("\n")
        sys.stdout.flush()


@since('3.0')
class TestMaterializedViewsLockcontention(Tester):
    """
    Test materialized views lock contention.
    @jira_ticket CASSANDRA-12689
    @since 3.0
    """

    def _prepare_cluster(self):
        self.cluster.populate(1)
        self.supports_v5_protocol = supports_v5_protocol(self.cluster.version())
        self.protocol_version = 5 if self.supports_v5_protocol else 4

        if self.cluster.version() < '4.0':
            self.cluster.set_configuration_options(values={
                'concurrent_materialized_view_writes': 2,
                'concurrent_writes': 2,
            })
        self.nodes = self.cluster.nodes.values()
        for node in self.nodes:
            remove_perf_disable_shared_mem(node)

        if self.cluster.version() < '4.0':
            self.cluster.start(wait_for_binary_proto=True, jvm_args=["-Dcassandra.test.fail_mv_locks_count=64"])
        else:
            self.cluster.start(wait_for_binary_proto=True)

        session = self.patient_exclusive_cql_connection(self.nodes[0], protocol_version=self.protocol_version)

        keyspace = "locktest"
        session.execute("""
                CREATE KEYSPACE IF NOT EXISTS {}
                WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }}
                """.format(keyspace))
        session.set_keyspace(keyspace)

        session.execute(
            "CREATE TABLE IF NOT EXISTS test (int1 int, int2 int, date timestamp, PRIMARY KEY (int1, int2))")
        session.execute("""CREATE MATERIALIZED VIEW test_sorted_mv AS
        SELECT int1, date, int2
        FROM test
        WHERE int1 IS NOT NULL AND date IS NOT NULL AND int2 IS NOT NULL
        PRIMARY KEY (int1, date, int2)
        WITH CLUSTERING ORDER BY (date DESC, int1 DESC)""")

        return session

    @since('3.0')
    def test_mutations_dontblock(self):
        session = self._prepare_cluster()
        records = 100
        records2 = 100
        params = []
        for x in xrange(records):
            for y in xrange(records2):
                params.append([x, y])

        execute_concurrent_with_args(
            session,
            session.prepare('INSERT INTO test (int1, int2, date) VALUES (?, ?, toTimestamp(now()))'),
            params
        )

        assert_one(session, "SELECT count(*) FROM test WHERE int1 = 1", [records2])

        # 4.0 would throw WouldBlockException
        if self.cluster.version() < '4.0':
            for node in self.nodes:
                with JolokiaAgent(node) as jmx:
                    mutationStagePending = jmx.read_attribute(
                        make_mbean('metrics', type="ThreadPools", path='request', scope='MutationStage', name='PendingTasks'), "Value"
                    )
                    assert_equal(0, mutationStagePending, "Pending mutations: {}".format(mutationStagePending))
