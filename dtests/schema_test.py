import datetime
import time

from dse import ConsistencyLevel
from dse.concurrent import execute_concurrent_with_args
from dse.query import SimpleStatement
from nose.plugins.attrib import attr

from dtest import DSE_VERSION_FROM_BUILD, Tester, debug
from tools.assertions import (assert_all, assert_invalid, assert_none,
                              assert_one, assert_some)
from tools.data import create_cf, create_ks
from tools.decorators import since


class TestSchema(Tester):

    @attr("smoke-test")
    def table_alteration_test(self):
        """
        Tests that table alters return as expected with many sstables at different schema points
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1, = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        session.execute("use ks;")
        session.execute("create table tbl_o_churn (id int primary key, c0 text, c1 text) "
                        "WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'min_threshold': 1024, 'max_threshold': 1024 };")

        stmt1 = session.prepare("insert into tbl_o_churn (id, c0, c1) values (?, ?, ?)")
        rows_to_insert = 50

        for n in range(5):
            parameters = [(x, 'aaa', 'bbb') for x in range(n * rows_to_insert, (n * rows_to_insert) + rows_to_insert)]
            execute_concurrent_with_args(session, stmt1, parameters, concurrency=rows_to_insert)
            node1.flush()

        session.execute("alter table tbl_o_churn add c2 text")
        session.execute("alter table tbl_o_churn drop c0")
        stmt2 = session.prepare("insert into tbl_o_churn (id, c1, c2) values (?, ?, ?);")

        for n in range(5, 10):
            parameters = [(x, 'ccc', 'ddd') for x in range(n * rows_to_insert, (n * rows_to_insert) + rows_to_insert)]
            execute_concurrent_with_args(session, stmt2, parameters, concurrency=rows_to_insert)
            node1.flush()

        rows = session.execute("select * from tbl_o_churn")
        for row in rows:
            if row.id < rows_to_insert * 5:
                self.assertEqual(row.c1, 'bbb')
                self.assertIsNone(row.c2)
                self.assertFalse(hasattr(row, 'c0'))
            else:
                self.assertEqual(row.c1, 'ccc')
                self.assertEqual(row.c2, 'ddd')
                self.assertFalse(hasattr(row, 'c0'))

    @attr("smoke-test")
    @since("2.0", max_version="3.X")  # Compact Storage
    def drop_column_compact_test(self):
        session = self.prepare()

        session.execute("USE ks")
        session.execute("CREATE TABLE cf (key int PRIMARY KEY, c1 int, c2 int) WITH COMPACT STORAGE")

        assert_invalid(session, "ALTER TABLE cf DROP c1", "Cannot drop columns from a")

    @attr("smoke-test")
    def drop_column_compaction_test(self):
        session = self.prepare()
        session.execute("USE ks")
        session.execute("CREATE TABLE cf (key int PRIMARY KEY, c1 int, c2 int)")

        # insert some data.
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (0, 1, 2)")
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (1, 2, 3)")
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (2, 3, 4)")

        # drop and readd c1.
        session.execute("ALTER TABLE cf DROP c1")
        session.execute("ALTER TABLE cf ADD c1 int")

        # add another row.
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (3, 4, 5)")

        node = self.cluster.nodelist()[0]
        node.flush()
        node.compact()

        # test that c1 values have been compacted away.
        session = self.patient_cql_connection(node)
        assert_all(session, "SELECT c1 FROM ks.cf", [[None], [None], [None], [4]], ignore_order=True)

    @attr("smoke-test")
    def drop_column_queries_test(self):
        session = self.prepare()

        session.execute("USE ks")
        session.execute("CREATE TABLE cf (key int PRIMARY KEY, c1 int, c2 int)")
        session.execute("CREATE INDEX ON cf(c2)")

        # insert some data.
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (0, 1, 2)")
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (1, 2, 3)")
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (2, 3, 4)")

        # drop and readd c1.
        session.execute("ALTER TABLE cf DROP c1")
        session.execute("ALTER TABLE cf ADD c1 int")

        # add another row.
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (3, 4, 5)")

        # test that old (pre-drop) c1 values aren't returned and new ones are.
        assert_all(session, "SELECT c1 FROM cf", [[None], [None], [None], [4]], ignore_order=True)

        assert_all(session, "SELECT * FROM cf", [[0, None, 2], [1, None, 3], [2, None, 4], [3, 4, 5]], ignore_order=True)

        assert_one(session, "SELECT c1 FROM cf WHERE key = 0", [None])

        assert_one(session, "SELECT c1 FROM cf WHERE key = 3", [4])

        assert_one(session, "SELECT * FROM cf WHERE c2 = 2", [0, None, 2])

        assert_one(session, "SELECT * FROM cf WHERE c2 = 5", [3, 4, 5])

    @attr("smoke-test")
    def drop_column_and_restart_test(self):
        """
        Simply insert data in a table, drop a column involved in the insert and restart the node afterwards.
        This ensures that the dropped_columns system table is properly flushed on the alter or the restart
        fails as in CASSANDRA-11050.

        @jira_ticket CASSANDRA-11050
        """
        session = self.prepare()

        session.execute("USE ks")
        session.execute("CREATE TABLE t (k int PRIMARY KEY, c1 int, c2 int)")

        session.execute("INSERT INTO t (k, c1, c2) VALUES (0, 0, 0)")
        session.execute("ALTER TABLE t DROP c2")

        assert_one(session, "SELECT * FROM t", [0, 0])

        self.cluster.stop()
        self.cluster.start()

        session = self.patient_cql_connection(self.cluster.nodelist()[0])

        session.execute("USE ks")
        assert_one(session, "SELECT * FROM t", [0, 0])

    @attr("smoke-test")
    def drop_static_column_and_restart_test(self):
        """
        Dropping a static column caused an sstable corrupt exception after restarting, here
        we test that we can drop a static column and restart safely.

        @jira_ticket CASSANDRA-12582
        """
        session = self.prepare()

        session.execute("USE ks")
        session.execute("CREATE TABLE ts (id1 int, id2 int, id3 int static, val text, PRIMARY KEY (id1, id2))")

        session.execute("INSERT INTO ts (id1, id2, id3, val) VALUES (1, 1, 0, 'v1')")
        session.execute("INSERT INTO ts (id1, id2, id3, val) VALUES (1, 2, 0, 'v2')")
        session.execute("INSERT INTO ts (id1, id2, id3, val) VALUES (2, 1, 1, 'v3')")

        self.cluster.nodelist()[0].nodetool('flush ks ts')
        assert_all(session, "SELECT * FROM ts", [[1, 1, 0, 'v1'], [1, 2, 0, 'v2'], [2, 1, 1, 'v3']])

        session.execute("alter table ts drop id3")
        assert_all(session, "SELECT * FROM ts", [[1, 1, 'v1'], [1, 2, 'v2'], [2, 1, 'v3']])

        self.cluster.stop()
        self.cluster.start()

        session = self.patient_cql_connection(self.cluster.nodelist()[0])

        session.execute("USE ks")
        assert_all(session, "SELECT * FROM ts", [[1, 1, 'v1'], [1, 2, 'v2'], [2, 1, 'v3']])

    @since('3.0')
    def drop_table_reflected_in_size_estimates_test(self):
        """
        A dropped table should result in its entries being removed from size estimates, on both
        nodes that are up and down at the time of the drop.

        @jira_ticket DB-913
        """
        cluster = self.cluster
        cluster.populate(2).start()
        node1, node2 = cluster.nodelist()
        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, 'ks1', 2)
        create_ks(session, 'ks2', 2)
        create_cf(session, 'ks1.cf1', columns={'c1': 'text', 'c2': 'text'})
        create_cf(session, 'ks2.cf1', columns={'c1': 'text', 'c2': 'text'})
        create_cf(session, 'ks2.cf2', columns={'c1': 'text', 'c2': 'text'})

        node1.nodetool('refreshsizeestimates')
        node2.nodetool('refreshsizeestimates')
        node2.stop()
        session.execute('DROP TABLE ks2.cf1')
        session.execute('DROP KEYSPACE ks1')
        node2.start(wait_for_binary_proto=True)
        session2 = self.patient_exclusive_cql_connection(node2)

        session.cluster.control_connection.wait_for_schema_agreement()

        assert_none(session, "SELECT * FROM system.size_estimates WHERE keyspace_name='ks1'")
        assert_none(session, "SELECT * FROM system.size_estimates WHERE keyspace_name='ks2' AND table_name='cf1'")
        assert_some(session, "SELECT * FROM system.size_estimates WHERE keyspace_name='ks2' AND table_name='cf2'")
        assert_none(session2, "SELECT * FROM system.size_estimates WHERE keyspace_name='ks1'")
        assert_none(session2, "SELECT * FROM system.size_estimates WHERE keyspace_name='ks2' AND table_name='cf1'")
        assert_some(session, "SELECT * FROM system.size_estimates WHERE keyspace_name='ks2' AND table_name='cf2'")

    @since('3.0')
    def invalid_entries_removed_from_size_estimates_on_restart_test(self):
        """
        Entries for dropped tables/keyspaces should be cleared from size_estimates on restart.

        @jira_ticket DB-913
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)
        session.execute("USE system;")
        session.execute("INSERT INTO size_estimates (keyspace_name, table_name, range_start, range_end, mean_partition_size, partitions_count) VALUES ( 'system_auth', 'bad_table', '-5', '5', 0, 0);")
        # Invalid keyspace and table
        session.execute("INSERT INTO size_estimates (keyspace_name, table_name, range_start, range_end, mean_partition_size, partitions_count) VALUES ( 'bad_keyspace', 'bad_table', '-5', '5', 0, 0);")
        node.stop()
        node.start()
        session = self.patient_cql_connection(node)
        assert_none(session, "SELECT * FROM system.size_estimates WHERE keyspace_name='system_auth' AND table_name='bad_table'")
        assert_none(session, "SELECT * FROM system.size_estimates WHERE keyspace_name='bad_keyspace'")

    @since('3.0')
    def schema_migration_concurrent_requests_test(self):
        """
        Simulates schema-changes with concurrent reads + writes.
        When the schema-change is not fully propagated but reads + writes against "previously" known columns still happen.

        The test basically simulates a schema change (an ALTER TABLE ADD) in a live cluster, where schema-migrations
        naturally take some time to propagate. DML statements (SELECT, INSERT; UPDATE, DELETE) must succeed in any
        case. To verify that, we "halt" schema migration on the nodes using a byteman script.

        Without the fix for DB-1649, this dtest errors out with a node logging unexpected errors like:
            java.lang.RuntimeException: Unknown column add_s_tiny during deserialization
                at org.apache.cassandra.db.Columns$Serializer.deserialize(Columns.java:452)
                at org.apache.cassandra.db.filter.ColumnFilter$Serializer.deserialize(ColumnFilter.java:504)
                at org.apache.cassandra.db.ReadCommand$Serializer.deserialize(ReadCommand.java:801)
                at org.apache.cassandra.db.ReadCommand$Serializer.deserialize(ReadCommand.java:738)

        This test uses ConsistencyLevel.ALL to ensure that both nodes are involved in the operations.

        @jira_ticket DB-1649
        """
        cluster = self.cluster
        cluster.populate(2, install_byteman=True).start()
        node1, node2 = cluster.nodelist()

        session1, session2 = [self.patient_exclusive_cql_connection(node, schema_timeout=2) for node in (node1, node2)]

        debug("Creating schema...")

        create_ks(session1, 'ks', 2)
        session1.execute("CREATE TABLE ks.complex_table ("
                         "pk int,"
                         "ck int,"
                         "s_tiny tinyint static,"
                         "s_int int static,"
                         "s_text text static,"
                         "s_map map<text, text> static,"
                         "s_timestamp timestamp static,"
                         "r_tiny tinyint,"
                         "r_int int,"
                         "r_text text,"
                         "r_map map<text, text>,"
                         "r_timestamp timestamp,"
                         "PRIMARY KEY (pk, ck))")

        debug("Preparing statements")

        non_prepared_insert = "INSERT INTO ks.complex_table(pk, ck," \
                              "s_tiny, s_int, s_text, s_map, s_timestamp," \
                              "r_tiny, r_int, r_text, r_map, r_timestamp) " \
                              "VALUES ({}, {}, " \
                              "{}, {}, {}, {}, {}, " \
                              "{}, {}, {}, {}, {})"
        prepared_insert = non_prepared_insert.format("?", "?",
                                                     "?", "?", "?", "?", "?",
                                                     "?", "?", "?", "?", "?")
        non_prepared_select = "SELECT pk, ck, " \
                              "s_tiny, s_int, s_text, s_map, s_timestamp," \
                              "r_tiny, r_int, r_text, r_map, r_timestamp " \
                              "FROM ks.complex_table WHERE pk = {}"
        prepared_select = non_prepared_select.format("?")

        non_prepared_delete = "DELETE FROM ks.complex_table WHERE pk={}"
        prepared_delete = non_prepared_delete.format("?")

        pstmt_insert1 = session1.prepare(prepared_insert)
        pstmt_select1 = session1.prepare(prepared_select)
        pstmt_delete1 = session1.prepare(prepared_delete)
        pstmt_insert2 = session2.prepare(prepared_insert)
        pstmt_select2 = session2.prepare(prepared_select)
        pstmt_delete2 = session2.prepare(prepared_delete)
        for pstmt in (pstmt_insert1, pstmt_insert2, pstmt_select1, pstmt_select2, pstmt_delete1, pstmt_delete2):
            pstmt.consistency_level = ConsistencyLevel.ALL

        schema_version_1 = session1.execute("SELECT schema_version FROM system.local")[0][0]
        schema_version_2 = session2.execute("SELECT schema_version FROM system.local")[0][0]
        self.assertEqual(schema_version_1, schema_version_2)

        debug("Insert, verify, delete...")

        self._schema_migration_concurrent_requests_insert(node1, session1, non_prepared_insert, pstmt_insert1)
        self._schema_migration_concurrent_requests_verify(node1, session1, non_prepared_select, pstmt_select1)
        self._schema_migration_concurrent_requests_delete(node1, session1, non_prepared_delete, pstmt_delete1)
        self._schema_migration_concurrent_requests_insert(node2, session2, non_prepared_insert, pstmt_insert2)
        self._schema_migration_concurrent_requests_verify(node2, session2, non_prepared_select, pstmt_select2)
        self._schema_migration_concurrent_requests_delete(node2, session2, non_prepared_delete, pstmt_delete2)

        self._schema_migration_concurrent_requests_check_errors(msg="after initial insert-verify-delete")

        debug("columns from 1: {}".format([row[0] for row in session1.execute("SELECT column_name FROM system_schema.columns WHERE keyspace_name='ks' AND table_name='complex_table'")]))
        debug("columns from 2: {}".format([row[0] for row in session2.execute("SELECT column_name FROM system_schema.columns WHERE keyspace_name='ks' AND table_name='complex_table'")]))

        debug("Injecting byteman script to prevent processing of schema-migrations")
        if DSE_VERSION_FROM_BUILD >= '6.0':
            script = ['./byteman/dse6.0/prevent_schema_migration.btm']
        else:
            script = ['./byteman/pre4.0/prevent_schema_migration.btm']
        [node.byteman_submit(script) for node in (node1, node2)]

        for column, ddl in [["add_s_tiny", "ALTER TABLE ks.complex_table ADD add_s_tiny tinyint static"],
                            ["add_s_tiny", "ALTER TABLE ks.complex_table ADD add_s_int int static"],
                            ["add_s_tiny", "ALTER TABLE ks.complex_table ADD add_s_text text static"],
                            ["add_s_tiny", "ALTER TABLE ks.complex_table ADD add_s_map map<text, text> static"],
                            ["add_s_tiny", "ALTER TABLE ks.complex_table ADD add_s_timestamp timestamp static"],
                            ["add_s_tiny", "ALTER TABLE ks.complex_table ADD add_r_tiny tinyint"],
                            ["add_s_tiny", "ALTER TABLE ks.complex_table ADD add_r_int int"],
                            ["add_s_tiny", "ALTER TABLE ks.complex_table ADD add_r_text text"],
                            ["add_s_tiny", "ALTER TABLE ks.complex_table ADD add_r_map map<text, text>"],
                            ["add_s_tiny", "ALTER TABLE ks.complex_table ADD add_r_timestamp timestamp"]]:
            debug("Altering table (adding column) ... {}".format(ddl))
            session1.execute(ddl)

            schema_version_1_ddl = session1.execute("SELECT schema_version FROM system.local")[0][0]
            schema_version_2_ddl = session2.execute("SELECT schema_version FROM system.local")[0][0]
            self.assertNotEqual(schema_version_1_ddl, schema_version_2_ddl)
            self.assertEqual(schema_version_2, schema_version_2_ddl)

            columns_1 = self._schema_migration_concurrent_requests_columns(session1)
            columns_2 = self._schema_migration_concurrent_requests_columns(session2)
            debug("columns from {}: {}".format(node1.name, columns_1))
            debug("columns from {}: {}".format(node2.name, columns_2))
            self.assertIn(column, columns_1)
            self.assertNotIn(column, columns_2)

            debug("Insert, verify, delete against {} - not having updated schema...".format(node2.name))

            self._schema_migration_concurrent_requests_insert(node2, session2, non_prepared_insert, pstmt_insert2)
            self._schema_migration_concurrent_requests_verify(node2, session2, non_prepared_select, pstmt_select2)
            self._schema_migration_concurrent_requests_verify(node1, session1, non_prepared_select, pstmt_select1)
            self._schema_migration_concurrent_requests_delete(node2, session2, non_prepared_delete, pstmt_delete2)

            debug("Insert, verify, delete against {} - with updated schema...".format(node1.name))

            self._schema_migration_concurrent_requests_insert(node1, session1, non_prepared_insert, pstmt_insert1)
            self._schema_migration_concurrent_requests_verify(node2, session2, non_prepared_select, pstmt_select2)
            self._schema_migration_concurrent_requests_verify(node1, session1, non_prepared_select, pstmt_select1)
            self._schema_migration_concurrent_requests_delete(node1, session1, non_prepared_delete, pstmt_delete1)

    def _schema_migration_concurrent_requests_check_errors(self, msg=''):
        if self.check_logs_for_errors():
            self.fail('Unexpected error in log ({}), see stdout'.format(msg))

    def _schema_migration_concurrent_requests_columns(self, session):
        return [row[0] for row in session.execute("SELECT column_name FROM system_schema.columns WHERE keyspace_name='ks' AND table_name='complex_table'")]

    def _schema_migration_concurrent_requests_insert(self, node, session, non_prepared_insert, pstmt_insert):
        debug("Insert rows against {}...".format(node.name))
        for i in range(0, 20):
            cql = non_prepared_insert.format(i, i,
                                             i, i, u"'{}'".format(i), "{{'{}': '{}'}}".format(i, i), i * 1000,
                                             i, i, u"'{}'".format(i), "{{'{}': '{}'}}".format(i, i), i * 1000)
            session.execute(SimpleStatement(cql, consistency_level=ConsistencyLevel.ALL))
        self._schema_migration_concurrent_requests_check_errors(msg="after insert w/ unprepared statement against {}".format(node.name))
        for i in range(20, 40):
            i_as_str = str(i)
            session.execute(pstmt_insert, (i, i,
                                           i, i, i_as_str, {i_as_str: i_as_str}, i * 1000,
                                           i, i, i_as_str, {i_as_str: i_as_str}, i * 1000))
        self._schema_migration_concurrent_requests_check_errors(msg="after insert w/ prepared statement against {}".format(node.name))

    def _schema_migration_concurrent_requests_verify(self, node, session, non_prepared_select, pstmt_select):
        debug("Verifying rows against {}...".format(node.name))
        for i in range(0, 40):
            assert_one(session, non_prepared_select.format(i), cl=ConsistencyLevel.ALL,
                       expected=self._schema_migration_concurrent_requests_row(i))
        self._schema_migration_concurrent_requests_check_errors(msg="after verify w/ unprepared statement against {}".format(node.name))
        for i in range(0, 40):
            rows = session.execute(pstmt_select, [i])
            expected = self._schema_migration_concurrent_requests_row(i)
            self.assertEqual([list(row) for row in rows], [expected])
        self._schema_migration_concurrent_requests_check_errors(msg="after verify w/ prepared statement against {}".format(node.name))

    def _schema_migration_concurrent_requests_delete(self, node, session, non_prepared_delete, pstmt_delete):
        debug("Deleting rows against {}".format(node.name))
        for i in range(0, 20):
            session.execute(SimpleStatement(non_prepared_delete.format(i), consistency_level=ConsistencyLevel.ALL))
        self._schema_migration_concurrent_requests_check_errors(msg="after delete w/ unprepared statement against {}".format(node.name))
        for i in range(20, 40):
            session.execute(pstmt_delete, [i])
        self._schema_migration_concurrent_requests_check_errors(msg="after delete w/ prepared statement against {}".format(node.name))

    def _schema_migration_concurrent_requests_row(self, i):
        i_as_str = str(i)
        map = {i_as_str: i_as_str}
        dt = datetime.datetime.utcfromtimestamp(i)
        return [i, i,
                i, i, i_as_str, map, dt,
                i, i, i_as_str, map, dt]

    @since('3.11')
    def add_column_with_UDT_type_then_drop_column_test(self):
        """
        @jira_ticket DB-1850
        """
        session = self.prepare()
        node = self.cluster.nodelist()[0]

        session.execute("USE ks")
        session.execute("CREATE TYPE custom_type (name text, age int)")
        session.execute("CREATE TABLE ts (id int, s custom_type, c int, PRIMARY KEY (id, c))")

        session.execute("INSERT INTO ts (id, c, s) VALUES (0, 10, {name: 'test', age: 10})")
        session.execute("INSERT INTO ts (id, c, s) VALUES (1, 10, {name: 'test', age: 20})")

        node.nodetool('flush ks ts')

        session.execute("ALTER TABLE ts drop s")
        self._check_data_is_correct_after_compaction([[1, 10], [0, 10]])

    @since('3.11')
    def add_column_with_frozen_UDT_type_then_drop_column_test(self):
        """
        @jira_ticket DB-1850
        """
        session = self.prepare()
        node = self.cluster.nodelist()[0]

        session.execute("USE ks")
        session.execute("CREATE TYPE custom_type (name text)")
        session.execute("CREATE TABLE ts (id int, s frozen<custom_type>, c int, PRIMARY KEY (id, c))")

        session.execute("INSERT INTO ts (id, c, s) VALUES (0, 10, {name: 'test'})")
        session.execute("INSERT INTO ts (id, c, s) VALUES (1, 10, {name: 'test'})")

        node.nodetool('flush ks ts')

        session.execute("ALTER TABLE ts drop s")
        self._check_data_is_correct_after_compaction([[1, 10], [0, 10]])

    @since('3.11')
    def add_column_with_nested_UDT_type_then_drop_column_test(self):
        """
        @jira_ticket DB-1850
        """
        session = self.prepare()
        node = self.cluster.nodelist()[0]

        session.execute("USE ks")
        session.execute("CREATE TYPE nested_custom_type (name text, age int)")
        session.execute("CREATE TYPE custom_type (tp frozen<nested_custom_type>, value text)")
        session.execute("CREATE TABLE ts (id int, s custom_type, c int, PRIMARY KEY (id, c))")

        session.execute("INSERT INTO ts (id, c, s) VALUES (0, 10, {tp: {name: 'test', age: 10}, value: 'aaa'})")
        session.execute("INSERT INTO ts (id, c, s) VALUES (1, 10, {tp: {name: 'test', age: 20}, value: 'eee'})")

        node.nodetool('flush ks ts')

        session.execute("ALTER TABLE ts drop s")
        self._check_data_is_correct_after_compaction([[1, 10], [0, 10]])

    @since('3.11')
    def add_column_with_collection_UDT_type_then_drop_column_test(self):
        """
        @jira_ticket DB-1850
        """
        session = self.prepare()
        node = self.cluster.nodelist()[0]

        session.execute("USE ks")
        session.execute("CREATE TYPE custom_type (name text, age int)")
        session.execute("CREATE TABLE ts (id int, s list<frozen<custom_type>>, c int, PRIMARY KEY (id, c))")

        session.execute("INSERT INTO ts (id, c, s) VALUES (0, 10, [{name: 'test', age: 10}])")
        session.execute("INSERT INTO ts (id, c, s) VALUES (1, 10, [{name: 'test', age: 20}])")

        node.nodetool('flush ks ts')

        session.execute("ALTER TABLE ts drop s")
        self._check_data_is_correct_after_compaction([[1, 10], [0, 10]])

    def prepare(self):
        cluster = self.cluster
        cluster.populate(1).start()
        time.sleep(.5)
        nodes = cluster.nodelist()
        session = self.patient_cql_connection(nodes[0])
        create_ks(session, 'ks', 1)
        return session

    def _assert_correct(self, node, expected_data, custom_type=None):
        session = self.patient_cql_connection(node)
        if custom_type:
            session.cluster.register_user_type('ks', 'custom_type', custom_type)
        assert_all(session, "SELECT * FROM ks.ts", expected_data)

    def _check_data_is_correct_after_compaction(self, expected_data, custom_type=None):
        node = self.cluster.nodelist()[0]
        node.nodetool('flush ks ts')
        node.stop()
        node.start()

        self._assert_correct(node, expected_data, custom_type)

        node.nodetool('compact ks ts')

        node.run_sstabledump(keyspace='ks', column_families=['ts'])

        self._assert_correct(node, expected_data, custom_type)

        node.stop()
        node.start()

        self._assert_correct(node, expected_data, custom_type)
