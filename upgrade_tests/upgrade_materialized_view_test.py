from dse import (ConsistencyLevel as CL, InvalidRequest)
from dtests.dtest import Tester, debug
from tools.assertions import assert_one, assert_none
from tools.data import create_ks
from tools.decorators import since_dse
from tools.jmxutils import (JolokiaAgent, make_mbean,
                            remove_perf_disable_shared_mem)


class UpgradeMaterializedViewTest(Tester):

    def prepare(self, version, rf=1, options=None, nodes=3, install_byteman=False, jmx=False, jvm_args=[], **kwargs):
        # not an error
        self.ignore_log_patterns = [r'did not send a value for the graph field']
        self.default_install_dir = self.cluster.get_install_dir()
        cluster = self.cluster
        cluster.set_install_dir(version=version)
        cluster.populate(nodes, install_byteman=install_byteman)
        if options:
            cluster.set_configuration_options(values=options)

        if jmx:
            for n in self.cluster.nodelist():
                remove_perf_disable_shared_mem(n)
        cluster.start(jvm_args=jvm_args)
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1, consistency_level=CL.ALL, **kwargs)
        create_ks(session, 'ks', rf)

        return session

    @since_dse('6.7')
    def test_view_creation_on_mixed_version_cluster(self):
        """
        Tests view creation on mixed version:
            1. if the coordinator is upgraded, it should block until entire cluster is upgraded
            2. if the coordinator is not upgraded, it will create view as legacy schema

        @jira_ticket DB-1060
        """
        version = "alias:bdp/6.0-dev"
        debug("populate cluster with version: {}".format(version))
        session = self.prepare(version=version, rf=2, nodes=2)
        cluster = self.cluster
        node1, node2 = cluster.nodelist()
        rows = 100

        debug("populate base table schema")
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute(("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t WHERE v IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v, id)"))
        session.cluster.control_connection.wait_for_schema_agreement()

        def populate_data(session, rows):
            debug("populate data")
            for i in xrange(rows):
                session.execute("INSERT INTO t (id, v) VALUES ({v}, {v})".format(v=i))
            for i in xrange(rows):
                session.execute("UPDATE t SET v = {} WHERE id = {}".format(i + 1, i))

        def verify_data(session, rows, view="mv"):
            debug("verify data")
            for i in xrange(rows):
                assert_one(session, "SELECT * FROM {} WHERE v = {}".format(view, i + 1), [i + 1, i])
                assert_one(session, "SELECT v,id FROM {} WHERE v = {}".format(view, i + 1), [i + 1, i])

        populate_data(session, rows)
        verify_data(session, rows)

        debug("upgrade {} to latest version {}".format(node1.name, self.default_install_dir))
        self.upgrade_to_version(self.default_install_dir, node1)

        session1 = self.patient_exclusive_cql_connection(node1, consistency_level=CL.ALL, keyspace="ks")
        verify_data(session1, rows)

        debug("create view on upgraded node, expect InvalidRequestt")
        with self.assertRaises(InvalidRequest):
            session1.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL "
                              "AND id IS NOT NULL PRIMARY KEY (v, id)"))
        session1.cluster.shutdown()

        debug("create view on non-upgraded node")
        session2 = self.patient_exclusive_cql_connection(node2, consistency_level=CL.ALL, keyspace="ks")
        session2.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL "
                          "AND id IS NOT NULL PRIMARY KEY (v, id)"))
        session2.cluster.shutdown()

        debug("upgrade {} to latest version {}".format(node2.name, self.default_install_dir))
        self.upgrade_to_version(self.default_install_dir, node2)
        session2 = self.patient_exclusive_cql_connection(node2, consistency_level=CL.ALL, keyspace="ks")
        verify_data(session2, rows)
        verify_data(session2, rows, view="t_by_v")

    @since_dse('6.7')
    def test_upgrade_with_required_column_from_6_0(self):
        self._test_perform_sstable_operation_with_required_column(lambda node: self.upgrade_sstable(node), "alias:bdp/6.0-dev")

    @since_dse('6.7')
    def test_compact_with_required_column_from_6_0(self):
        self._test_perform_sstable_operation_with_required_column(lambda node: self.compact_sstable(node), "alias:bdp/6.0-dev")

    def _test_perform_sstable_operation_with_required_column(self, sstable_operation, version):
        """
        Tests upgrade between 6.0 and DSE-6.7.0 to verify materialized view backward compatibility with required column

        @jira_ticket DB-1060
        """
        debug("populate cluster with version: {}".format(version))
        session = self.prepare(version=version, rf=3, nodes=3)
        cluster = self.cluster
        node1, node2, node3 = cluster.nodelist()

        debug("populate legacy schema")
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT * FROM t WHERE v IS NOT NULL "
                         "AND id IS NOT NULL PRIMARY KEY (v, id)"))
        session.cluster.control_connection.wait_for_schema_agreement()

        rows = 100

        def populate_data(session, rows):
            debug("populate data")
            for i in xrange(rows):
                session.execute("INSERT INTO t (id, v) VALUES ({v}, {v})".format(v=i))
            for i in xrange(rows):
                session.execute("UPDATE t SET v = {} WHERE id = {}".format(i + 1, i))

        populate_data(session, rows)
        self.cluster.flush()

        def verify_data(session, rows):
            debug("verify data")
            for i in xrange(rows):
                assert_one(session, "SELECT * FROM t_by_v WHERE v = {}".format(i + 1), [i + 1, i])
                assert_one(session, "SELECT v,id FROM t_by_v WHERE v = {}".format(i + 1), [i + 1, i])

        self.do_rolling_upgrade(session, rows, verify_data, sstable_operation)

        debug("update data after version-up")
        for i in xrange(rows):
            session.execute("UPDATE t SET v = {} WHERE id = {}".format(i, i))
        debug("verify data")
        for i in xrange(rows):
            assert_one(session, "SELECT * FROM t_by_v WHERE v = {}".format(i), [i, i])
            assert_one(session, "SELECT v,id FROM t_by_v WHERE v = {}".format(i), [i, i])

    @since_dse('6.7')
    def test_upgrade_with_unselected_column_from_6_0(self):
        self._test_perform_sstable_operation_with_unselected_column(lambda node: self.upgrade_sstable(node), "alias:bdp/6.0-dev")

    @since_dse('6.7')
    def test_compact_with_unselected_column_from_6_0(self):
        self._test_perform_sstable_operation_with_unselected_column(lambda node: self.compact_sstable(node), "alias:bdp/6.0-dev")

    def _test_perform_sstable_operation_with_unselected_column(self, sstable_operation, version):
        """
        Tests upgrade between 6.0 and DSE-6.7.0 to verify materialized view backward compatibility with unselected columns

        @jira_ticket DB-1060
        """
        debug("populate cluster with version: {}".format(version))
        session = self.prepare(version=version, rf=3, nodes=3)
        cluster = self.cluster
        node1, node2, node3 = cluster.nodelist()

        debug("populate legacy schema")
        session.execute("CREATE TABLE t (p int, k int, a int, b int, c int, d int, primary key(p,k))")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT p,k,a,b FROM t WHERE p IS NOT NULL "
                         "AND k IS NOT NULL PRIMARY KEY (p, k)"))
        session.cluster.control_connection.wait_for_schema_agreement()

        rows = 100

        def populate_data(session, rows):
            debug("populate data")
            for i in xrange(rows):
                # update on unselected then delete it
                session.execute("UPDATE t SET c = {} WHERE p=1 AND k={}".format(i, i))
                session.execute("UPDATE t SET c = null WHERE p=1 AND k={}".format(i))
                # update on unselected
                session.execute("UPDATE t SET c = {}, d = {} WHERE p=2 AND k={}".format(i, i, i))

        populate_data(session, rows)
        self.cluster.flush()

        def verify_data(session, rows):
            debug("verify data")
            for i in xrange(rows):
                assert_none(session, "SELECT * FROM t WHERE p=1 AND k={}".format(i))
                assert_none(session, "SELECT * FROM t_by_v WHERE p=1 AND k={}".format(i))
                assert_one(session, "SELECT * FROM t WHERE p=2 AND k={}".format(i), [2, i, None, None, i, i])
                assert_one(session, "SELECT * FROM t_by_v WHERE p=2 AND k={}".format(i), [2, i, None, None])

        self.do_rolling_upgrade(session, rows, verify_data, sstable_operation)

        debug("update data after upgrade DSE")
        for i in xrange(rows):
            # update on selected
            session.execute("UPDATE t SET c = {} WHERE p=1 AND k={}".format(i, i))
            # delete all unselected columns, view row should be dead
            session.execute("UPDATE t SET c = null, d = null WHERE p=2 AND k={}".format(i))
        self.cluster.flush()

        debug("verify data after upgrade DSE")
        for i in xrange(rows):
            assert_one(session, "SELECT * FROM t WHERE p=1 AND k={}".format(i), [1, i, None, None, i, None])
            assert_one(session, "SELECT * FROM t_by_v WHERE p=1 AND k={}".format(i), [1, i, None, None])
            assert_none(session, "SELECT * FROM t WHERE p=2 AND k={}".format(i))
            assert_none(session, "SELECT * FROM t_by_v WHERE p=2 AND k={}".format(i))

    @since_dse('6.7')
    def test_upgrade_conflict_column_name(self):
        """
        Verify legacy view schema with conflict column name works after upgrade to DSE 6.7
        """
        def sstable_operation(node):
            self.upgrade_sstable(node)

        version = "alias:bdp/6.0-dev"
        debug("populate cluster with version: {}".format(version))
        session = self.prepare(version=version, rf=3, nodes=3)
        cluster = self.cluster
        node1, node2, node3 = cluster.nodelist()

        debug("populate legacy schema with potential conflict after upgrade")
        session.execute("CREATE TABLE t (p int, k int, \".c\" int, c int, primary key(p,k))")
        session.execute(("CREATE MATERIALIZED VIEW t_by_v AS SELECT p,k,\".c\" FROM t WHERE p IS NOT NULL "
                         "AND k IS NOT NULL PRIMARY KEY (p, k)"))
        session.cluster.control_connection.wait_for_schema_agreement()

        debug("populate data")
        rows = 100
        for i in xrange(rows):
            # update on unselected then delete it
            session.execute("UPDATE t SET c = {} WHERE p=1 AND k={}".format(i, i))
            session.execute("UPDATE t SET c = null WHERE p=1 AND k={}".format(i))
            # update on  unselected
            session.execute("UPDATE t SET c = {} WHERE p=2 AND k={}".format(i, i))
        self.cluster.flush()

        def verify_data(session, rows):
            debug("verify data")
            for i in xrange(rows):
                assert_none(session, "SELECT * FROM t WHERE p=1 AND k={}".format(i))
                assert_none(session, "SELECT * FROM t_by_v WHERE p=1 AND k={}".format(i))
                assert_one(session, "SELECT * FROM t WHERE p=2 AND k={}".format(i), [2, i, None, i])
                assert_one(session, "SELECT * FROM t_by_v WHERE p=2 AND k={}".format(i), [2, i, None])

        self.do_rolling_upgrade(session, rows, verify_data, sstable_operation)

        debug("update data after cluster upgrade")
        for i in xrange(rows):
            # update on selected
            session.execute("UPDATE t SET c = {} WHERE p=1 AND k={}".format(i, i))
            # delete all columns, view row should be dead
            session.execute("UPDATE t SET \".c\" = null, c=null WHERE p=2 AND k={}".format(i))
        self.cluster.flush()

        debug("verify data after cluster upgrade")
        for i in xrange(rows):
            assert_one(session, "SELECT * FROM t WHERE p=1 AND k={}".format(i), [1, i, None, i])
            assert_one(session, "SELECT * FROM t_by_v WHERE p=1 AND k={}".format(i), [1, i, None])
            assert_none(session, "SELECT * FROM t WHERE p=2 AND k={}".format(i))
            assert_none(session, "SELECT * FROM t_by_v WHERE p=2 AND k={}".format(i))

    @since_dse('6.7')
    def test_upgrade_compatibility_with_no_hidden_column_from_6_0(self):
        """
        DB-1060: test view that has the same primary key columns as base table and all columns are selected.
        """
        version = "alias:bdp/6.0-dev"
        debug("populate cluster with version: {}".format(version))
        session = self.prepare(version=version, rf=1, nodes=2, jmx=True)
        cluster = self.cluster
        node1, node2 = cluster.nodelist()

        debug("populate legacy schema")
        session.execute("CREATE TABLE t (p int, k int, a int, b int, c int, d int, primary key(p,k))")
        session.execute(("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t WHERE p IS NOT NULL "
                         "AND k IS NOT NULL PRIMARY KEY (k, p)"))
        session.cluster.control_connection.wait_for_schema_agreement()

        rows = 100

        def populate_data(session, rows):
            debug("populate data")
            for i in xrange(rows):
                # update on unselected then delete it
                session.execute("UPDATE t SET c = {} WHERE p={} AND k={}".format(i, i, i))
                session.execute("UPDATE t SET c = null WHERE p={} AND k={}".format(i, i))
                # update on unselected
                session.execute("UPDATE t SET c = {}, d = {} WHERE p={} AND k={}".format(i, i, i, i + 1))

        def verify_data(session, rows):
            debug("verify data")
            for i in xrange(rows):
                assert_none(session, "SELECT * FROM t WHERE p={} AND k={}".format(i, i))
                assert_none(session, "SELECT * FROM mv WHERE p={} AND k={}".format(i, i))
                assert_one(session, "SELECT * FROM t WHERE p={} AND k={}".format(i, i + 1), [i, i + 1, None, None, i, i])
                assert_one(session, "SELECT * FROM mv WHERE p={} AND k={}".format(i, i + 1), [i + 1, i, None, None, i, i])

        debug("upgrade node1 to DSE 6.7")
        self.upgrade_to_version(self.default_install_dir, node1, jmx=True)

        debug("populate view rows on mixed cluster")
        session = self.patient_cql_connection(node1, consistency_level=CL.ALL, keyspace="ks")
        populate_data(session, rows)
        verify_data(session, rows)
        self._verify_all_remote_view_write_succeess()

        debug("upgrade node2 to DSE 6.7")
        self.upgrade_to_version(self.default_install_dir, node2, jmx=True)

        debug("populate view rows on new cluster")
        session = self.patient_cql_connection(node1, consistency_level=CL.ALL, keyspace="ks")
        populate_data(session, rows)
        verify_data(session, rows)
        self._verify_all_remote_view_write_succeess()

    def _verify_all_remote_view_write_succeess(self):
        for node in self.cluster.nodelist():
            attemp = self._get_view_write_replica_attemp(node)
            success = self._get_view_write_replica_success(node)
            debug("{}, attemp: {}, success: {}".format(node.name, attemp, success))
            self.assertTrue(success == attemp, msg="Expect no failed remote view writes on {} but got {}/{}.".format(node.name, success, attemp))

    def _get_view_write_replica_success(self, node):
        mbean = make_mbean('metrics', type="ClientRequest", scope="ViewWrite", name='ViewReplicasSuccess')
        with JolokiaAgent(node) as jmx:
            return jmx.read_attribute(mbean, 'Count')

    def _get_view_write_replica_attemp(self, node):
        mbean = make_mbean('metrics', type="ClientRequest", scope="ViewWrite", name='ViewReplicasAttempted')
        with JolokiaAgent(node) as jmx:
            return jmx.read_attribute(mbean, 'Count')

    def do_rolling_upgrade(self, session, rows, verify_data, sstable_operation):
        debug("rolling upgrade to latest version {}".format(self.default_install_dir))

        for node in self.cluster.nodelist():
            verify_data(session, rows)
            self.upgrade_to_version(self.default_install_dir, node)
            session = self.patient_exclusive_cql_connection(node, consistency_level=CL.ALL, keyspace="ks")

        verify_data(session, rows)

        debug("Perform sstable operation and verify data")
        for node in self.cluster.nodelist():
            sstable_operation(node)
            verify_data(session, rows)

    def upgrade_to_version(self, tag, node, jvm_args=[], jmx=False):
        format_args = {'node': node.name, 'tag': tag}
        debug('Upgrading node {node} to {tag}'.format(**format_args))
        # drain and shutdown
        node.drain()
        node.watch_log_for("DRAINED")
        node.stop(wait_other_notice=False)
        debug('{node} stopped'.format(**format_args))

        # Update Cassandra Directory
        debug('Updating version to tag {tag}'.format(**format_args))
        node.set_install_dir(install_dir=tag, verbose=True)
        debug('Set new cassandra dir for {node}: {tag}'.format(**format_args))

        if jmx:
            remove_perf_disable_shared_mem(node)

        # Restart node on new version
        debug('Starting {node} on new version ({tag})'.format(**format_args))
        node.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=jvm_args)

    def scrub_sstable(self, node):
        debug('Running scrub')
        node.nodetool('scrub')
        debug('Scrub of {} complete'.format(node.name))

    def compact_sstable(self, node):
        debug('Running compaction')
        node.nodetool('compact')
        debug('Compact of {} complete'.format(node.name))

    def upgrade_sstable(self, node):
        debug('Running upgradesstables')
        node.nodetool('upgradesstables -a')
        debug('Upgrade of {} complete'.format(node.name))
