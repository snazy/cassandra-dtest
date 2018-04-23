import time
from unittest import skipIf

from ccmlib.common import is_win
from ccmlib.node import Node
from dse import Unauthorized

from dtest import MEMTABLE_TYPE, Tester, debug
from tools.assertions import assert_all, assert_invalid
from tools.decorators import since
from tools.misc import ImmutableMapping


@since('2.2')
class TestAuthUpgrade(Tester):
    cluster_options = ImmutableMapping({'authenticator': 'PasswordAuthenticator',
                                        'authorizer': 'CassandraAuthorizer'})

    _ignore_log_patterns = (
        # This one occurs if we do a non-rolling upgrade, the node
        # it's trying to send the migration to hasn't started yet,
        # and when it does, it gets replayed and everything is fine.
        r'Can\'t send migration request: node.*is down',
    )
    _ignore_log_patterns_21 = _ignore_log_patterns + (
        # 2.1 may complain about unknown column familiy
        r'org\.apache\.cassandra\.db\.UnknownColumnFamilyException',
    )

    ignore_log_patterns = _ignore_log_patterns

    def upgrade_to_22_test(self):
        self.do_upgrade_with_internal_auth("github:apache/cassandra-2.2")

    @since('3.0')
    @skipIf(MEMTABLE_TYPE.startswith("offheap"), 'offheap_objects are not available in 3.0')
    def upgrade_to_30_test(self):
        self.do_upgrade_with_internal_auth("github:apache/cassandra-3.0")

    @since('2.2', max_version='3.X')
    def test_upgrade_legacy_table(self):
        """
        Upgrade with bringing up the legacy tables after the newer nodes (without legacy tables)
        were started.

        @jira_ticket CASSANDRA-12813
        """

        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_install_dir(version="2.1.16")
        cluster.populate(3).start()

        node1, node2, node3 = cluster.nodelist()

        # Wait for default user to get created on one of the nodes
        time.sleep(15)

        # Upgrade to current version
        for node in [node1, node2, node3]:
            node.drain()
            node.watch_log_for("DRAINED")
            node.stop(gently=True)
            self.set_node_to_current_version(node)

        cluster.start()

        # Make sure the system_auth table will get replicated to the node that we're going to replace
        session = self.patient_cql_connection(node1, user='cassandra', password='cassandra')
        session.execute("ALTER KEYSPACE system_auth WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
        cluster.repair()

        cluster.stop()

        # Replace the node
        cluster.seeds.remove(node1)
        cluster.remove(node1)

        self._check_and_mark_errors_in_log()

        # check logs for errors, with UnknownColumnFamilyException excluded
        self.ignore_log_patterns = self._ignore_log_patterns_21

        replacement_address = node1.address()
        replacement_node = Node('replacement', cluster=self.cluster, auto_bootstrap=True,
                                thrift_interface=(replacement_address, 9160), storage_interface=(replacement_address, 7000),
                                jmx_port='7400', remote_debug_port='0', initial_token=None, binary_interface=(replacement_address, 9042))
        self.set_node_to_current_version(replacement_node)

        cluster.add(replacement_node, True)
        replacement_node.start(wait_for_binary_proto=True)

        node2.start(wait_for_binary_proto=True)
        node3.start(wait_for_binary_proto=True)

        replacement_node.watch_log_for('Initializing system_auth.credentials')
        replacement_node.watch_log_for('Initializing system_auth.permissions')
        replacement_node.watch_log_for('Initializing system_auth.users')

        cluster.repair()
        replacement_node.watch_log_for('Repair command')

        # Should succeed. Will throw an NPE on pre-12813 code.
        self.patient_cql_connection(replacement_node, user='cassandra', password='cassandra')

    def do_upgrade_with_internal_auth(self, target_version):
        """
        Tests upgrade between 2.1->2.2 & 2.1->3.0 as the schema and apis around authn/authz changed

        @jira_ticket CASSANDRA-7653
        """
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_install_dir(version="github:apache/cassandra-2.1")
        cluster.populate(3).start()

        node1, node2, node3 = cluster.nodelist()

        # wait for default superuser creation
        # The log message
        # node.watch_log_for('Created default superuser')
        # will only appear on one of the three nodes, and we don't know
        # which ahead of time. Grepping all three in parallel is unpleasant.
        # See auth_test and auth_roles test for instances of this as well.
        # Should be fixed by C*-6177
        time.sleep(15)

        session = self.patient_cql_connection(node1, user='cassandra', password='cassandra')
        session.execute("CREATE USER klaus WITH PASSWORD '12345' SUPERUSER")
        session.execute("CREATE USER michael WITH PASSWORD '54321' NOSUPERUSER")
        session.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        session.execute("CREATE TABLE ks.cf1 (id int primary key, val int)")
        session.execute("CREATE TABLE ks.cf2 (id int primary key, val int)")
        session.execute("GRANT MODIFY ON ks.cf1 TO michael")
        session.execute("GRANT SELECT ON ks.cf2 TO michael")

        self.check_permissions(node1, False)
        session.cluster.shutdown()

        # check logs for errors, with UnknownColumnFamilyException excluded
        self.ignore_log_patterns = self._ignore_log_patterns_21

        self._check_and_mark_errors_in_log()
        # upgrade node1 to 2.2/3.0
        self.upgrade_to_version(target_version, node1)
        # run the permissions checking queries on the upgraded node
        # this will be using the legacy tables as the conversion didn't complete
        # but the output format should be updated on the upgraded node
        self.check_permissions(node1, True)
        # and check on those still on the old version
        self.check_permissions(node2, False)
        self.check_permissions(node3, False)

        # now upgrade the remaining nodes
        self.upgrade_to_version(target_version, node2)
        self.upgrade_to_version(target_version, node3)

        self._check_and_mark_errors_in_log()

        # reset 'ignore_log_patterns'
        self.ignore_log_patterns = self._ignore_log_patterns

        self.check_permissions(node2, True)
        self.check_permissions(node3, True)

        # we should now be able to drop the old auth tables
        session = self.patient_cql_connection(node1, user='cassandra', password='cassandra')
        session.execute('DROP TABLE system_auth.users', timeout=60)
        session.execute('DROP TABLE system_auth.credentials', timeout=60)
        session.execute('DROP TABLE system_auth.permissions', timeout=60)
        # and we should still be able to authenticate and check authorization
        self.check_permissions(node1, True)
        debug('Test completed successfully')

    def _check_and_mark_errors_in_log(self):
        if not self.allow_log_errors and self.check_logs_for_errors():
            raise AssertionError('Unexpected error in log before upgrade, see stdout')
        for node in self.cluster.nodelist():
            node.mark_log_for_errors()

    def check_permissions(self, node, upgraded):
        # use an exclusive connection to ensure we only talk to the specified node
        klaus = self.patient_exclusive_cql_connection(node, user='klaus', password='12345', timeout=20)
        # klaus is a superuser, so should be able to list all permissions
        # the output of LIST PERMISSIONS changes slightly with #7653 adding
        # a new role column to results, so we need to tailor our check
        # based on whether the node has been upgraded or not
        if not upgraded:
            assert_all(klaus,
                       'LIST ALL PERMISSIONS',
                       [['michael', '<table ks.cf1>', 'MODIFY'],
                        ['michael', '<table ks.cf2>', 'SELECT']],
                       timeout=60)
        else:
            assert_all(klaus,
                       'LIST ALL PERMISSIONS',
                       [['michael', 'michael', '<table ks.cf1>', 'MODIFY'],
                        ['michael', 'michael', '<table ks.cf2>', 'SELECT']],
                       timeout=60)

        klaus.cluster.shutdown()

        michael = self.patient_exclusive_cql_connection(node, user='michael', password='54321')
        michael.execute('INSERT INTO ks.cf1 (id, val) VALUES (0,0)')
        michael.execute('SELECT * FROM ks.cf2')
        assert_invalid(michael,
                       'SELECT * FROM ks.cf1',
                       'User michael has no SELECT permission on <table ks.cf1> or any of its parents',
                       Unauthorized)
        michael.cluster.shutdown()

    def upgrade_to_version(self, tag, node):
        format_args = {'node': node.name, 'tag': tag}
        debug('Upgrading node {node} to {tag}'.format(**format_args))
        # drain and shutdown
        node.drain()
        node.watch_log_for("DRAINED")
        node.stop(wait_other_notice=False)
        debug('{node} stopped'.format(**format_args))

        # Ignore errors before upgrade on Windows
        if is_win():
            node.mark_log_for_errors()

        # Update Cassandra Directory
        debug('Updating version to tag {tag}'.format(**format_args))
        node.set_install_dir(version=tag, verbose=True)
        debug('Set new cassandra dir for {node}: {tag}'.format(**format_args))

        # Restart node on new version
        debug('Starting {node} on new version ({tag})'.format(**format_args))
        # Setup log4j / logback again (necessary moving from 2.0 -> 2.1):
        node.set_log_level("INFO")
        node.start(wait_other_notice=True)
        # wait for the conversion of legacy data to either complete or fail
        # (because not enough upgraded nodes are available yet)
        debug('Waiting for conversion of legacy data to complete or fail')
        node.watch_log_for('conversion of legacy permissions')

        debug('Running upgradesstables')
        node.nodetool('upgradesstables -a')
        debug('Upgrade of {node} complete'.format(**format_args))