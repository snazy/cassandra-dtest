from ccmlib.node import TimeoutError
from dse.query import ConsistencyLevel, SimpleStatement

from dtests.dtest import Tester, debug, get_dse_version_from_build, get_dse_version_from_build_safe
from tools.assertions import assert_all
from tools.data import rows_to_list
from tools.decorators import since_dse


class TestFeatureUpgrade(Tester):
    """
    Upgrade test for features using com.datastax.bdp.db.upgrade.VersionDependentFeature.

    This is CassandraAuthorizer from DSE 5.1 to DSE 6.0
    and CassandraAuditWriter from DSE 5.0 to DSE 6.0 (DSE 5.1 is not affected).

    The new functionalities must only be enabled, when
    - all nodes are on the DSE 6.0
    - there is schema agreement to perform the DDL changes (not explicitly tested here, but in unit tests)
    - there is schema agreement after the DDL changes (not explicitly tested here, but in unit tests)

    This test uses the log messages to verify whether the features are enabled or not.
    """

    _default_config = {
        'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
        'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer'
    }

    _audit_log_config = {
        'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
        'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
        'audit_logging_options': {
            'enabled': 'true',
            'logger': 'com.datastax.bdp.db.audit.CassandraAuditWriter',
            'included_roles': 'testuser'
        }
    }

    _auth_compat_message = "Not all live nodes are running DSE 6\.0 or newer or upgrade in progress - " \
                           "GRANT AUHTORIZE FOR and RESTRICT are not available until all nodes are running DSE 6\.0 " \
                           "or newer and automatic schema upgrade has finished"
    _auth_native_message = "All live nodes are running DSE 6\.0 or newer - GRANT AUHTORIZE FOR and RESTRICT available"
    _audit_compat_message = "Using DSE 5\.0 compatible audit log entries - DSE 5\.1 compatible audit log entries will " \
                            "be written when all nodes are on DSE 5\.1 or newer and automatic schema upgrade has finished"
    _audit_native_message = "Unlocking DSE 5\.1.6\.0 audit log entries"
    _audit_log_enabled = "Audit logging is enabled with com.datastax.bdp.db.audit.CassandraAuditWriter"
    _version_XX_on_all_nodes = "All nodes in this cluster are on version {}.*|" \
                               "All nodes in this cluster are on DSE version {}.*"
    _mixed_versions = "Nodes in this cluster are on version .* up to version .*|" \
                      "Nodes in this cluster are on DSE version .* up to version .*"
    _mixed_versions_XX = "Nodes in this cluster are on version {}.* up to version {}.*|" \
                         "Nodes in this cluster are on DSE version {}.* up to version {}.*"
    _mixed_versions_OSS_XX = "Nodes in this cluster are on OSS C\* versions {}.* - {}.* and DSE up to version {}.*"

    # Mapping of current version to the branch of the previous version.
    # The general_* upgrade tests will fail, if there is no
    _previous_dse_version_branches = {'6.0': "alias:bdp/5.1-dev",
                                      '6.7': "alias:bdp/6.0-dev",
                                      '7.0': "alias:bdp/6.7-dev"}
    _release_version_for_dse_version = {'5.1': '3.11',
                                        '6.0': '4.0',
                                        '6.7': '4.0',
                                        '7.0': '4.0'}

    def _get_dse_version(self, install_dir=None):
        if not install_dir:
            install_dir = self.cluster.get_install_dir()
        ver = get_dse_version_from_build(install_dir)
        return "{}.{}".format(ver.version[0], ver.version[1])

    def _get_previous_version_branch(self):
        dse_version = self._get_dse_version()
        debug("Target DSE version: {}".format(dse_version))
        if dse_version not in self._previous_dse_version_branches:
            self.fail("No branch name for the previous version configured for DSE version {}".format(dse_version))
        return self._previous_dse_version_branches[dse_version]

    def _prepare_cluster(self, config=None, start_version=None, num_nodes=1):

        # Prefer local install_dir (handy for development)
        self.cluster.set_install_dir(version=start_version)

        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(num_nodes, install_byteman=True)
        self.cluster.start(wait_for_binary_proto=True)
        install_dir = self.cluster.get_install_dir()
        release_version = self.cluster.version()
        dse_version = self._get_dse_version(install_dir=install_dir)
        debug("{} node cluster populated - DSE version: {} ({}) - install_dir: {}".format(num_nodes,
                                                                                          dse_version,
                                                                                          release_version,
                                                                                          install_dir))
        return release_version

    def _maybe_fake_dse_version(self, nodes, start_dse_version, dse_version):
        """
        dse-db versions before 6.7 do not maintain the 'dse_version' column in system.peers and system.local
        This method populates that column for dse-db >= 6.7, which relies on that that column containing valid
        information. This is only needed for tests that simulate a dead node or an offline upgrade, because
        live nodes keep the dse_version in Gossip state and cached peer-info.
        """
        # only dse-db since 6.0 has the DSE specific columns in system.peers + system.local
        if start_dse_version >= '6.0' and dse_version >= '6.7':
            for node in nodes:
                session = self.patient_exclusive_cql_connection(node, user='cassandra', password='cassandra')
                rows = rows_to_list(session.execute("SELECT peer, dse_version FROM system.peers"))
                for row in rows:
                    if not row[1]:
                        debug("  Updating system.peers.dse_version on {} to {} for {}".format(node.name, start_dse_version, row[0]))
                        session.execute("UPDATE system.peers SET dse_version = '{}' WHERE peer = '{}'".format(start_dse_version, row[0]))
                session.shutdown()

    def _setup_auth_schema(self, node, repl_factor):
        # Create the dse_audit.audit_log table as it looked like in DSE 5.0
        session = self.patient_exclusive_cql_connection(node, user='cassandra', password='cassandra')
        session.execute("ALTER KEYSPACE system_auth WITH replication = {{'class':'SimpleStrategy', 'replication_factor':{}}}".format(repl_factor))
        # rely on read-repair
        session.execute(SimpleStatement("SELECT * FROM system_auth.roles", consistency_level=ConsistencyLevel.ALL))
        session.execute(SimpleStatement("SELECT * FROM system_auth.role_members", consistency_level=ConsistencyLevel.ALL))
        session.execute(SimpleStatement("SELECT * FROM system_auth.role_permissions", consistency_level=ConsistencyLevel.ALL))

        session.cluster.control_connection.wait_for_schema_agreement()
        session.shutdown()

    def _setup_upgrade_schema(self, node, forDSE50, repl_factor):
        self._setup_auth_schema(node, repl_factor)
        session = self.patient_exclusive_cql_connection(node, user='cassandra', password='cassandra')
        # create test user
        session.execute("CREATE USER testuser WITH password 'topsecret'")
        # setup dse_audit schema (old dse5.0 + dse5.1 does not have audit-log functionality in apollo)
        session.execute("CREATE KEYSPACE dse_audit WITH replication = {{'class':'SimpleStrategy', 'replication_factor':{}}}".format(repl_factor))
        if forDSE50:
            # Create the dse_audit.audit_log table as it looked like in DSE 5.0
            session.execute("CREATE TABLE dse_audit.audit_log (" +
                            "date timestamp," +
                            "node inet," +
                            "day_partition int," +
                            "event_time timeuuid," +
                            "batch_id uuid," +
                            "category text," +
                            "keyspace_name text," +
                            "operation text," +
                            "source text," +
                            "table_name text," +
                            "type text," +
                            "username text," +
                            "PRIMARY KEY ((date, node, day_partition), event_time))" +
                            " WITH COMPACTION={'class':'TimeWindowCompactionStrategy'}")
        else:
            # Create the dse_audit.audit_log table as it looks since DSE 5.1
            session.execute("CREATE TABLE dse_audit.audit_log (" +
                            "date timestamp," +
                            "node inet," +
                            "day_partition int," +
                            "event_time timeuuid," +
                            "batch_id uuid," +
                            "category text," +
                            "keyspace_name text," +
                            "operation text," +
                            "source text," +
                            "table_name text," +
                            "type text," +
                            "username text," +
                            "authenticated text," +
                            "consistency text," +
                            "PRIMARY KEY ((date, node, day_partition), event_time))" +
                            " WITH COMPACTION={'class':'TimeWindowCompactionStrategy'}")
        # create a table - (keyspace does not matter here)
        session.execute("CREATE TABLE dse_audit.dummy_table ( id int primary key )")
        session.execute("GRANT SELECT ON dse_audit.dummy_table TO testuser")

        session.cluster.control_connection.wait_for_schema_agreement()
        session.shutdown()

    def _upgrade_node(self, node, install_dir=None, config=None, running_node=True):
        format_args = {'node': node.name, 'new_version': install_dir, 'config': config}
        debug('Upgrading node {node} to {new_version} with config {config}'.format(**format_args))
        # drain and shutdown
        if running_node:
            node.drain()
            node.watch_log_for("DRAINED")
            node.stop(wait_other_notice=False)
            debug('{node} stopped'.format(**format_args))

        # Update Cassandra Directory
        debug('Updating version to {new_version}'.format(**format_args))
        node.set_install_dir(install_dir=install_dir, verbose=True)
        debug('Set new cassandra dir for {node}: {new_version}'.format(**format_args))

        # Restart node on new version
        # Setup log4j / logback again (necessary moving from 2.0 -> 2.1):
        if config:
            node.set_configuration_options(values=config)
        mark = node.mark_log(filename='debug.log')
        if running_node:
            debug('Starting {node} on new version ({new_version})'.format(**format_args))
            node.start(wait_other_notice=True, wait_for_binary_proto=True)

        debug('Upgrade of {} to {} ({}), install-dir {} complete'.format(node.name,
                                                                         get_dse_version_from_build(node.get_install_dir()),
                                                                         node.get_cassandra_version(),
                                                                         node.get_install_dir()))

        return mark

    def _perform_audited_operation(self, node, forDSE50, key):
        """
        Inserts a row into dse_audit.dummy_table using the audited test user and returns the row
        expected in dse_audit.audit_log.
        """
        query = u'SELECT * FROM dse_audit.dummy_table WHERE id = {}'.format(key)
        user_session = self.patient_exclusive_cql_connection(node, user='testuser', password='topsecret')
        user_session.execute(query)
        user_session.shutdown()
        if forDSE50:
            # DSE 5.0 does not know about 'authenticated' and 'consistency' columns
            return [[u'dse_audit', u'dummy_table', query, None, None]]
        return [[u'dse_audit', u'dummy_table', query, u'testuser', u'ONE']]

    def _verify_audit_log(self, expected_rows, node):
        """
        Verifies that all the rows as returned by _perform_audited_operation exist in dse_audit.audit_log
        for the test table.
        """
        validation_query = "SELECT keyspace_name, table_name, operation, authenticated, consistency " \
                           "FROM dse_audit.audit_log WHERE table_name='dummy_table' ALLOW FILTERING"
        superuser_session = self.patient_exclusive_cql_connection(node, user='cassandra', password='cassandra')
        assert_all(superuser_session, validation_query, expected_rows, ignore_order=True)
        superuser_session.shutdown()
        debug("dse_audit.audit_log contents verified")

    @since_dse('6.0', max_version='6.7.9999')
    def upgrade_two_nodes_50_to_6x_test(self):
        """
        2-node upgrade from 5.0.latest to 6.0/6.7.latest test.

        Validations:
        * Audit log rows have not all columns filled (2 new columns 'authenticated' + 'consistency' added in DSE 5.1)
          until all nodes are on DSE 6.0
        * CassandraAuditWriter falls into legacy (DSE 5.0) mode until all nodes are on DSE 6.0
        * CassandraAuthorizer falls into legacy mode until all nodes are on DSE 6.0
        * Version logging in CassandraDaemon emits the correct versions

        @jira_ticket DB-1597
        """

        self._upgrade_two_nodes_test(True, start_version="alias:bdp/5.0-dev")

    @since_dse('6.0', max_version='6.7.9999')
    def upgrade_two_nodes_51_to_6x_test(self):
        """
        2-node upgrade from 5.1.latest to 6.0/6.7.latest test.

        Validations:
        * Audit log rows have all columns filled (2 new columns 'authenticated' + 'consistency' added in DSE 5.1)
        * CassandraAuditWriter never falls into legacy (DSE 5.0) mode
        * CassandraAuthorizer falls into legacy mode until all nodes are on DSE 6.0
        * Version logging in CassandraDaemon emits the correct versions

        @jira_ticket DB-1597
        """

        self._upgrade_two_nodes_test(False, start_version="alias:bdp/5.1-dev")

    def _upgrade_two_nodes_test(self, dse50, start_version=None):
        """
        :param dse50: True, if source version is dse5.0
        :param start_version: Start version tag (or git alias or so)
        """

        dse_version = self._get_dse_version()
        release_version = self._release_version_for_dse_version[dse_version]

        debug("two-node-feature-upgrade-dtest from DSE {} to DSE {}, install-dir {}".format("5.0" if dse50 else "5.1",
                                                                                            dse_version,
                                                                                            self.cluster.get_install_dir()))

        # save the current install_dir as the target-install_dir as we change the version/install_dir with _prepare_cluster
        target_install_dir = self.cluster.get_install_dir()

        start_release_version = self._prepare_cluster(start_version=start_version, num_nodes=2, config=self._default_config)
        start_dse_version = get_dse_version_from_build_safe(self.cluster.get_install_dir())
        node1, node2 = self.cluster.nodelist()

        # Create the schema - dse_audit keyspace with audit_log table, "audited" test user and test table
        self._setup_upgrade_schema(node1, dse50, repl_factor=2)

        # Upgrade node 1 to dse6.0
        log_mark1 = self._upgrade_node(node1, install_dir=target_install_dir, config=self._audit_log_config)

        # There are three kinds of version-barriers at play:
        # - The informal notification whether all nodes are running the same version or not.
        # - The notification whether the new "separation of duties" authorization feature can be used.
        #   I.e. whether the two new columns in 'system_auth.role_permissions' are accessible and therefore
        #   the GRANT AUTHORIZE FOR and RESTRICT functionality.
        #   This notification requires both a minimum-version (dse6.0) _and_ schema agreement.
        #   The schema agreement is not required, when all nodes were on dse6.0 on startup.
        # - The notification whether the columns added to dse_audit.audit_log in DSE 5.1
        #   This notification requires both a minimum-version (dse5.1) _and_ schema agreement.
        #   The schema agreement is not required, when all nodes were on dse5.1 on startup.

        # Check log messages after upgrading node1 to dse6.0
        node1.watch_log_for([self._audit_log_enabled,
                             self._auth_compat_message],
                            filename='debug.log', from_mark=log_mark1, timeout=3)
        if dse50:
            node1.watch_log_for([self._audit_compat_message,
                                 self._mixed_versions_XX.format(start_release_version, release_version, start_dse_version, dse_version)],
                                filename='debug.log', from_mark=log_mark1, timeout=3)
        else:  # dse5.1
            with self.assertRaises(TimeoutError):
                node1.watch_log_for(self._audit_compat_message, filename='debug.log', from_mark=log_mark1, timeout=3)
            node1.watch_log_for([self._audit_native_message,
                                 self._mixed_versions_XX.format(start_release_version, release_version, start_dse_version, dse_version)],
                                filename='debug.log', from_mark=log_mark1, timeout=3)

        # Produce the first audited operation...
        expected_rows = self._perform_audited_operation(node1, dse50, 1)

        # Upgrade node 2 to dse6.0
        log_mark1 = node1.mark_log(filename='debug.log')
        log_mark2 = self._upgrade_node(node2, install_dir=target_install_dir, config=self._audit_log_config)

        # Check log messages after upgrading node2 to dse6.0
        with self.assertRaises(TimeoutError):
            node2.watch_log_for(self._audit_compat_message, filename='debug.log', from_mark=log_mark2, timeout=3)
        with self.assertRaises(TimeoutError):
            node2.watch_log_for(self._mixed_versions, filename='debug.log', from_mark=log_mark2, timeout=3)
        node2.watch_log_for([self._audit_log_enabled,
                             self._audit_native_message,
                             self._auth_native_message,
                             self._version_XX_on_all_nodes.format(release_version, dse_version)],
                            filename='debug.log', from_mark=log_mark2, timeout=3)
        node1.watch_log_for([self._auth_native_message,
                             self._version_XX_on_all_nodes.format(release_version, dse_version)],
                            filename='debug.log', from_mark=log_mark1, timeout=3)
        if dse50:
            node1.watch_log_for(self._audit_native_message, filename='debug.log', from_mark=log_mark1, timeout=3)
        else:  # dse5.1
            with self.assertRaises(TimeoutError):
                node1.watch_log_for(self._audit_native_message, filename='debug.log', from_mark=log_mark1, timeout=3)

        # Perform another audited operation...
        expected_rows += self._perform_audited_operation(node1, False, 2)

        debug('Bouncing {}'.format(node1.name))
        node1.stop(wait_other_notice=False)
        log_mark1 = node1.mark_log(filename='debug.log')
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Check log messages after bouncing node1
        node1.watch_log_for([self._audit_log_enabled,
                             self._audit_native_message,
                             self._auth_native_message,
                             self._version_XX_on_all_nodes.format(release_version, dse_version)],
                            filename='debug.log', from_mark=log_mark1, timeout=3)

        # Produce the third audited operation...
        expected_rows += self._perform_audited_operation(node1, False, 3)

        # Verify the audit log entry does or does not include the columns 'authenticated' + 'consistency'
        # Within a mixed-version cluster (dse5.0 + dse6.0), those columns are not usable - but for a mixed
        # version cluster having dse5.1 + dse6.0, those columns are usable.
        self._verify_audit_log(expected_rows, node1)

    @since_dse('6.0', max_version='6.7.9999')
    def upgrade_single_node_50_to_6x_test(self):
        """
        1-node upgrade from 5.0.latest to 6.0/6.7.latest test.

        Validations:
        * Audit log rows have not all columns filled (2 new columns 'authenticated' + 'consistency' added in DSE 5.1)
          until all nodes are on DSE 6.0
        * CassandraAuditWriter falls into legacy (DSE 5.0) mode until all nodes are on DSE 6.0
        * CassandraAuthorizer falls into legacy mode until all nodes are on DSE 6.0
        * Version logging in CassandraDaemon emits the correct versions

        @jira_ticket DB-1597
        """

        self._upgrade_single_node_test(True, start_version="alias:bdp/5.0-dev")

    @since_dse('6.0', max_version='6.7.9999')
    def upgrade_single_node_51_to_6x_test(self):
        """
        1-node upgrade from 5.1.latest to 6.0/6.7.latest test.

        Validations:
        * Audit log rows have all columns filled (2 new columns 'authenticated' + 'consistency' added in DSE 5.1)
        * CassandraAuditWriter never falls into legacy (DSE 5.0) mode
        * CassandraAuthorizer falls into legacy mode until all nodes are on DSE 6.0
        * Version logging in CassandraDaemon emits the correct versions

        @jira_ticket DB-1597
        """

        self._upgrade_single_node_test(False, start_version="alias:bdp/5.1-dev")

    def _upgrade_single_node_test(self, dse50, start_version=None):
        """
        :param dse50: True, if source version is dse5.0
        :param start_version: Start version tag (or git alias or so)
        """

        dse_version = self._get_dse_version()
        release_version = self._release_version_for_dse_version[dse_version]

        debug("single-node audit-log upgrade-dtest from DSE {} to DSE {}, install-dir {}".format("5.0" if dse50 else "5.1",
                                                                                                 dse_version,
                                                                                                 self.cluster.get_install_dir()))

        # save the current install_dir as the target-install_dir as we change the version/install_dir with _prepare_cluster
        target_install_dir = self.cluster.get_install_dir()

        self._prepare_cluster(start_version=start_version, num_nodes=1, config=self._default_config)
        node1 = self.cluster.nodelist()[0]

        # Create the schema - dse_audit keyspace with audit_log table, "audited" test user and test table
        self._setup_upgrade_schema(node1, dse50, repl_factor=1)

        # Upgrade node 1 to dse6.0
        log_mark1 = self._upgrade_node(node1, install_dir=target_install_dir, config=self._audit_log_config)

        # There are three kinds of version-barriers at play:
        # - The informal notification whether all nodes are running the same version or not.
        # - The notification whether the new "separation of duties" authorization feature can be used.
        #   I.e. whether the two new columns in 'system_auth.role_permissions' are accessible and therefore
        #   the GRANT AUTHORIZE FOR and RESTRICT functionality.
        #   This notification requires both a minimum-version (dse6.0) _and_ schema agreement.
        #   The schema agreement is not required, when all nodes were on dse6.0 on startup.
        # - The notification whether the columns added to dse_audit.audit_log in DSE 5.1
        #   This notification requires both a minimum-version (dse5.1) _and_ schema agreement.
        #   The schema agreement is not required, when all nodes were on dse5.1 on startup.

        # Check log messages after upgrading node1 to dse6.0
        if dse50:
            node1.watch_log_for([
                # expect both negative AND positive entries here, as the schema needs to be updated
                self._auth_compat_message,
                self._auth_native_message,
                # expect both negative AND positive entries here, as the schema needs to be updated
                self._audit_compat_message,
                self._audit_native_message,
                # same version - true
                self._version_XX_on_all_nodes.format(release_version, dse_version)],
                filename='debug.log', from_mark=log_mark1, timeout=10)
        else:
            node1.watch_log_for([
                # expect both negative AND positive entries here, as the schema needs to be updated
                self._auth_compat_message,
                self._auth_native_message,
                # only the "all good" message for audit, as the table schema is the same from DSE 5.1
                self._audit_native_message,
                # same version - true
                self._version_XX_on_all_nodes.format(release_version, dse_version)],
                filename='debug.log', from_mark=log_mark1, timeout=10)

        # Produce the first audited operation...
        expected_rows = self._perform_audited_operation(node1, False, 1)

        debug('Bouncing {}'.format(node1.name))
        node1.stop(wait_other_notice=False)
        log_mark1 = node1.mark_log(filename='debug.log')
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Check log messages after bouncing node1
        node1.watch_log_for([self._audit_log_enabled,
                             self._audit_native_message,
                             self._auth_native_message,
                             self._version_XX_on_all_nodes.format(release_version, dse_version)],
                            filename='debug.log', from_mark=log_mark1, timeout=3)

        # Produce the third audited operation...
        expected_rows += self._perform_audited_operation(node1, False, 2)

        # Verify the audit log entry does or does not include the columns 'authenticated' + 'consistency'
        # Within a mixed-version cluster (dse5.0 + dse6.0), those columns are not usable - but for a mixed
        # version cluster having dse5.1 + dse6.0, those columns are usable.
        self._verify_audit_log(expected_rows, node1)

    @since_dse('6.0', max_version='6.7.9999')
    def upgrade_with_a_dead_node_50_to_6x_test(self):
        self._upgrade_with_a_dead_node_test(True, start_version="alias:bdp/5.0-dev")

    def _upgrade_with_a_dead_node_test(self, dse50, start_version=None):
        """
        Simplified test compared to _upgrade_from_5x_two_nodes_test - checks that upgrade
        with one node dead in a 3-node cluster works, when the dead node is assassinated
        when the two other nodes are already upgraded.

        @jira_ticket DB-1597
        """

        dse_version = self._get_dse_version()
        release_version = self._release_version_for_dse_version[dse_version]

        debug("dead-node-feature-upgrade-dtest from DSE {} to DSE {}, install-dir {}".format("5.0" if dse50 else "5.1",
                                                                                             dse_version,
                                                                                             self.cluster.get_install_dir()))

        # save the current install_dir as the target-install_dir as we change the version/install_dir with _prepare_cluster
        target_install_dir = self.cluster.get_install_dir()

        start_release_version = self._prepare_cluster(start_version=start_version, num_nodes=3, config=self._default_config)
        start_dse_version = get_dse_version_from_build_safe(self.cluster.get_install_dir())
        node1, node2, node3 = self.cluster.nodelist()

        # Create the schema - dse_audit keyspace with audit_log table, "audited" test user and test table
        self._setup_upgrade_schema(node1, dse50, repl_factor=3)

        # Node 3 is our dead node, that will stay on 5.0 and will later be assassinated
        debug("Stopping {}".format(node3.name))
        node3.stop(wait_other_notice=True)

        # Upgrade node 1 to dse6.0
        log_mark1 = self._upgrade_node(node1, install_dir=target_install_dir, config=self._audit_log_config)
        log_mark2 = self._upgrade_node(node2, install_dir=target_install_dir, config=self._audit_log_config)

        # Verify that node 1 does NOT enable the new features
        node1.watch_log_for([self._audit_log_enabled,
                             self._audit_compat_message,
                             self._auth_compat_message,
                             self._mixed_versions_XX.format(start_release_version, release_version, start_dse_version, dse_version)],
                            filename='debug.log', from_mark=log_mark1, timeout=3)
        with self.assertRaises(TimeoutError):
            node1.watch_log_for(self._audit_native_message, filename='debug.log', from_mark=log_mark1, timeout=3)
        with self.assertRaises(TimeoutError):
            node1.watch_log_for(self._auth_native_message, filename='debug.log', from_mark=log_mark1, timeout=3)

        # Verify that node 2 does NOT enable the new features
        node2.watch_log_for([self._audit_log_enabled,
                             self._audit_compat_message,
                             self._auth_compat_message,
                             self._mixed_versions_XX.format(start_release_version, release_version, start_dse_version, dse_version)],
                            filename='debug.log', from_mark=log_mark2, timeout=3)
        with self.assertRaises(TimeoutError):
            node2.watch_log_for(self._audit_native_message, filename='debug.log', from_mark=log_mark2, timeout=3)
        with self.assertRaises(TimeoutError):
            node2.watch_log_for(self._auth_native_message, filename='debug.log', from_mark=log_mark2, timeout=3)

        log_mark1 = node1.mark_log(filename='debug.log')
        log_mark2 = node2.mark_log(filename='debug.log')

        debug("Assassinating {} ...".format(node3.name))
        node1.nodetool("assassinate {}".format(node3.network_interfaces['binary'][0]))
        debug("Assassinated {}".format(node3.name))

        node1.watch_log_for([self._audit_native_message,
                             self._auth_native_message,
                             self._version_XX_on_all_nodes.format(release_version, dse_version)],
                            filename='debug.log', from_mark=log_mark1, timeout=10)

        node2.watch_log_for([self._audit_native_message,
                             self._auth_native_message,
                             self._version_XX_on_all_nodes.format(release_version, dse_version)],
                            filename='debug.log', from_mark=log_mark2, timeout=3)

    @since_dse('6.0', max_version='6.7.9999')
    def offline_upgrade_50_to_6x_test(self):
        self._offline_upgrade_test(True, start_version="alias:bdp/5.0-dev")

    def _offline_upgrade_test(self, dse50, start_version=None):
        """
        Test the offline upgrade scenario.
        - setup an "old" cluster0
        - stop the cluster
        - upgrade the nodes one after each other (update software + start)

        New feature must only be enabled when all nodes are started, as the releas_version should
        still be the old one.

        @jira_ticket DB-1597
        """

        dse_version = self._get_dse_version()
        release_version = self._release_version_for_dse_version[dse_version]

        debug("offline-feature-upgrade-dtest from DSE {} to DSE {}, install-dir {}".format("5.0" if dse50 else "5.1",
                                                                                           dse_version,
                                                                                           self.cluster.get_install_dir()))

        # save the current install_dir as the target-install_dir as we change the version/install_dir with _prepare_cluster
        target_install_dir = self.cluster.get_install_dir()

        start_release_version = self._prepare_cluster(start_version=start_version, num_nodes=3, config=self._default_config)
        start_dse_version = get_dse_version_from_build_safe(self.cluster.get_install_dir())
        node1, node2, node3 = self.cluster.nodelist()

        # Create the schema - dse_audit keyspace with audit_log table, "audited" test user and test table
        self._setup_upgrade_schema(node1, dse50, repl_factor=3)

        # Node 3 is our dead node, that will stay on 5.0 and will later be assassinated
        for node in [node1, node2, node3]:
            debug("Stopping {}".format(node.name))
            node.stop(wait_other_notice=True)

        # Upgrade software of all nodes to dse6.0
        log_mark1 = self._upgrade_node(node1, install_dir=target_install_dir, config=self._audit_log_config, running_node=False)
        log_mark2 = self._upgrade_node(node2, install_dir=target_install_dir, config=self._audit_log_config, running_node=False)
        log_mark3 = self._upgrade_node(node3, install_dir=target_install_dir, config=self._audit_log_config, running_node=False)

        # start nodes 1 - verify log messages
        debug("Starting {}".format(node1.name))
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        log_watches = [self._audit_log_enabled,
                       self._audit_compat_message,
                       self._auth_compat_message]
        # dse-db 6.7 distinguishes between OSS and DSE versions.
        # dse-db since 6.0 has the DSE specific columns (dse_version in this case) in system.peers, but 5.0+5.1 don't.
        # So we cannot "fake" the dse_version column for _dead_ nodes (node 3) in this test, but we can live with
        # a different log message that (wrongly) indicates an upgrade from OSS to DSE - not relevant in production with
        # DSE code.
        if dse_version < '6.7':
            log_watches += [self._mixed_versions_XX.format(start_release_version, release_version, start_dse_version, dse_version)]
        else:
            log_watches += [self._mixed_versions_OSS_XX.format(start_release_version, release_version, dse_version)]

        node1.watch_log_for(log_watches, filename='debug.log', from_mark=log_mark1, timeout=3)
        with self.assertRaises(TimeoutError):
            node1.watch_log_for(self._audit_native_message, filename='debug.log', from_mark=log_mark1, timeout=3)
        with self.assertRaises(TimeoutError):
            node1.watch_log_for(self._auth_native_message, filename='debug.log', from_mark=log_mark1, timeout=3)
        log_mark1 = node1.mark_log(filename='debug.log')

        # start node 2 - verify log messages
        debug("Starting {}".format(node2.name))
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        node2.watch_log_for(log_watches, filename='debug.log', from_mark=log_mark2, timeout=3)
        with self.assertRaises(TimeoutError):
            node2.watch_log_for(self._audit_native_message, filename='debug.log', from_mark=log_mark2, timeout=3)
        with self.assertRaises(TimeoutError):
            node2.watch_log_for(self._auth_native_message, filename='debug.log', from_mark=log_mark2, timeout=3)
        log_mark2 = node2.mark_log(filename='debug.log')

        # start node 3 - verify log messages
        debug("Starting {}".format(node3.name))
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)

        # node 3 (the last node) announces it's release-version during startup and therefore the
        # other nodes can enable the new features.
        # node 3 itself may or may not see the new features needed to be disabled.
        node3.watch_log_for([self._audit_log_enabled,
                             self._audit_native_message,
                             self._auth_native_message,
                             self._version_XX_on_all_nodes.format(release_version, dse_version)],
                            filename='debug.log', from_mark=log_mark3, timeout=3)
        # note: do not change log_mark3, as the 3rd node is the last node and might have therefore
        # already logged the expected messages before we call node3.mark_log()

        # All nodes must now enable the new features

        debug("Verifying that all nodes enabled the new features")
        node1.watch_log_for([self._audit_native_message,
                             self._auth_native_message,
                             self._version_XX_on_all_nodes.format(release_version, dse_version)],
                            filename='debug.log', from_mark=log_mark1, timeout=10)
        node2.watch_log_for([self._audit_native_message,
                             self._auth_native_message,
                             self._version_XX_on_all_nodes.format(release_version, dse_version)],
                            filename='debug.log', from_mark=log_mark2, timeout=10)

    @since_dse('6.0')
    def general_upgrade_single_node_test(self):
        """
        General, non source version dependent upgrade test for a single node.

        @jira_ticket DB-1597
        """

        dse_version = self._get_dse_version()
        start_version = self._get_previous_version_branch()
        release_version = self._release_version_for_dse_version[dse_version]

        debug("single-node-feature-upgrade-dtest from {} to DSE {}, install-dir {}".format(start_version,
                                                                                           dse_version,
                                                                                           self.cluster.get_install_dir()))

        # save the current install_dir as the target-install_dir as we change the version/install_dir with _prepare_cluster
        target_install_dir = self.cluster.get_install_dir()

        self._prepare_cluster(start_version=start_version, num_nodes=1, config=self._default_config)
        node1 = self.cluster.nodelist()[0]

        # Fix RF of system_auth
        self._setup_auth_schema(node1, repl_factor=1)

        # Upgrade node 1
        log_mark1 = self._upgrade_node(node1, install_dir=target_install_dir, config=self._default_config)

        node1.watch_log_for(self._version_XX_on_all_nodes.format(release_version, dse_version),
                            filename='debug.log', from_mark=log_mark1, timeout=10)

        debug('Bouncing {}'.format(node1.name))
        node1.stop(wait_other_notice=False)
        log_mark1 = node1.mark_log(filename='debug.log')
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Check log messages after bouncing node1
        node1.watch_log_for(self._version_XX_on_all_nodes.format(release_version, dse_version),
                            filename='debug.log', from_mark=log_mark1, timeout=3)

    @since_dse('6.0')
    def general_upgrade_two_nodes_test(self):
        """
        General, non source version dependent upgrade test for a a two node cluster.

        @jira_ticket DB-1597
        """

        dse_version = self._get_dse_version()
        start_version = self._get_previous_version_branch()
        release_version = self._release_version_for_dse_version[dse_version]

        debug("two-node-feature-upgrade-dtest from {} to DSE {}, install-dir {}".format(start_version,
                                                                                        dse_version,
                                                                                        self.cluster.get_install_dir()))

        # save the current install_dir as the target-install_dir as we change the version/install_dir with _prepare_cluster
        target_install_dir = self.cluster.get_install_dir()

        start_release_version = self._prepare_cluster(start_version=start_version, num_nodes=2, config=self._default_config)
        start_dse_version = get_dse_version_from_build_safe(self.cluster.get_install_dir())
        node1, node2 = self.cluster.nodelist()

        # Fix RF of system_auth
        self._setup_auth_schema(node1, repl_factor=2)

        # Upgrade node 1
        log_mark1 = self._upgrade_node(node1, install_dir=target_install_dir, config=self._default_config)

        # Check log messages after upgrading node1
        node1.watch_log_for(self._mixed_versions_XX.format(start_release_version, release_version, start_dse_version, dse_version),
                            filename='debug.log', from_mark=log_mark1, timeout=3)

        # Upgrade node 2 to dse6.0
        log_mark1 = node1.mark_log(filename='debug.log')
        log_mark2 = self._upgrade_node(node2, install_dir=target_install_dir, config=self._default_config)

        # Check log messages after upgrading node2 to dse6.0
        with self.assertRaises(TimeoutError):
            node2.watch_log_for(self._mixed_versions, filename='debug.log', from_mark=log_mark2, timeout=3)
        node2.watch_log_for(self._version_XX_on_all_nodes.format(release_version, dse_version),
                            filename='debug.log', from_mark=log_mark2, timeout=3)
        node1.watch_log_for(self._version_XX_on_all_nodes.format(release_version, dse_version),
                            filename='debug.log', from_mark=log_mark1, timeout=3)

        debug('Bouncing {}'.format(node1.name))
        node1.stop(wait_other_notice=False)
        log_mark1 = node1.mark_log(filename='debug.log')
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # Check log messages after bouncing node1
        node1.watch_log_for(self._version_XX_on_all_nodes.format(release_version, dse_version),
                            filename='debug.log', from_mark=log_mark1, timeout=3)

    @since_dse('6.0')
    def general_upgrade_with_a_dead_node_test(self):
        """
        General, non source version dependent upgrade test for a a three node cluster with a dead node
        assassinated after upgrade of the other two nodes.

        @jira_ticket DB-1597
        """

        dse_version = self._get_dse_version()
        start_version = self._get_previous_version_branch()
        release_version = self._release_version_for_dse_version[dse_version]

        debug("dead-node-feature-upgrade-dtest from {} to DSE {}, install-dir {}".format(start_version,
                                                                                         dse_version,
                                                                                         self.cluster.get_install_dir()))

        # save the current install_dir as the target-install_dir as we change the version/install_dir with _prepare_cluster
        target_install_dir = self.cluster.get_install_dir()

        start_release_version = self._prepare_cluster(start_version=start_version, num_nodes=3, config=self._default_config)
        start_dse_version = get_dse_version_from_build_safe(self.cluster.get_install_dir())
        node1, node2, node3 = self.cluster.nodelist()

        # Fix RF of system_auth
        self._setup_auth_schema(node1, repl_factor=3)

        self._maybe_fake_dse_version([node1, node2, node3], start_dse_version, dse_version)

        # Node 3 is our dead node, that will stay on the previous version and will later be assassinated
        debug("Stopping {}".format(node3.name))
        node3.stop(wait_other_notice=True)

        # Upgrade node 1
        log_mark1 = self._upgrade_node(node1, install_dir=target_install_dir, config=self._default_config)
        log_mark2 = self._upgrade_node(node2, install_dir=target_install_dir, config=self._default_config)

        # Verify that node 1 does NOT enable the new features
        node1.watch_log_for(self._mixed_versions_XX.format(start_release_version, release_version, start_dse_version, dse_version),
                            filename='debug.log', from_mark=log_mark1, timeout=3)
        if start_dse_version < '6.0':
            with self.assertRaises(TimeoutError):
                node1.watch_log_for(self._auth_native_message, filename='debug.log', from_mark=log_mark1, timeout=3)
        else:
            node1.watch_log_for(self._auth_native_message, filename='debug.log', from_mark=log_mark1, timeout=3)

        # Verify that node 2 does NOT enable the new features
        node2.watch_log_for(self._mixed_versions_XX.format(start_release_version, release_version, start_dse_version, dse_version),
                            filename='debug.log', from_mark=log_mark2, timeout=3)
        if start_dse_version < '6.0':
            with self.assertRaises(TimeoutError):
                node2.watch_log_for(self._auth_native_message, filename='debug.log', from_mark=log_mark2, timeout=3)
        else:
            node2.watch_log_for(self._auth_native_message, filename='debug.log', from_mark=log_mark2, timeout=3)

        log_mark1 = node1.mark_log(filename='debug.log')
        log_mark2 = node2.mark_log(filename='debug.log')

        debug("Assassinating {} ...".format(node3.name))
        node1.nodetool("assassinate {}".format(node3.network_interfaces['binary'][0]))
        debug("Assassinated {}".format(node3.name))

        node1.watch_log_for(self._version_XX_on_all_nodes.format(release_version, dse_version),
                            filename='debug.log', from_mark=log_mark1, timeout=10)

        node2.watch_log_for(self._version_XX_on_all_nodes.format(release_version, dse_version),
                            filename='debug.log', from_mark=log_mark2, timeout=3)

    @since_dse('6.0')
    def general_offline_upgrade_test(self):
        """
        General, non source version dependent offline upgrade test.
        - setup an "old" cluster0
        - stop the cluster
        - upgrade the nodes one after each other (update software + start)

        @jira_ticket DB-1597
        """

        dse_version = self._get_dse_version()
        start_version = self._get_previous_version_branch()
        release_version = self._release_version_for_dse_version[dse_version]

        debug("offline-feature-upgrade-dtest from {} to DSE {}, install-dir {}".format(start_version,
                                                                                       dse_version,
                                                                                       self.cluster.get_install_dir()))

        # save the current install_dir as the target-install_dir as we change the version/install_dir with _prepare_cluster
        target_install_dir = self.cluster.get_install_dir()

        start_release_version = self._prepare_cluster(start_version=start_version, num_nodes=3, config=self._default_config)
        start_dse_version = get_dse_version_from_build_safe(self.cluster.get_install_dir())
        node1, node2, node3 = self.cluster.nodelist()

        # Fix RF of system_auth
        self._setup_auth_schema(node1, repl_factor=3)

        self._maybe_fake_dse_version([node1, node2, node3], start_dse_version, dse_version)

        # Node 3 is our dead node, that will stay on the previous version and will later be assassinated
        for node in [node1, node2, node3]:
            debug("Stopping {}".format(node.name))
            node.stop(wait_other_notice=True)

        # Upgrade software of all nodes
        log_mark1 = self._upgrade_node(node1, install_dir=target_install_dir, config=self._default_config, running_node=False)
        log_mark2 = self._upgrade_node(node2, install_dir=target_install_dir, config=self._default_config, running_node=False)
        log_mark3 = self._upgrade_node(node3, install_dir=target_install_dir, config=self._default_config, running_node=False)

        # start nodes 1 - verify log messages
        debug("Starting {}".format(node1.name))
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        node1.watch_log_for(self._mixed_versions_XX.format(start_release_version, release_version, start_dse_version, dse_version),
                            filename='debug.log', from_mark=log_mark1, timeout=3)
        if start_dse_version < '6.0':
            with self.assertRaises(TimeoutError):
                node1.watch_log_for(self._auth_native_message, filename='debug.log', from_mark=log_mark1, timeout=3)
        else:
            node1.watch_log_for(self._auth_native_message, filename='debug.log', from_mark=log_mark1, timeout=3)
        log_mark1 = node1.mark_log(filename='debug.log')

        # start node 2 - verify log messages
        debug("Starting {}".format(node2.name))
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        node2.watch_log_for(self._mixed_versions_XX.format(start_release_version, release_version, start_dse_version, dse_version),
                            filename='debug.log', from_mark=log_mark2, timeout=3)
        if start_dse_version < '6.0':
            with self.assertRaises(TimeoutError):
                node2.watch_log_for(self._auth_native_message, filename='debug.log', from_mark=log_mark2, timeout=3)
        else:
            node2.watch_log_for(self._auth_native_message, filename='debug.log', from_mark=log_mark2, timeout=3)
        log_mark2 = node2.mark_log(filename='debug.log')

        # start node 3 - verify log messages
        debug("Starting {}".format(node3.name))
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)

        # node 3 (the last node) announces it's release-version during startup and therefore the
        # other nodes can enable the new features.
        # node 3 itself may or may not see the new features needed to be disabled.
        node3.watch_log_for(self._version_XX_on_all_nodes.format(release_version, dse_version),
                            filename='debug.log', from_mark=log_mark3, timeout=3)
        # note: do not change log_mark3, as the 3rd node is the last node and might have therefore
        # already logged the expected messages before we call node3.mark_log()

        # All nodes must now enable the new features

        debug("Verifying that all nodes enabled the new features")
        node1.watch_log_for(self._version_XX_on_all_nodes.format(release_version, dse_version),
                            filename='debug.log', from_mark=log_mark1, timeout=10)
        node2.watch_log_for(self._version_XX_on_all_nodes.format(release_version, dse_version),
                            filename='debug.log', from_mark=log_mark2, timeout=10)
