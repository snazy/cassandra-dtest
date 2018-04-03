from dtest import Tester
from tools.assertions import assert_invalid, assert_unauthorized
from tools.data import rows_to_list
from tools.decorators import since


class TestSystemKeyspaceFiltering(Tester):
    """
    Filtering on rows in system and system_schema keyspaces.
    @jira_ticket APOLLO-404
    """

    @since("4.0")
    def system_ks_filtering_test(self):
        self._system_auth_ks_is_alterable_test(True)

    @since("4.0")
    def system_ks_no_filtering_test(self):
        self._system_auth_ks_is_alterable_test(True)

    def _system_auth_ks_is_alterable_test(self, with_filtering=True):
        cluster = self.cluster
        config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                  'system_keyspaces_filtering': with_filtering,
                  'permissions_validity_in_ms': 0}
        self.cluster.set_configuration_options(values=config)
        cluster.set_configuration_options(values=config)
        cluster.populate(3).start()
        node = cluster.nodelist()[0]

        self.cluster.wait_for_any_log('Created default superuser', 25)

        session = self.patient_cql_connection(node, user='cassandra', password='cassandra')
        session.execute("""
                        ALTER KEYSPACE system_auth
                            WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};
                        """)

        # Run repair to workaround read repair issues caused by CASSANDRA-10655
        self.cluster.repair()

        for ddl in [
            'CREATE KEYSPACE ks WITH replication={ \'class\' : \'SimpleStrategy\', \'replication_factor\' : 1 }',
            'CREATE TABLE ks.tab1 (id int PRIMARY KEY, val text)',
            'CREATE TABLE ks.tab2 (id int PRIMARY KEY, val text)',
            'CREATE USER one WITH PASSWORD \'one\'',
            'CREATE USER two WITH PASSWORD \'two\'',
            'GRANT CREATE ON ALL KEYSPACES TO one',
            'GRANT CREATE ON ALL KEYSPACES TO two'
        ]:
            session.execute(ddl)

        sessionOne = self.patient_cql_connection(node, user='one', password='one')
        sessionTwo = self.patient_cql_connection(node, user='two', password='two')

        sessionOne.execute(
            'CREATE KEYSPACE ks_one WITH replication={ \'class\' : \'SimpleStrategy\', \'replication_factor\' : 1 }')
        sessionTwo.execute(
            'CREATE KEYSPACE ks_two WITH replication={ \'class\' : \'SimpleStrategy\', \'replication_factor\' : 1 }')

        # users must see "their" keyspaces in metadata
        self.assertTrue('ks_one' in sessionOne.cluster.metadata.keyspaces, 'ks_one metadata for user one')
        self.assertTrue('ks_two' in sessionTwo.cluster.metadata.keyspaces, 'ks_two metadata for user two')
        self.assertEqual(not with_filtering, 'ks_two' in sessionOne.cluster.metadata.keyspaces,
                         'ks_two metadata for user one')
        self.assertEqual(not with_filtering, 'ks_one' in sessionTwo.cluster.metadata.keyspaces,
                         'ks_one metadata for user two')

        # Result set columns: role, username, resource, permission
        self.assertEqual(rows_to_list(session.execute('LIST ALL PERMISSIONS OF one')),
                         [[u'one', u'one', u'<all keyspaces>', u'CREATE', True, False, False],
                          [u'one', u'one', u'<keyspace ks_one>', u'CREATE', True, False, False],
                          [u'one', u'one', u'<keyspace ks_one>', u'ALTER', True, False, False],
                          [u'one', u'one', u'<keyspace ks_one>', u'DROP', True, False, False],
                          [u'one', u'one', u'<keyspace ks_one>', u'SELECT', True, False, False],
                          [u'one', u'one', u'<keyspace ks_one>', u'MODIFY', True, False, False],
                          [u'one', u'one', u'<keyspace ks_one>', u'AUTHORIZE', True, False, False],
                          [u'one', u'one', u'<keyspace ks_one>', u'DESCRIBE', True, False, False],
                          [u'one', u'one', u'<all functions in ks_one>', u'CREATE', True, False, False],
                          [u'one', u'one', u'<all functions in ks_one>', u'ALTER', True, False, False],
                          [u'one', u'one', u'<all functions in ks_one>', u'DROP', True, False, False],
                          [u'one', u'one', u'<all functions in ks_one>', u'AUTHORIZE', True, False, False],
                          [u'one', u'one', u'<all functions in ks_one>', u'EXECUTE', True, False, False]
                          ])
        self.assertEqual(rows_to_list(session.execute('LIST ALL PERMISSIONS OF two')),
                         [[u'two', u'two', u'<all keyspaces>', u'CREATE', True, False, False],
                          [u'two', u'two', u'<keyspace ks_two>', u'CREATE', True, False, False],
                          [u'two', u'two', u'<keyspace ks_two>', u'ALTER', True, False, False],
                          [u'two', u'two', u'<keyspace ks_two>', u'DROP', True, False, False],
                          [u'two', u'two', u'<keyspace ks_two>', u'SELECT', True, False, False],
                          [u'two', u'two', u'<keyspace ks_two>', u'MODIFY', True, False, False],
                          [u'two', u'two', u'<keyspace ks_two>', u'AUTHORIZE', True, False, False],
                          [u'two', u'two', u'<keyspace ks_two>', u'DESCRIBE', True, False, False],
                          [u'two', u'two', u'<all functions in ks_two>', u'CREATE', True, False, False],
                          [u'two', u'two', u'<all functions in ks_two>', u'ALTER', True, False, False],
                          [u'two', u'two', u'<all functions in ks_two>', u'DROP', True, False, False],
                          [u'two', u'two', u'<all functions in ks_two>', u'AUTHORIZE', True, False, False],
                          [u'two', u'two', u'<all functions in ks_two>', u'EXECUTE', True, False, False]
                          ])

        self.assertEqual(rows_to_list(sessionOne.execute('SELECT keyspace_name '
                                                         'FROM system_schema.keyspaces '
                                                         'WHERE keyspace_name IN (\'ks_one\', \'ks_two\')')),
                         [['ks_one']] if with_filtering else [['ks_one', 'ks_two']],
                         'ks_one in system_schema.keyspaces')
        self.assertEqual(rows_to_list(sessionTwo.execute('SELECT keyspace_name '
                                                         'FROM system_schema.keyspaces '
                                                         'WHERE keyspace_name IN (\'ks_one\', \'ks_two\')')),
                         [['ks_two']] if with_filtering else [['ks_one', 'ks_two']],
                         'ks_two in system_schema.keyspaces')

        self.assertTrue('system_distributed' in session.cluster.metadata.keyspaces,
                        'system_distributed metadata, superuser')
        self.assertTrue('system_auth' in session.cluster.metadata.keyspaces, 'system_auth metadata, superuser')
        self.assertTrue('system_traces' in session.cluster.metadata.keyspaces, 'system_traces metadata, superuser')

        # users must see "other" keyspaces in metadata (no system-ks-filtering)
        for userSession in [sessionOne, sessionTwo]:

            # verify special keyspaces

            self.assertTrue('system' in userSession.cluster.metadata.keyspaces, 'system metadata')
            self.assertTrue('system_schema' in userSession.cluster.metadata.keyspaces, 'system_schema metadata')
            self.assertEqual(not with_filtering, 'system_distributed' in userSession.cluster.metadata.keyspaces,
                             'system_distributed metadata')
            self.assertEqual(not with_filtering, 'system_auth' in userSession.cluster.metadata.keyspaces,
                             'system_auth metadata')
            self.assertEqual(not with_filtering, 'system_traces' in userSession.cluster.metadata.keyspaces,
                             'system_traces metadata')

            # users must only see system+schema keypaces (if system-ks-filtering is enabled)

            self.assertEqual(rows_to_list(userSession.execute('SELECT keyspace_name '
                                                              'FROM system_schema.keyspaces '
                                                              'WHERE keyspace_name IN (\'system\', \'system_schema\','
                                                              '\'system_distributed\', \'system_auth\', \'system_traces\')')),
                             [[u'system'], [u'system_schema']]
                             if with_filtering else
                             [[u'system'], [u'system_schema'], [u'system_distributed'], [u'system_auth'], [u'system_traces']])
            self.assertEqual(rows_to_list(userSession.execute('SELECT DISTINCT keyspace_name '
                                                              'FROM system_schema.tables '
                                                              'WHERE keyspace_name IN (\'system\', \'system_schema\','
                                                              '\'system_distributed\', \'system_auth\', \'system_traces\')')),
                             [[u'system'], [u'system_schema']]
                             if with_filtering else
                             [[u'system'], [u'system_schema'], [u'system_distributed'], [u'system_auth'], [u'system_traces']])
            self.assertEqual(rows_to_list(userSession.execute('SELECT DISTINCT keyspace_name '
                                                              'FROM system_schema.columns '
                                                              'WHERE keyspace_name IN (\'system\', \'system_schema\','
                                                              '\'system_distributed\', \'system_auth\', \'system_traces\')')),
                             [[u'system'], [u'system_schema']]
                             if with_filtering else
                             [[u'system'], [u'system_schema'], [u'system_distributed'], [u'system_auth'], [u'system_traces']])

            # tables in 'system' keyspace for which everybody can get the schema information

            for sysTable in [u'local', u'peers',
                             u'sstable_activity', u'size_estimates', u'IndexInfo', u'built_views',
                             u'available_ranges', u'view_builds_in_progress']:
                self.assertTrue(sysTable in userSession.cluster.metadata.keyspaces['system'].tables,
                                '{} in metadata for a user'.format(sysTable))
                self.assertEqual(rows_to_list(userSession.execute('SELECT table_name '
                                                                  'FROM system_schema.tables '
                                                                  'WHERE keyspace_name = \'system\' AND table_name=\'{}\''.
                                                                  format(sysTable))),
                                 [[sysTable]],
                                 '{} in schema tables for a user'.format(sysTable))

            # tables in 'system' keyspace which are protected with system-ks-filtering

            for sysTable in [u'batches', u'paxos', u'peer_events', u'range_xfers', u'compaction_history',
                             u'transferred_ranges', u'prepared_statements', u'repairs']:
                self.assertEqual(not with_filtering,
                                 sysTable in userSession.cluster.metadata.keyspaces['system'].tables,
                                 '{} in metadata for a user'.format(sysTable))
                self.assertEqual(rows_to_list(userSession.execute('SELECT table_name '
                                                                  'FROM system_schema.tables '
                                                                  'WHERE keyspace_name = \'system\' AND table_name=\'{}\''.
                                                                  format(sysTable))),
                                 [] if with_filtering else [[sysTable]],
                                 '{} in schema tables for a user'.format(sysTable))

            # tables in 'system_schema' keyspace for which everybody can get the schema information

            for schemaTable in [u'keyspaces', u'tables', u'columns', u'dropped_columns', u'triggers', u'views',
                                u'types', u'functions', u'aggregates', u'indexes']:
                self.assertTrue(schemaTable in userSession.cluster.metadata.keyspaces['system_schema'].tables,
                                '{} in metadata for a user'.format(schemaTable))
                self.assertEqual(rows_to_list(userSession.execute('SELECT table_name '
                                                                  'FROM system_schema.tables '
                                                                  'WHERE keyspace_name = \'system_schema\' AND table_name=\'{}\''.
                                                                  format(schemaTable))),
                                 [[schemaTable]],
                                 '{} in schema tables for a user'.format(schemaTable))

        sessionOne.execute('CREATE TABLE ks_one.tab1 (id int PRIMARY KEY, val text, to_drop text)')
        sessionOne.cluster.refresh_schema_metadata()
        sessionOne.execute('INSERT INTO ks_one.tab1 (id, val) VALUES (1, \'1\')')
        sessionOne.execute('ALTER TABLE ks_one.tab1 DROP to_drop')
        sessionTwo.execute('CREATE TABLE ks_two.tab2 (id int PRIMARY KEY, val text)')
        sessionOne.cluster.refresh_schema_metadata()
        sessionTwo.execute('INSERT INTO ks_two.tab2 (id, val) VALUES (2, \'2\')')

        for dml in [u'SELECT di, da, do FROM ks_two.nonsense',
                    u'UPDATE ks_two.nonsense SET abc = ? WHERE this_thing = ?',
                    u'SELECT di, da, do FROM ks_two.tab2',
                    u'UPDATE ks_two.tab2 SET abc = ? WHERE this_thing = ?']:
            assert_invalid(sessionOne, dml)

        # GRANT to user two

        sessionOne.execute('GRANT DESCRIBE ON KEYSPACE ks_one TO two')

        # Need to re-create the session for user two, since schema updates are _not_ pushed upon
        # a grant for users who haven't had DESCRIBE permission on a keyspace.
        sessionTwo.shutdown()
        sessionTwo = self.patient_cql_connection(node, user='two', password='two')

        # verify permissions

        self.assertEqual(rows_to_list(session.execute('LIST ALL PERMISSIONS OF two')),
                         [[u'two', u'two', u'<all keyspaces>', u'CREATE', True, False, False],
                          [u'two', u'two', u'<keyspace ks_one>', u'DESCRIBE', True, False, False],
                          [u'two', u'two', u'<keyspace ks_two>', u'CREATE', True, False, False],
                          [u'two', u'two', u'<keyspace ks_two>', u'ALTER', True, False, False],
                          [u'two', u'two', u'<keyspace ks_two>', u'DROP', True, False, False],
                          [u'two', u'two', u'<keyspace ks_two>', u'SELECT', True, False, False],
                          [u'two', u'two', u'<keyspace ks_two>', u'MODIFY', True, False, False],
                          [u'two', u'two', u'<keyspace ks_two>', u'AUTHORIZE', True, False, False],
                          [u'two', u'two', u'<keyspace ks_two>', u'DESCRIBE', True, False, False],
                          [u'two', u'two', u'<table ks_two.tab2>', u'ALTER', True, False, False],
                          [u'two', u'two', u'<table ks_two.tab2>', u'DROP', True, False, False],
                          [u'two', u'two', u'<table ks_two.tab2>', u'SELECT', True, False, False],
                          [u'two', u'two', u'<table ks_two.tab2>', u'MODIFY', True, False, False],
                          [u'two', u'two', u'<table ks_two.tab2>', u'AUTHORIZE', True, False, False],
                          [u'two', u'two', u'<all functions in ks_two>', u'CREATE', True, False, False],
                          [u'two', u'two', u'<all functions in ks_two>', u'ALTER', True, False, False],
                          [u'two', u'two', u'<all functions in ks_two>', u'DROP', True, False, False],
                          [u'two', u'two', u'<all functions in ks_two>', u'AUTHORIZE', True, False, False],
                          [u'two', u'two', u'<all functions in ks_two>', u'EXECUTE', True, False, False]
                          ])

        # verify user 'two' can now see the ks_one keyspace schema information

        self.assertTrue('ks_one' in sessionTwo.cluster.metadata.keyspaces, 'ks_one metadata for user two')

        self.assertEqual(rows_to_list(sessionTwo.execute('SELECT keyspace_name '
                                                         'FROM system_schema.keyspaces '
                                                         'WHERE keyspace_name IN (\'ks_one\', \'ks_two\')')),
                         [[u'ks_one'], [u'ks_two']],
                         'ks_two in system_schema.keyspaces')

        self.assertEqual(rows_to_list(sessionTwo.execute('SELECT keyspace_name '
                                                         'FROM system_schema.keyspaces '
                                                         'WHERE keyspace_name = \'ks_one\'')),
                         [[u'ks_one']],
                         'schema table keyspaces')
        self.assertEqual(rows_to_list(sessionTwo.execute('SELECT keyspace_name, table_name '
                                                         'FROM system_schema.columns '
                                                         'WHERE keyspace_name = \'ks_one\' AND table_name = \'tab1\'')),
                         [[u'ks_one', u'tab1'],  # two columns -> two rows
                          [u'ks_one', u'tab1']],
                         'schema table columns')
        for schemaTable in [u'tables', u'dropped_columns']:
            self.assertEqual(rows_to_list(sessionTwo.execute('SELECT keyspace_name, table_name '
                                                             'FROM system_schema.{} '
                                                             'WHERE keyspace_name = \'ks_one\' AND table_name = \'tab1\''.
                                                             format(schemaTable))),
                             [[u'ks_one', u'tab1']],
                             'schema table {}'.format(schemaTable))

        self.assertTrue('ks_one' in sessionTwo.cluster.metadata.keyspaces, 'ks_one metadata for user two')

        # verify user 'two' can still not read from that table

        assert_unauthorized(sessionTwo,
                            'SELECT * FROM ks_one.tab1',
                            "User two has no SELECT permission on <table ks_one.tab1> or any of its parents")

        # GRANT SELECT permission to user 'two' and verify 'two' can read from that table

        sessionOne.execute('GRANT SELECT ON ks_one.tab1 TO two')
        self.assertEqual(rows_to_list(sessionTwo.execute('SELECT * FROM ks_one.tab1')),
                         [[1, u'1']])
