import struct
from dtests.cql_tests import CQLTester

from thrift_bindings.v22.ttypes import CfDef, Column, ColumnOrSuperColumn
from thrift_bindings.v22.ttypes import \
    ConsistencyLevel as ThriftConsistencyLevel
from thrift_bindings.v22.ttypes import Mutation
from .thrift_tests import get_thrift_client
from tools.assertions import assert_one
from tools.decorators import since


class ThriftCQLTester(CQLTester):

    @since('2.0', max_version='4')
    def cql3_insert_thrift_test(self):
        """
        Check that we can insert from thrift into a CQL3 table:

        - CREATE a table via CQL
        - insert values via thrift
        - SELECT the inserted values and assert they are there as expected

        @jira_ticket CASSANDRA-4377
        """
        session = self.prepare(start_rpc=True)

        session.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            )
        """)

        node = self.cluster.nodelist()[0]
        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)
        client.transport.open()
        client.set_keyspace('ks')
        key = struct.pack('>i', 2)
        column_name_component = struct.pack('>i', 4)
        # component length + component + EOC + component length + component + EOC
        column_name = '\x00\x04' + column_name_component + '\x00' + '\x00\x01' + 'v' + '\x00'
        value = struct.pack('>i', 8)
        client.batch_mutate(
            {key: {'test': [Mutation(ColumnOrSuperColumn(column=Column(name=column_name, value=value, timestamp=100)))]}},
            ThriftConsistencyLevel.ONE)

        assert_one(session, "SELECT * FROM test", [2, 4, 8])

    @since('2.0', max_version='4')
    def rename_test(self):
        """
        Check that a thrift-created table can be renamed via CQL:

        - create a table via the thrift interface
        - INSERT a row via CQL
        - ALTER the name of the table via CQL
        - SELECT from the table and assert the values inserted are there
        """
        session = self.prepare(start_rpc=True)

        node = self.cluster.nodelist()[0]
        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)
        client.transport.open()

        cfdef = CfDef()
        cfdef.keyspace = 'ks'
        cfdef.name = 'test'
        cfdef.column_type = 'Standard'
        cfdef.comparator_type = 'CompositeType(Int32Type, Int32Type, Int32Type)'
        cfdef.key_validation_class = 'UTF8Type'
        cfdef.default_validation_class = 'UTF8Type'

        client.set_keyspace('ks')
        client.system_add_column_family(cfdef)

        session.execute("INSERT INTO ks.test (key, column1, column2, column3, value) VALUES ('foo', 4, 3, 2, 'bar')")
        session.execute("ALTER TABLE test RENAME column1 TO foo1 AND column2 TO foo2 AND column3 TO foo3")
        assert_one(session, "SELECT foo1, foo2, foo3 FROM test", [4, 3, 2])
