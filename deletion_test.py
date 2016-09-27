import time

from dtest import Tester, create_ks, create_cf
from tools.data import rows_to_list
from tools.jmxutils import (JolokiaAgent, make_mbean,
                            remove_perf_disable_shared_mem)


class TestDeletion(Tester):

    def gc_test(self):
        """
        Test that tombstone purging doesn't bring back deleted data by writing
        2 rows to a table with gc_grace=0, deleting one of those rows, then
        asserting that it isn't present in the results of SELECT *, before and
        after a flush and compaction.
        """
        cluster = self.cluster

        cluster.populate(1).start()
        [node1] = cluster.nodelist()

        time.sleep(.5)
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        create_cf(session, 'cf', gc_grace=0, key_type='int', columns={'c1': 'int'})

        session.execute('insert into cf (key, c1) values (1,1)')
        session.execute('insert into cf (key, c1) values (2,1)')
        node1.flush()

        self.assertEqual(rows_to_list(session.execute('select * from cf;')),
                         [[1, 1], [2, 1]])

        session.execute('delete from cf where key=1')

        self.assertEqual(rows_to_list(session.execute('select * from cf;')),
                         [[2, 1]])

        node1.flush()
        time.sleep(.5)
        node1.compact()
        time.sleep(.5)

        self.assertEqual(rows_to_list(session.execute('select * from cf;')),
                         [[2, 1]])

    def tombstone_size_test(self):
        self.cluster.populate(1)
        node1 = self.cluster.nodelist()[0]

        remove_perf_disable_shared_mem(node1)

        self.cluster.start(wait_for_binary_proto=True)
        [node1] = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE test (i int PRIMARY KEY)')

        stmt = session.prepare('DELETE FROM test where i = ?')
        for i in range(100):
            session.execute(stmt, [i])

        self.assertEqual(memtable_count(node1, 'ks', 'test'), 100)
        self.assertGreater(memtable_size(node1, 'ks', 'test'), 0)


def memtable_size(node, keyspace, table):
    return table_metric(node, keyspace, table, 'MemtableOnHeapSize')


def memtable_count(node, keyspace, table):
    return table_metric(node, keyspace, table, 'MemtableColumnsCount')


def table_metric(node, keyspace, table, name):
    version = node.get_cassandra_version()
    typeName = "ColumnFamily" if version <= '2.2.X' else 'Table'
    with JolokiaAgent(node) as jmx:
        mbean = make_mbean('metrics', type=typeName,
                           name=name, keyspace=keyspace, scope=table)
        value = jmx.read_attribute(mbean, 'Value')

    return value
