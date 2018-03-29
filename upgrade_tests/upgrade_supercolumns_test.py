import os
from collections import OrderedDict
from unittest import skipUnless

from dse.query import dict_factory
from dse.util import OrderedMap
from nose.tools import assert_equal, assert_not_in
from pycassa.columnfamily import ColumnFamily
from pycassa.pool import ConnectionPool
from dtests.thrift_tests import get_thrift_client, _i64

from dtests.dtest import CASSANDRA_VERSION_FROM_BUILD, RUN_STATIC_UPGRADE_MATRIX, Tester, debug
from thrift_bindings.v22 import Cassandra
from thrift_bindings.v22.Cassandra import (Column, ColumnParent, SuperColumn, ConsistencyLevel)
from tools.assertions import assert_all
from upgrade_base import UpgradeTester
from upgrade_manifest import build_upgrade_pairs, MANIFEST, indev_dse_5_1

# Use static supercolumn data to reduce total test time and avoid driver issues connecting to C* 1.2.
# The data contained in the SSTables is (name, {'attr': {'name': name}}) for the name in NAMES.
SCHEMA_PATH = os.path.join("./", "upgrade_tests", "supercolumn-data", "cassandra-2.0", "schema-2.0.cql")
TABLES_PATH = os.path.join("./", "upgrade_tests", "supercolumn-data", "cassandra-2.0", "supcols", "cols")
NAMES = ["Alice", "Bob", "Claire", "Dave", "Ed", "Frank", "Grace"]


def _create_super_cf(name):
    return Cassandra.KsDef('keyspace1', 'org.apache.cassandra.locator.SimpleStrategy', {'replication_factor': '1'},
                           cf_defs=[Cassandra.CfDef('keyspace1', name, column_type='Super', subcomparator_type='LongType')])


def _insert_supercolumn_data(client, cf_name):
    columns = [SuperColumn(name='sc1', columns=[Column(_i64(1), 'value1', 0)]),
               SuperColumn(name='sc2',
                           columns=[Column(_i64(2), 'value2', 0), Column(_i64(3), 'value3', 0)])]

    for column in columns:
        for subcolumn in column.columns:
            client.insert('key', ColumnParent(cf_name, column.name), subcolumn, ConsistencyLevel.ONE)


def _validate_cql_empty_selector(cursor, ks='keyspace1', cf='sc'):
    rows = list(cursor.execute('SELECT "" FROM {}.{}'.format(ks, cf)))
    orderedmap_rows = [{k: OrderedMap(v) for k, v in row.items()} for row in rows]
    assert_equal(orderedmap_rows,
                 [{"": OrderedMap({1: 'value1'})},
                  {"": OrderedMap({2: 'value2', 3: 'value3'})}])


class TestSCUpgrade(Tester):
    """
    Tests upgrade between 1.2->2.0 for super columns (since that's where we
    removed then internally).
    """

    def __init__(self, *args, **kwargs):
        self.ignore_log_patterns = [
            # This one occurs if we do a non-rolling upgrade, the node
            # it's trying to send the migration to hasn't started yet,
            # and when it does, it gets replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
        ]
        if CASSANDRA_VERSION_FROM_BUILD < '2.2':
            _known_teardown_race_error = (
                'ScheduledThreadPoolExecutor$ScheduledFutureTask@[0-9a-f]+ '
                'rejected from org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor'
            )
            # don't alter ignore_log_patterns on the class, just the obj for this test
            self.ignore_log_patterns += [_known_teardown_race_error]

        Tester.__init__(self, *args, **kwargs)

    def prepare(self, num_nodes=1, cassandra_version="github:apache/cassandra-2.1"):
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_install_dir(version=cassandra_version)
        if "memtable_allocation_type" in cluster._config_options:
            del cluster._config_options['memtable_allocation_type']
        cluster.populate(num_nodes).start()

        return cluster

    def verify_with_thrift(self):
        # No more thrift in 4.0
        if self.cluster.version() >= '4':
            return

        pool = ConnectionPool("supcols", pool_size=1)
        super_col_fam = ColumnFamily(pool, "cols")
        for name in NAMES:
            super_col_value = super_col_fam.get(name)
            self.assertEqual(OrderedDict([(('attr', u'name'), name)]), super_col_value)

    def verify_with_cql(self, session):
        session.execute("USE supcols")
        expected = [[name, 'attr', u'name', name] for name in ['Grace', 'Claire', 'Dave', 'Frank', 'Ed', 'Bob', 'Alice']]
        assert_all(session, "SELECT * FROM cols", expected)

    def _upgrade_super_columns_through_versions_test(self, upgrade_path, drop_cs_after):
        cluster = self.prepare()
        node1 = cluster.nodelist()[0]
        node1.run_cqlsh(cmds="""CREATE KEYSPACE supcols WITH replication = {
                                    'class': 'SimpleStrategy',
                                    'replication_factor': '1'
                                  };

                                USE supcols;

                                CREATE TABLE cols (
                                  key blob,
                                  column1 blob,
                                  column2 text,
                                  value blob,
                                  PRIMARY KEY ((key), column1, column2)
                                ) WITH COMPACT STORAGE AND
                                  bloom_filter_fp_chance=0.010000 AND
                                  caching='KEYS_ONLY' AND
                                  comment='' AND
                                  dclocal_read_repair_chance=0.000000 AND
                                  gc_grace_seconds=864000 AND
                                  index_interval=128 AND
                                  read_repair_chance=0.100000 AND
                                  replicate_on_write='true' AND
                                  populate_io_cache_on_flush='false' AND
                                  default_time_to_live=0 AND
                                  speculative_retry='99.0PERCENTILE' AND
                                  memtable_flush_period_in_ms=0 AND
                                  compaction={'class': 'SizeTieredCompactionStrategy'} AND
                                  compression={'sstable_compression': 'SnappyCompressor'};
                              """)
        node1.bulkload(options=[TABLES_PATH])
        node1.nodetool("upgradesstables -a")

        session = self.patient_exclusive_cql_connection(node1)

        self.verify_with_cql(session)
        self.verify_with_thrift()

        for version in upgrade_path:
            self.upgrade_to_version(version)

            if self.cluster.version() < '4':
                node1.nodetool("enablethrift")

            session = self.patient_exclusive_cql_connection(node1)

            self.verify_with_cql(session)
            self.verify_with_thrift()

            if version == drop_cs_after:
                session.execute("ALTER TABLE supcols.cols DROP COMPACT STORAGE")

        cluster.remove(node=node1)

    def upgrade_super_columns_through_all_versions_test(self):
        self._upgrade_super_columns_through_versions_test(upgrade_path=['alias:apollo/cassandra-2.1_dse', 'alias:apollo/dse5.0',
                                                                        'alias:apollo/dse5.1', 'alias:apollo/dse6.0'],
                                                          drop_cs_after='alias:apollo/dse5.1')

    def upgrade_super_columns_through_limited_versions_test(self):
        self._upgrade_super_columns_through_versions_test(upgrade_path=['alias:apollo/dse5.0', 'alias:apollo/dse6.0'],
                                                          drop_cs_after='alias:apollo/dse5.0')

    def upgrade_to_version(self, tag, nodes=None):
        debug('Upgrading to ' + tag)
        if nodes is None:
            nodes = self.cluster.nodelist()

        for node in nodes:
            debug('Shutting down node: ' + node.name)
            node.drain()
            node.watch_log_for("DRAINED")
            node.stop(wait_other_notice=False)

        # Update Cassandra Directory
        for node in nodes:
            node.set_install_dir(version=tag)
            if tag < "2.1":
                if "memtable_allocation_type" in node.config_options:
                    del node.config_options['memtable_allocation_type']
            debug("Set new cassandra dir for %s: %s" % (node.name, node.get_install_dir()))
        self.cluster.set_install_dir(version=tag)

        # Restart nodes on new version
        for node in nodes:
            debug('Starting %s on new version (%s)' % (node.name, tag))
            # Setup log4j / logback again (necessary moving from 2.0 -> 2.1):
            node.set_log_level("INFO")
            node.start(wait_other_notice=True, wait_for_binary_proto=True)
            node.nodetool('upgradesstables -a')


class SuperColumnDropCompactStorageTest(UpgradeTester):
    # Compact Storage
    def supercolumn_drop_compact_storage_upgrade_test(self):
        cluster = self.prepare()
        node = self.cluster.nodelist()[0]
        node.nodetool("enablethrift")

        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)

        client.transport.open()

        cf_name = 'sc'

        client.system_add_keyspace(_create_super_cf(cf_name))

        client.set_keyspace('keyspace1')

        _insert_supercolumn_data(client, cf_name)

        cursor = self.patient_cql_connection(node, row_factory=dict_factory)

        cursor.execute("ALTER TABLE {}.{} DROP COMPACT STORAGE".format('keyspace1', cf_name))

        _validate_cql_empty_selector(cursor, cf=cf_name)

        for is_upgraded, cursor in self.do_upgrade(cursor):
            cursor = self.patient_cql_connection(node, row_factory=dict_factory)

            _validate_cql_empty_selector(cursor, cf=cf_name)


for path in build_upgrade_pairs({indev_dse_5_1: MANIFEST[indev_dse_5_1]}):
    gen_class_name = SuperColumnDropCompactStorageTest.__name__ + path.name
    assert_not_in(gen_class_name, globals())
    spec = {'UPGRADE_PATH': path,
            '__test__': True}

    upgrade_applies_to_env = RUN_STATIC_UPGRADE_MATRIX or path.upgrade_meta.matches_current_env_version_family
    globals()[gen_class_name] = skipUnless(upgrade_applies_to_env, 'test not applicable to env.')(type(gen_class_name, (SuperColumnDropCompactStorageTest,), spec))
