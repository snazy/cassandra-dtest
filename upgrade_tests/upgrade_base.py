import os
import sys
import time
from abc import ABCMeta
from distutils.version import LooseVersion  # pylint: disable=import-error
from unittest import skipIf

from ccmlib.common import get_version_from_build, is_win

from dtests.dtest import (CASSANDRA_VERSION_FROM_BUILD, DEBUG, TRACE, Tester,
                          debug)
from tools.data import create_ks
from tools.jmxutils import remove_perf_disable_shared_mem


def switch_jdks(major_version_int):
    """
    Changes the jdk version globally, by setting JAVA_HOME = JAVA[N]_HOME.
    This means the environment must have JAVA[N]_HOME set to switch to jdk version N.
    """
    new_java_home = 'JAVA{}_HOME'.format(major_version_int)

    try:
        os.environ[new_java_home]
    except KeyError:
        raise RuntimeError("You need to set {} to run these tests!".format(new_java_home))

    # don't change if the same version was requested
    current_java_home = os.environ.get('JAVA_HOME')
    if current_java_home != os.environ[new_java_home]:
        debug("Switching jdk to version {} (JAVA_HOME is changing from {} to {})".format(major_version_int, current_java_home or 'undefined', os.environ[new_java_home]))
        os.environ['JAVA_HOME'] = os.environ[new_java_home]


@skipIf(sys.platform == 'win32', 'Skip upgrade tests on Windows')
class UpgradeTester(Tester):
    """
    When run in 'normal' upgrade mode without specifying any version to run,
    this will test different upgrade paths depending on what version of C* you
    are testing. When run on 2.1 or 2.2, this will test the upgrade to 3.0.
    When run on 3.0, this will test the upgrade path to trunk. When run on
    versions above 3.0, this will test the upgrade path from 3.0 to HEAD.
    """
    # make this an abc so we can get all subclasses with __subclasses__()
    __metaclass__ = ABCMeta
    NODES, RF, __test__, CL, UPGRADE_PATH = 2, 1, False, None, None

    compact_storage_dropped = False

    # known non-critical bug during teardown:
    # https://issues.apache.org/jira/browse/CASSANDRA-12340
    if CASSANDRA_VERSION_FROM_BUILD < '2.2':
        _known_teardown_race_error = (
            'ScheduledThreadPoolExecutor$ScheduledFutureTask@[0-9a-f]+ '
            'rejected from org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor'
        )
        # don't alter ignore_log_patterns on the class, just the obj for this test
        ignore_log_patterns = [_known_teardown_race_error]

    def __init__(self, *args, **kwargs):
        try:
            self.ignore_log_patterns
        except AttributeError:
            self.ignore_log_patterns = []

        self.ignore_log_patterns = self.ignore_log_patterns[:] + [
            r'RejectedExecutionException.*ThreadPoolExecutor has shut down',  # see  CASSANDRA-12364
        ]

        self.enable_for_jolokia = False
        super(UpgradeTester, self).__init__(*args, **kwargs)

    def setUp(self):
        self.validate_class_config()
        debug("Upgrade test beginning, setting CASSANDRA_VERSION to {}, and jdk to {}. (Prior values will be restored after test)."
              .format(self.UPGRADE_PATH.starting_version, self.UPGRADE_PATH.starting_meta.java_version))
        switch_jdks(self.UPGRADE_PATH.starting_meta.java_version)
        os.environ['CASSANDRA_VERSION'] = self.UPGRADE_PATH.starting_version
        super(UpgradeTester, self).setUp()

    def prepare(self, ordered=False, create_keyspace=True, use_cache=False, use_thrift=False,
                nodes=None, rf=None, protocol_version=None, cl=None, **kwargs):
        nodes = self.NODES if nodes is None else nodes
        rf = self.RF if rf is None else rf

        cl = self.CL if cl is None else cl
        self.CL = cl  # store for later use in do_upgrade

        self.assertGreaterEqual(nodes, 2, "backwards compatibility tests require at least two nodes")

        self.protocol_version = protocol_version

        cluster = self.cluster

        if ordered:
            cluster.set_partitioner("org.apache.cassandra.dht.ByteOrderedPartitioner")

        if use_cache:
            cluster.set_configuration_options(values={'row_cache_size_in_mb': 100})

        if use_thrift:
            cluster.set_configuration_options(values={'start_rpc': 'true'})

        start_rpc = kwargs.pop('start_rpc', False)
        if start_rpc:
            cluster.set_configuration_options(values={'start_rpc': True})

        cluster.set_configuration_options(values={'internode_compression': 'none'})

        cluster.populate(nodes)
        node1 = cluster.nodelist()[0]
        cluster.set_install_dir(version=self.UPGRADE_PATH.starting_version)

        self.enable_for_jolokia = kwargs.pop('jolokia', False)
        if self.enable_for_jolokia:
            remove_perf_disable_shared_mem(node1)

        cluster.start(wait_for_binary_proto=True)

        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        if cl:
            session = self.patient_cql_connection(node1, protocol_version=protocol_version, consistency_level=cl, **kwargs)
        else:
            session = self.patient_cql_connection(node1, protocol_version=protocol_version, **kwargs)
        if create_keyspace:
            create_ks(session, 'ks', rf)

        return session

    def drop_compact_storage_on_all_tables(self, node):
        session = self.patient_exclusive_cql_connection(node)
        for ks in session.cluster.metadata.keyspaces.values():
            if not ks.name.startswith('system'):
                for tab in ks.tables.values():
                    if tab.is_compact_storage:
                        debug("Dropping COMPACT STORAGE from {}.{}".format(ks.name, tab.name))
                        session.execute('ALTER TABLE {}.{} DROP COMPACT STORAGE'.format(ks.name, tab.name))
        session.cluster.shutdown()

    def has_compact_storage_table(self, node):
        session = self.patient_exclusive_cql_connection(node)
        r = False
        for ks in session.cluster.metadata.keyspaces.values():
            if not ks.name.startswith('system'):
                for tab in ks.tables.values():
                    if tab.is_compact_storage:
                        r = True
        session.cluster.shutdown()
        return r

    def maybe_with_compact_storage(self, continuation=False):
        current_is_40 = self.cluster.version() >= "4.0"
        if continuation:
            return "WITH COMPACT STORAGE AND" if not current_is_40 else "WITH"
        return "WITH COMPACT STORAGE" if not current_is_40 else ""

    def do_upgrade(self, session, use_thrift=False, return_nodes=False, **kwargs):
        """
        Upgrades the first node in the cluster and returns a list of
        (is_upgraded, Session) tuples.  If `is_upgraded` is true, the
        Session is connected to the upgraded node. If `return_nodes`
        is True, a tuple of (is_upgraded, Session, Node) will be
        returned instead.
        """
        session.cluster.shutdown()
        node1 = self.cluster.nodelist()[0]
        node2 = self.cluster.nodelist()[1]

        # stop the nodes
        node1.drain()
        node1.stop(gently=True)

        # Ignore errors before upgrade on Windows
        # We ignore errors from 2.1, because windows 2.1
        # support is only beta. There are frequent log errors,
        # related to filesystem interactions that are a direct result
        # of the lack of full functionality on 2.1 Windows, and we dont
        # want these to pollute our results.
        if is_win() and self.cluster.version() <= '2.2':
            node1.mark_log_for_errors()

        # This error is harmless and fixed in CASSANDRA-12133, but we want
        # to detect regressions in newer versions. We should ignore it if
        # the version matches pre- or post-upgrade
        if LooseVersion('3.0') <= get_version_from_build(node1.get_install_dir()) < LooseVersion('3.10'):
            self.ignore_log_patterns.append("Failed to load Java8 implementation ohc-core-j8")

        debug('upgrading node1 to {}'.format(self.UPGRADE_PATH.upgrade_version))
        switch_jdks(self.UPGRADE_PATH.upgrade_meta.java_version)

        previous_version = node1.get_cassandra_version()

        prev_install_dir = node1.get_install_dir()
        node1.set_install_dir(version=self.UPGRADE_PATH.upgrade_version)

        # this is a bandaid; after refactoring, upgrades should account for protocol version
        new_version_from_build = get_version_from_build(node1.get_install_dir())

        # Since 4.0 / DSE 6.0, we have to DROP COMPACT STORAGE on all tables, _before_ the upgrade.
        # However, there is no reliable way to check the version-from-build before it's been applied.
        # In this case, we have to put node1 back to the previous version, start it again, execute the
        # DROP COMPACT STORAGE, and set the install dir again.
        if new_version_from_build >= '4.0' > previous_version and self.has_compact_storage_table(node2):
            debug('Dropping COMPACT STORAGE from some table(s) for the upgrade from {} to {}...'.format(previous_version, new_version_from_build))
            node1.set_install_dir(install_dir=prev_install_dir)
            node1.start(wait_for_binary_proto=True, wait_other_notice=True)
            self.drop_compact_storage_on_all_tables(node1)
            node1.drain()
            node1.stop(gently=True)
            node1.set_install_dir(version=self.UPGRADE_PATH.upgrade_version)
            self.compact_storage_dropped = True

        # Check if a since annotation with a max_version was set on this test.
        # The since decorator can only check the starting version of the upgrade,
        # so here we check to new version of the upgrade as well.
        if hasattr(self, 'max_version') and self.max_version is not None and new_version_from_build >= self.max_version:  # pylint: disable=no-member
            self.skip("Skipping test, new version {} is equal to or higher than max version {}".format(new_version_from_build, self.max_version))  # pylint: disable=no-member

        if (new_version_from_build >= '3' and self.protocol_version is not None and self.protocol_version < 3):
            self.skip('Protocol version {} incompatible '
                      'with Cassandra version {}'.format(self.protocol_version, new_version_from_build))

        # This error is harmless and fixed in CASSANDRA-12133, but we want
        # to detect regressions in newer versions. We should ignore it if
        # the version matches pre- or post-upgrade
        if LooseVersion('3.0') <= new_version_from_build < LooseVersion('3.10'):
            self.ignore_log_patterns.append("Failed to load Java8 implementation ohc-core-j8")

        node1.set_log_level("DEBUG" if DEBUG else "TRACE" if TRACE else "INFO")
        node1.set_configuration_options(values={'internode_compression': 'none'})

        if use_thrift:
            node1.set_configuration_options(values={'start_rpc': 'true'})

        if self.enable_for_jolokia:
            remove_perf_disable_shared_mem(node1)

        node1.start(wait_for_binary_proto=True, wait_other_notice=True)

        sessions_and_meta = []
        if self.CL:
            session = self.patient_exclusive_cql_connection(node1, protocol_version=self.protocol_version, consistency_level=self.CL, **kwargs)
        else:
            session = self.patient_exclusive_cql_connection(node1, protocol_version=self.protocol_version, **kwargs)
        session.set_keyspace('ks')

        if return_nodes:
            sessions_and_meta.append((True, session, node1))
        else:
            sessions_and_meta.append((True, session))

        # open a second session with the node on the old version
        if self.CL:
            session = self.patient_exclusive_cql_connection(node2, protocol_version=self.protocol_version, consistency_level=self.CL, **kwargs)
        else:
            session = self.patient_exclusive_cql_connection(node2, protocol_version=self.protocol_version, **kwargs)
        session.set_keyspace('ks')

        if return_nodes:
            sessions_and_meta.append((False, session, node2))
        else:
            sessions_and_meta.append((False, session))

        # Let the nodes settle briefly before yielding connections in turn (on the upgraded and non-upgraded alike)
        # CASSANDRA-11396 was the impetus for this change, wherein some apparent perf noise was preventing
        # CL.ALL from being reached. The newly upgraded node needs to settle because it has just barely started, and each
        # non-upgraded node needs a chance to settle as well, because the entire cluster (or isolated nodes) may have been doing resource intensive activities
        # immediately before.
        for s in sessions_and_meta:
            time.sleep(5)
            yield s

    def get_version(self):
        node1 = self.cluster.nodelist()[0]
        return node1.get_cassandra_version()

    def get_node_versions(self):
        return [n.get_cassandra_version() for n in self.cluster.nodelist()]

    def node_version_above(self, version):
        return min(self.get_node_versions()) >= version

    def get_node_version(self, is_upgraded):
        """
        Used in places where is_upgraded was used to determine if the node version was >=2.2.
        """
        node_versions = self.get_node_versions()
        self.assertLessEqual(len({v.vstring for v in node_versions}), 2)
        return max(node_versions) if is_upgraded else min(node_versions)

    def tearDown(self):
        # Ignore errors before upgrade on Windows
        # We ignore errors from 2.1, because windows 2.1
        # support is only beta. There are frequent log errors,
        # related to filesystem interactions that are a direct result
        # of the lack of full functionality on 2.1 Windows, and we dont
        # want these to pollute our results.
        if is_win() and self.cluster.version() <= '2.2':
            self.cluster.nodelist()[1].mark_log_for_errors()
        super(UpgradeTester, self).tearDown()

    def validate_class_config(self):
        # check that an upgrade path is specified
        subclasses = self.__class__.__subclasses__()
        no_upgrade_path_error = (
            'No upgrade path specified. {klaus} may not be configured to run as a test.'.format(klaus=self.__class__) +
            (' Did you mean to run one of its subclasses, such as {sub}?'.format(sub=subclasses[0])
             if subclasses else
             '')
        )
        self.assertIsNotNone(self.UPGRADE_PATH, no_upgrade_path_error)
