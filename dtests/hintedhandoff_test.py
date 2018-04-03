import os
import time

from dse import ConsistencyLevel, WriteFailure
from nose.plugins.attrib import attr

from dtest import DISABLE_VNODES, Tester, debug
from tools.data import create_c1c2_table, create_ks, insert_c1c2, query_c1c2
from tools.decorators import no_vnodes, since
from tools.jmxutils import (JolokiaAgent, make_mbean,
                            remove_perf_disable_shared_mem)


@since('3.0')
class TestHintedHandoffConfig(Tester):
    """
    Tests the hinted handoff configuration options introduced in
    CASSANDRA-9035.

    @jira_ticket CASSANDRA-9035
    """

    def _start_two_node_cluster(self, config_options=None):
        """
        Start a cluster with two nodes and return them
        """
        cluster = self.cluster

        if config_options:
            cluster.set_configuration_options(values=config_options)

        if DISABLE_VNODES:
            cluster.populate([2]).start()
        else:
            tokens = cluster.balanced_tokens(2)
            cluster.populate([2], tokens=tokens).start()

        return cluster.nodelist()

    def _launch_nodetool_cmd(self, node, cmd):
        """
        Launch a nodetool command and check there is no error, return the result
        """
        out, err, _ = node.nodetool(cmd)
        self.assertEqual('', err)
        return out

    def _do_hinted_handoff(self, node1, node2, enabled, keyspace='ks'):
        """
        Test that if we stop one node the other one
        will store hints only when hinted handoff is enabled
        """
        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, keyspace, 2)
        create_c1c2_table(self, session)

        node2.stop(wait_other_notice=True)

        insert_c1c2(session, n=100, consistency=ConsistencyLevel.ONE)

        log_mark = node1.mark_log()
        node2.start(wait_other_notice=True)

        if enabled:
            node1.watch_log_for(["Finished hinted"], from_mark=log_mark, timeout=120)

        node1.stop(wait_other_notice=True)

        # Check node2 for all the keys that should have been delivered via HH if enabled or not if not enabled
        session = self.patient_exclusive_cql_connection(node2, keyspace=keyspace)
        for n in xrange(0, 100):
            if enabled:
                query_c1c2(session, n, ConsistencyLevel.ONE)
            else:
                query_c1c2(session, n, ConsistencyLevel.ONE, tolerate_missing=True, must_be_missing=True)

    @attr("smoke-test")
    def nodetool_test(self):
        """
        Test various nodetool commands
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': True})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running', res.rstrip())

            self._launch_nodetool_cmd(node, 'disablehandoff')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is not running', res.rstrip())

            self._launch_nodetool_cmd(node, 'enablehandoff')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running', res.rstrip())

            self._launch_nodetool_cmd(node, 'disablehintsfordc dc1')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running{}Data center dc1 is disabled'.format(os.linesep), res.rstrip())

            self._launch_nodetool_cmd(node, 'enablehintsfordc dc1')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running', res.rstrip())

    @attr("smoke-test")
    def hintedhandoff_disabled_test(self):
        """
        Test gloabl hinted handoff disabled
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': False})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is not running', res.rstrip())

        self._do_hinted_handoff(node1, node2, False)

    @attr("smoke-test")
    def hintedhandoff_enabled_test(self):
        """
        Test global hinted handoff enabled
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': True})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running', res.rstrip())

        self._do_hinted_handoff(node1, node2, True)

    @since('4.0')
    def hintedhandoff_setmaxwindow_test(self):
        """
        Test global hinted handoff against max_hint_window_in_ms update via nodetool
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': True, "max_hint_window_in_ms": 300000})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running', res.rstrip())

        res = self._launch_nodetool_cmd(node, 'getmaxhintwindow')
        self.assertEqual('Current max hint window: 300000 ms', res.rstrip())
        self._do_hinted_handoff(node1, node2, True)
        node1.start(wait_other_notice=True)
        self._launch_nodetool_cmd(node, 'setmaxhintwindow 1')
        res = self._launch_nodetool_cmd(node, 'getmaxhintwindow')
        self.assertEqual('Current max hint window: 1 ms', res.rstrip())
        self._do_hinted_handoff(node1, node2, False, keyspace='ks2')

    def hintedhandoff_dc_disabled_test(self):
        """
        Test global hinted handoff enabled with the dc disabled
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': True,
                                                     'hinted_handoff_disabled_datacenters': ['dc1']})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running{}Data center dc1 is disabled'.format(os.linesep), res.rstrip())

        self._do_hinted_handoff(node1, node2, False)

    def hintedhandoff_dc_reenabled_test(self):
        """
        Test global hinted handoff enabled with the dc disabled first and then re-enabled
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': True,
                                                     'hinted_handoff_disabled_datacenters': ['dc1']})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running{}Data center dc1 is disabled'.format(os.linesep), res.rstrip())

        for node in node1, node2:
            self._launch_nodetool_cmd(node, 'enablehintsfordc dc1')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running', res.rstrip())

        self._do_hinted_handoff(node1, node2, True)


class TestHintedHandoff(Tester):

    @no_vnodes()
    def hintedhandoff_decom_test(self):
        self.cluster.populate(4).start()
        [node1, node2, node3, node4] = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 2)
        create_c1c2_table(self, session)
        node4.stop(wait_other_notice=True)
        insert_c1c2(session, n=100, consistency=ConsistencyLevel.ONE)
        node1.decommission()
        node4.start(wait_for_binary_proto=True)

        force = self.cluster.version() >= '3.11'
        node2.decommission(force=force)
        node3.decommission(force=force)

        time.sleep(5)
        for x in xrange(0, 100):
            query_c1c2(session, x, ConsistencyLevel.ONE)

    @since('4.0')
    def hintedhandoff_on_replica_failure_test(self):
        self.ignore_log_patterns = ["RejectedExecutionException"]
        self.cluster.populate(2, install_byteman=True)

        [node1, node2] = self.cluster.nodelist()
        node1.start(wait_for_binary_proto=True, wait_other_notice=True)
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 2)
        create_c1c2_table(self, session)

        # For some reason, the insert could go to any node, so we need to inject both:
        # this is not an issue as the rule will trigger only on the replica, and only once.
        node1.byteman_submit(['./byteman/4.0/write_receive_failure.btm'])
        node2.byteman_submit(['./byteman/4.0/write_receive_failure.btm'])

        try:
            insert_c1c2(session, n=1, consistency=ConsistencyLevel.ALL)
            self.fail("Expected WriteFailure")
        except WriteFailure as ex1:
            self.cluster.wait_for_any_log("Finished hinted", timeout=30)

        node1.stop(wait_other_notice=True)
        session = self.patient_cql_connection(node2)
        session.execute('USE ks')
        query_c1c2(session, 0, ConsistencyLevel.ONE)

    @since('4.0')
    def hintedhandoff_localhost_test(self):
        """
        @jira_ticket: DB-1832
        Verify hints are written for localhost on timeout or failure
        """
        self.ignore_log_patterns = ["InternalRequestExecutionException"]
        self.cluster.populate(1)

        [node1] = self.cluster.nodelist()
        for node in self.cluster.nodelist():
            remove_perf_disable_shared_mem(node)  # necessary for jmx
        node1.start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=["-Dcassandra.test.fail_writes_ks=ks"])

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        create_c1c2_table(self, session)

        with self.assertRaises(WriteFailure):
            insert_c1c2(session, n=1)
        hint_size = self._get_hint_size(node)
        debug("hint size {}".format(hint_size))
        self.assertEquals(1, hint_size, msg="Expect 1 hints written for localhost, but got {}".format(hint_size))

        debug("grep hinted handoff log to {}".format(node1.address()))
        node1.watch_log_for('Finished hinted handoff of file .* to endpoint /{}'.format(node1.address()), filename='system.log')

    def _get_hint_size(self, node):
        mbean = make_mbean('metrics', type='Storage', name='TotalHints')
        with JolokiaAgent(node) as jmx:
            return jmx.read_attribute(mbean, 'Count')
