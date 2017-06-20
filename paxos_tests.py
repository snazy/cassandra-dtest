# coding: utf-8

import time
from threading import Thread

from dse import ConsistencyLevel, WriteTimeout
from dse.query import SimpleStatement

from dtest import Tester
from tools.assertions import assert_unavailable
from tools.data import create_ks
from tools.decorators import no_vnodes, since
from tools.interceptors import (delaying_interceptor, dropping_interceptor, Verb, Direction, Type)
from tools.preparation import prepare as standard_prepare


@since('2.0.6')
class TestPaxos(Tester):

    def prepare(self, ordered=False, create_keyspace=True, use_cache=False, nodes=1, rf=1):
        cluster = self.cluster

        if (ordered):
            cluster.set_partitioner("org.apache.cassandra.dht.ByteOrderedPartitioner")

        if (use_cache):
            cluster.set_configuration_options(values={'row_cache_size_in_mb': 100})

        cluster.populate(nodes).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1)
        if create_keyspace:
            create_ks(session, 'ks', rf)
        return session

    def replica_availability_test(self):
        """
        @jira_ticket CASSANDRA-8640

        Regression test for a bug (CASSANDRA-8640) that required all nodes to
        be available in order to run LWT queries, even if the query could
        complete correctly with quorum nodes available.
        """
        session = self.prepare(nodes=3, rf=3)
        session.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")
        session.execute("INSERT INTO test (k, v) VALUES (0, 0) IF NOT EXISTS")

        self.cluster.nodelist()[2].stop()
        session.execute("INSERT INTO test (k, v) VALUES (1, 1) IF NOT EXISTS")

        self.cluster.nodelist()[1].stop()
        assert_unavailable(session.execute, "INSERT INTO test (k, v) VALUES (2, 2) IF NOT EXISTS")

        self.cluster.nodelist()[1].start(wait_for_binary_proto=True, wait_other_notice=True)
        session.execute("INSERT INTO test (k, v) VALUES (3, 3) IF NOT EXISTS")

        self.cluster.nodelist()[2].start(wait_for_binary_proto=True)
        session.execute("INSERT INTO test (k, v) VALUES (4, 4) IF NOT EXISTS")

    @no_vnodes()
    def cluster_availability_test(self):
        # Warning, a change in partitioner or a change in CCM token allocation
        # may require the partition keys of these inserts to be changed.
        # This must not use vnodes as it relies on assumed token values.

        session = self.prepare(nodes=3)
        session.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")
        session.execute("INSERT INTO test (k, v) VALUES (0, 0) IF NOT EXISTS")

        self.cluster.nodelist()[2].stop()
        session.execute("INSERT INTO test (k, v) VALUES (1, 1) IF NOT EXISTS")

        self.cluster.nodelist()[1].stop()
        session.execute("INSERT INTO test (k, v) VALUES (3, 2) IF NOT EXISTS")

        self.cluster.nodelist()[1].start(wait_for_binary_proto=True)
        session.execute("INSERT INTO test (k, v) VALUES (5, 5) IF NOT EXISTS")

        self.cluster.nodelist()[2].start(wait_for_binary_proto=True)
        session.execute("INSERT INTO test (k, v) VALUES (6, 6) IF NOT EXISTS")

    @since('4.0')
    def commit_repair_test(self):
        """
        When "repairing" Paxos commits when beginning a Paxos round, we shouldn't block
        on more responses than requests sent
        @jira_ticket APOLLO-761
        """

        dropping_commit = dropping_interceptor(Verb.LWT.msg("COMMIT"))
        dropping_commit.intercept(types=Type.REQUEST, directions=Direction.RECEIVING)
        dropping_prepare = dropping_interceptor(Verb.LWT.msg("PREPARE"), name="Prepare")
        dropping_prepare.intercept(types=Type.REQUEST, directions=Direction.RECEIVING)
        delaying = delaying_interceptor(500, Verb.LWT.msg("COMMIT"))
        delaying.intercept(types=Type.REQUEST, directions=Direction.RECEIVING)

        standard_prepare(self, nodes=3, rf=3, interceptors=[dropping_commit, dropping_prepare, delaying])
        node1, node2, node3 = self.cluster.nodelist()

        session = self.patient_exclusive_cql_connection(node1, keyspace="ks",
                                                        consistency_level=ConsistencyLevel.QUORUM)

        session.execute("CREATE TABLE test (k int, v int, PRIMARY KEY (k))")

        with dropping_commit.enable(node3) as interception:
            session.execute("INSERT INTO test (k, v) VALUES (0, 0) IF NOT EXISTS")
            # We should intercept one commit to node3 when the Paxos round completes
            self.assertEqual(interception.intercepted_count(), 1)

        # We need to drop the prepare going to node2 to insure that node3 gets the missing
        # commit "repaired"
        with dropping_commit.enable(node1) as interception_1,\
                dropping_prepare.enable(node2) as interception_2_prepare,\
                delaying.enable(node3) as interception_3:
            session.execute("INSERT INTO test (k, v) VALUES (0, 1) IF NOT EXISTS")
            # since the condition does not imply, we would only see commits sent to node1 and node2
            # if they needed to be repaired, which shouldn't be, since the above round completes
            # with a commit CL of QUORUM
            self.assertEqual(interception_1.intercepted_count(), 0)
            self.assertEqual(interception_2_prepare.intercepted_count(), 1)
            self.assertEqual(interception_3.intercepted_count(), 1)

    def contention_test_multi_iterations(self):
        self.skipTest("Hanging the build")
        self._contention_test(8, 100)

    # Warning, this test will require you to raise the open
    # file limit on OSX. Use 'ulimit -n 1000'
    def contention_test_many_threads(self):
        self._contention_test(300, 1)

    def _contention_test(self, threads, iterations):
        """
        Test threads repeatedly contending on the same row.
        """

        verbose = False

        session = self.prepare(nodes=3)
        session.execute("CREATE TABLE test (k int, v int static, id int, PRIMARY KEY (k, id))")
        session.execute("INSERT INTO test(k, v) VALUES (0, 0)")

        class Worker(Thread):

            def __init__(self, wid, session, iterations, query):
                Thread.__init__(self)
                self.wid = wid
                self.iterations = iterations
                self.query = query
                self.session = session
                self.errors = 0
                self.retries = 0

            def run(self):
                global worker_done
                i = 0
                prev = 0
                while i < self.iterations:
                    done = False
                    while not done:
                        try:
                            res = self.session.execute(self.query, (prev + 1, prev, self.wid))
                            if verbose:
                                print "[%3d] CAS %3d -> %3d (res: %s)" % (self.wid, prev, prev + 1, str(res))
                            if res[0][0] is True:
                                done = True
                                prev = prev + 1
                            else:
                                self.retries = self.retries + 1
                                # There is 2 conditions, so 2 reasons to fail: if we failed because the row with our
                                # worker ID already exists, it means we timeout earlier but our update did went in,
                                # so do consider this as a success
                                prev = res[0][3]
                                if res[0][2] is not None:
                                    if verbose:
                                        print "[%3d] Update was inserted on previous try (res = %s)" % (self.wid, str(res))
                                    done = True
                        except WriteTimeout as e:
                            if verbose:
                                print "[%3d] TIMEOUT (%s)" % (self.wid, str(e))
                            # This means a timeout: just retry, if it happens that our update was indeed persisted,
                            # we'll figure it out on the next run.
                            self.retries = self.retries + 1
                        except Exception as e:
                            if verbose:
                                print "[%3d] ERROR: %s" % (self.wid, str(e))
                            self.errors = self.errors + 1
                            done = True
                    i = i + 1
                    # Clean up for next iteration
                    while True:
                        try:
                            self.session.execute("DELETE FROM test WHERE k = 0 AND id = %d IF EXISTS" % self.wid)
                            break
                        except WriteTimeout as e:
                            pass

        nodes = self.cluster.nodelist()
        workers = []

        c = self.patient_cql_connection(nodes[0], keyspace='ks')
        q = c.prepare("""
                BEGIN BATCH
                   UPDATE test SET v = ? WHERE k = 0 IF v = ?;
                   INSERT INTO test (k, id) VALUES (0, ?) IF NOT EXISTS;
                APPLY BATCH
            """)

        for n in range(0, threads):
            workers.append(Worker(n, c, iterations, q))

        start = time.time()

        for w in workers:
            w.start()

        for w in workers:
            w.join()

        if verbose:
            runtime = time.time() - start
            print "runtime:", runtime

        query = SimpleStatement("SELECT v FROM test WHERE k = 0", consistency_level=ConsistencyLevel.ALL)
        rows = session.execute(query)
        value = rows[0][0]

        errors = 0
        retries = 0
        for w in workers:
            errors = errors + w.errors
            retries = retries + w.retries

        self.assertTrue((value == threads * iterations) and (errors == 0), "value={}, errors={}, retries={}".format(value, errors, retries))
