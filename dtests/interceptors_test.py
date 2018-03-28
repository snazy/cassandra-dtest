# coding: utf-8

"""
Tests for the interceptors introduced by APOLLO-419
"""

import time

from dse import ConsistencyLevel, OperationTimedOut, WriteTimeout
from dse.query import SimpleStatement

from dtest import Tester, FlakyRetryPolicy
from tools.assertions import assert_all
from tools.decorators import since
from tools.interceptors import (Direction, Locality, Type, Verb, delaying_interceptor,
                                dropping_interceptor, fake_write_interceptor)
from tools.preparation import get_local_reads_properties, prepare

# A few methods to insert/read from a table. For those tests, we don't really
# care about the details of the table and insertions so we keep it simple. We
# focus writes on a single partition as that makes it a tad easier to reason
# about (no underlying range queries for instance).


def _create_table(session, guarantee_local_reads=False):
    cmd = "CREATE TABLE test (k int, v int, PRIMARY KEY(k, v))"
    if guarantee_local_reads:
        cmd += " WITH " + get_local_reads_properties()
    session.execute(cmd)


def _insert(session, values, cl=ConsistencyLevel.ALL, rp=None):
    query = "INSERT INTO test(k, v) VALUES (0, %i)"
    for v in values:
        if rp is None:
            session.execute(SimpleStatement(query % v, consistency_level=cl))
        else:
            session.execute(SimpleStatement(query % v, consistency_level=cl, retry_policy=rp))


def _assert(session, expected, cl=ConsistencyLevel.ALL):
    query = "SELECT v FROM test WHERE k=0"
    assert_all(session, query, [[x] for x in expected], cl=cl)


no_retry_policy = FlakyRetryPolicy(max_retries=0)


@since('4.0')
class InterceptorsTester(Tester):
    """
    Basic tests of the default interceptors to ensure they do what they are
    supposed to do.
    """

    def dropping_interceptor_test(self):
        """
        Test of the interceptor dropping messages
        """
        interceptor = dropping_interceptor(Verb.WRITES)
        # Only drop received local requests: this is to make the test less fragile.
        # If we weren't doing so, the number of intercepted message would vary based on
        # whether node3 is the coordinator of our intercepted inserts or not. Only
        # dropping just received local requests guarantees we'll only drop 1 message below.
        interceptor.intercept(types=Type.REQUEST, directions=Direction.RECEIVING, localities=Locality.LOCAL)
        session = prepare(self, nodes=3, rf=3, interceptors=interceptor, byteman=True)
        node1, node2, node3 = self.cluster.nodelist()
        node3.byteman_submit(['./byteman/4.0/prevent_default_role_setup_write.btm'])

        _create_table(session)

        # Sanity checks
        _insert(session, range(0, 5))
        _assert(session, range(0, 5))

        # Start the interceptor and check we do timeout (we use CL.ALL)
        with interceptor.enable(node3) as interception:
            with self.assertRaises((OperationTimedOut, WriteTimeout)):
                # Disable the retry policy used by default on dtests, as its triggering will result in unexpected
                # additional inserts that will later fail the interception count assert.
                _insert(session, range(5, 10), cl=ConsistencyLevel.ALL, rp=no_retry_policy)

            # We'll have timeout on the very first insert, so only one message
            # should have been intercepted
            self.assertEqual(interception.intercepted_count(), 1)

    def delaying_interceptor_test(self):
        """
        Test of the interceptor delaying messages
        """
        interceptor = delaying_interceptor(2500, Verb.WRITES)
        session = prepare(self, nodes=3, rf=3, interceptors=interceptor)
        node1, node2, node3 = self.cluster.nodelist()

        _create_table(session)

        # Sanity checks. Note that testing the delaying interceptor is tricky
        # because we're obviously timing sensitive. So we're hoping a single
        # won't even take more than 2.5 seconds even when CI gets very slow.
        # Note: we use 2.5 seconds because the default read timeout is 5 seconds
        start = time.time()
        _insert(session, range(0, 1))
        self.assertLess(time.time() - start, 2.5, msg="""
                A simple insert took more than 2.5 seconds: this is fishy, but
                it could be we just got very unlucky and the environment got
                really really slow on us. So if this happen just once, this can
                probably be ignored""")
        _assert(session, range(0, 1))

        # Start the interceptor with a 2.5 second delay, and check we do get
        # the answer after that time (again, CL.ALL means a single node will
        # slow everything down). Here again, we're sensible to timing in that
        # on a slow environment we might end up timeouting. Hopefully, as the
        # timeout is at 5 seconds, we have a good enough margin in practice
        with interceptor.enable(node3) as interception:
            start = time.time()
            try:
                _insert(session, range(1, 2))
            except (OperationTimedOut, WriteTimeout):
                assert False, """We should have timed out here, but as this test
                    is timing sensitive so if this happen just once, this can be
                    due to a very slow environment and can probably be ignored"""

            # Note that this one is not timing sensitive. If we run in less
            # than the configured delay, that's a bug in the interceptor
            self.assertGreater(time.time() - start, 2.4)  # keeping a 100ms margin of error
            self.assertEqual(interception.intercepted_count(), 1)

            # Sanity check that both our inserts are here
            _assert(session, range(0, 2))

    def fake_write_interceptor_test(self):
        """
        Test of the interceptor dropping writes but fakely responding to them
        """
        interceptor = fake_write_interceptor()
        session = prepare(self, nodes=3, rf=3, interceptors=interceptor, guarantee_local_reads=True)
        node1, node2, node3 = self.cluster.nodelist()

        _create_table(session, guarantee_local_reads=True)

        # Sanity checks
        _insert(session, range(0, 5))
        _assert(session, range(0, 5))

        # Start the interceptor: we shouldn't timeout since writes are still
        # acknowledged. We should however see that despite a successful CL.ALL
        # write, node3 doesn't truly have the inserts
        with interceptor.enable(node3) as interception:
            _insert(session, range(5, 10))

            # Node3 should have intercepted each of its writes
            self.assertEqual(interception.intercepted_count(), 5)

            # Make sure node3 doesn't have the new writes (only the old)
            _assert(self.exclusive_cql_connection(node3, keyspace='ks'), range(0, 5), cl=ConsistencyLevel.ONE)
            # ... but that they are on the other nodes
            _assert(session, range(0, 10))
