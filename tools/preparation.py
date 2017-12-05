# coding: utf-8

"""
This module mostly exposes the prepare() method that is meant to perform all
preparation steps needed by most tests, that is starting a cluster (with
potential options) and creating an keyspace.
"""

import copy

from tools.data import create_ks
from tools.interceptors import make_jvm_args
from tools.jmxutils import remove_perf_disable_shared_mem


def prepare(tester,
            nodes=1,
            rf=1,
            ordered=False,
            options={},
            interceptors=None,
            user=None,
            password=None,
            protocol_version=None,
            guarantee_local_reads=False,
            nodesync_options=None,
            byteman=False,
            schema_timeout=10,
            request_timeout=10):
    """
    Prepare a cluster for a given test. This configure and start the cluster
    with the options provided, as well as create a keyspace, and return a
    connection to said cluster.

    @param test the dtest.Tester object for which we should prepare a cluster.
    @param nodes the number of nodes to populate the cluster with
    @param rf the replication factor to use. This can be None, in which case no
              keyspace is created by this method.
    @param ordered whether to use ByteOrderedPartitioner or not.
    @param options specific 'configuration options' to use for the cluster. In
                   many cases, using the config_opts() method provides a
                   convenient way to create a value for this parameters.
    @param interceptors optional interceptor to setup (but not start) on the
                        nodes of the created cluster. This can be a single
                        tools.interceptors.Interceptor object, or a list of
                        them.
    @param user the username to use if authentication is to be used
    @param password the password to use if authentication is to be used
    @param protocol_version an optional specific protocol version to use for
           the connection returned by this method (which is also used to create
           the keyspace in this method)
    @param guarantee_local_reads configure the cluster so that coordinators are
                                 guranteed to pick themselves as primary replica
                                 on reads (assuming they are replica). This is
                                 necessary if you want to do CL.ONE reads on some
                                 node (that you know is a replica for what is
                                 read) and need to guarantee the read is local.
    @param nodesync_options startup options that allow to set NodeSync parameters
                            more suited for testing. This is stictly speaking a
                            list of JVM parameters, but can be more conveniently
                            built using the nodesync_opts method from tools/nodesync.py
    @param byteman if True, setup byteman for the run.
    """

    cluster = tester.cluster

    assert not cluster.nodelist(), "Can't prepare an already populated cluster"

    if ordered:
        cluster.set_partitioner("org.apache.cassandra.dht.ByteOrderedPartitioner")

    cluster.set_configuration_options(values=options)

    if user:
        config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                  'permissions_validity_in_ms': 0}
        cluster.set_configuration_options(values=config)

    # If we want to guarantee that coordinator will always pick themselves
    # first, we need to do 2 things:
    #  1) disable the dynamic snitch
    #  2) pick the PropertyFileSnitch in the single-dc case (in the multi-dc
    #     case, ccm already does that for us). This is because SimpleSnitch
    #     simply return replicas in their order on the ring, so the first
    #     replica depends on token assigments and may not be the coordinator.
    #     PropertyFileSnitch does sort by proximity to the local host always
    #     however.
    if guarantee_local_reads:
        options['dynamic_snitch'] = False
        if isinstance(nodes, int):
            for node in cluster.nodelist():
                node.data_center = 'dc1'
            options['endpoint_snitch'] = 'org.apache.cassandra.locator.PropertyFileSnitch'
        cluster.set_configuration_options(values=options)

    jvm_args = []
    if interceptors:
        jvm_args += make_jvm_args(interceptors)

    if nodesync_options:
        jvm_args += nodesync_options

    cluster.populate(nodes, install_byteman=byteman)

    # We'll use JMX to control interceptors, and what requires this apparently
    if interceptors:
        for node in cluster.nodelist():
            remove_perf_disable_shared_mem(node)

    cluster.start(jvm_args=jvm_args)

    node1 = cluster.nodelist()[0]

    session = tester.patient_cql_connection(node1, protocol_version=protocol_version, user=user, password=password,
                                            schema_timeout=schema_timeout, request_timeout=request_timeout)
    if rf:
        create_ks(session, 'ks', rf)

    return session


def config_opts(use_cache=False, start_thrift=False, num_tokens=None, **kwargs):
    options = copy.deepcopy(kwargs)

    if use_cache:
        options['row_cache_size_in_mb'] = 100
    if start_thrift:
        options['start_rpc'] = True
    if num_tokens:
        options['num_tokens'] = num_tokens

    return options


def get_local_reads_properties():
    """
    If we must read from the local replica first, then we should disable read repair and
    speculative retry, see CASSANDRA-12092
    """
    return " dclocal_read_repair_chance = 0 AND read_repair_chance = 0 AND speculative_retry =  'NONE'"
