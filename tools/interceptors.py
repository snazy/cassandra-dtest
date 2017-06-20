# coding: utf-8

"""
Functions to work with Apollo's internode message interceptors (as introduced
by APOLLO-419).

The general usage is that one first creates the interceptor(s) to use using one
of the *_interceptor() method of this module, after which it must make sure
the cluster is started with those interceptors, preferably though the use of
the 'interceptors' parameter of the tools.preparation.prepare() method.

Once set-up, the interceptor can be enable on a node using the
Interceptor.enable() method, which return a session that is meant to be used
though the python context-manager API (using 'with').

Simple example of usage can be found in the interceptors_test.py file.
"""

from enum import Enum
from tools.jmxutils import JolokiaAgent

_PREFIX = "datastax.net.interceptors"
_JMX_TEMPLATE = "com.datastax.net:type=Interceptors,name=%s"


class Verb(Enum):
    """
    The verbs that can be intercepted. Note that those are actually verb
    'groups', which is often more convenient that listing all individual verbs
    to intercept, but one can intercept a single verb using the msg() method
    on one of those verb 'group' to specify the exact verb name.
    """
    READS = 'READS'
    WRITES = 'WRITES'
    LWT = 'LWT'
    HINTS = 'HINTS'
    OPERATIONS = 'OPERATIONS'
    GOSSIP = 'GOSSIP'
    REPAIR = 'REPAIR'
    SCHEMA = 'SCHEMA'

    def msg(self, msg_name):
        return "%s.%s" % (self.value, msg_name)

    def __str__(self):
        return self.value


class Type(Enum):
    """
    The type of messages that can be set for interception.
    """
    REQUEST = 'REQUEST'
    RESPONSE = 'RESPONSE'

    def __str__(self):
        return self.value


class Direction(Enum):
    """
    The direction of messages on a node that can be set for interception.
    """
    SENDING = 'SENDING'
    RECEIVING = 'RECEIVING'

    def __str__(self):
        return self.value


class Locality(Enum):
    """
    The locality of messages that can be set for interception. More precisely,
    intercepting 'LOCAL' message means "locally delivered messages", so message
    that the node sends to itself, while 'REMOTE' is any message that transit
    over the network. In many cases, both of those should be intercepted but
    this allow to intercept only one or the other if necessary.
    """
    LOCAL = 'LOCAL'
    REMOTE = 'REMOTE'

    def __str__(self):
        return self.value


def dropping_interceptor(verbs, name=None):
    """
    Creates a DroppingInterceptor
    verbs is either a single Verb or String or a list of Verbs or Strings
    name is used for interceptor disambiguation
    """
    return Interceptor(_interceptor_name("DroppingInterceptor", name), verbs=verbs)


def delaying_interceptor(delay_ms, verbs, name=None):
    """
    Creates a DelayingInterceptor
    verbs is either a single Verb or String or a list of Verbs or Strings
    name is used for interceptor disambiguation
    """
    delay_str = "-D%s.message_delay_ms=%d" % (_PREFIX, delay_ms)
    return Interceptor(_interceptor_name("DelayingInterceptor", name), verbs=verbs,
                       runtime_properties=[delay_str])


def fake_write_interceptor():
    return Interceptor("FakeWriteInterceptor")


def make_jvm_args(interceptors):
    """
    Generates the proper JVM arguments needed when starting a node or cluster
    so as to setup the provided interceptors (which can either be a single
    interceptor or a list of them). Note that this method always disable the
    interceptors on startup, so it only set them up, and enabling them should
    be instead done through the Interceptor.enable() method.
    """
    if isinstance(interceptors, Interceptor):
        names = [interceptors.name]
        props = interceptors.runtime_properties
    else:
        names = [interceptor.name for interceptor in interceptors]
        props = [p for interceptor in interceptors for p in interceptor.runtime_properties]
    return ["-D%s='%s'" % (_PREFIX, ','.join(names)),
            "-D%s.disable_on_startup=true" % _PREFIX] + props


def _pack(prop):
    return ','.join([str(v) for v in prop])


def _interceptor_name(interceptor_class, name):
    return interceptor_class + "=" + name if name is not None else interceptor_class


class Interceptor:
    def __init__(self, name, verbs=None, runtime_properties=None):
        self.name = name
        self.verbs = verbs if isinstance(verbs, list) or verbs is None else [verbs]
        self.types = None
        self.directions = None
        self.localities = None
        self.runtime_properties = [] if runtime_properties is None else runtime_properties

    def intercept(self, verbs=None, types=None, directions=None, localities=None):
        """
        Configure aspects of what is intercepted.
        """
        if verbs:
            self.verbs = verbs if isinstance(verbs, list) else [verbs]
        if types:
            self.types = types if isinstance(types, list) else [types]
        if directions:
            self.directions = directions if isinstance(directions, list) else [directions]
        if localities:
            self.localities = localities if isinstance(localities, list) else [localities]
        return self

    def enable(self, node):
        """
        Enable the interceptor on the provided node and return the corresponding
        session which is a context manager and should be used through "with".
        For instance:
            with interceptor.enable(node1):
                # do something ....

        On exit, the interception is disabled and the underlying JMX connection
        stopped.
        """
        return self.Session(self, node)

    class Session:
        def __init__(self, interceptor, node):
            self.interceptor = interceptor
            self.node = node
            self.__mbean = _JMX_TEMPLATE % (interceptor.name.split("=")[1] if "=" in interceptor.name else interceptor.name)
            self.__jmx = JolokiaAgent(node)

        def intercepted_count(self):
            return self.__read('InterceptedCount')

        def __set_property(self, jmx_name, prop):
            if prop:
                self.__write(jmx_name, _pack(prop))

        def __enter__(self):
            self.__jmx.start()
            self.__set_property('Intercepted', self.interceptor.verbs)
            self.__set_property('InterceptedTypes', self.interceptor.types)
            self.__set_property('InterceptedDirections', self.interceptor.directions)
            self.__set_property('InterceptedLocalities', self.interceptor.localities)
            self.__execute('enable')
            return self

        def __exit__(self, exc_type, value, traceback):
            self.__execute('disable')
            self.__jmx.stop()
            return exc_type is None

        def __execute(self, method):
            self.__jmx.execute_method(self.__mbean, method)

        def __write(self, attr, value):
            self.__jmx.write_attribute(self.__mbean, attr, value)

        def __read(self, attr):
            return self.__jmx.read_attribute(self.__mbean, attr)
