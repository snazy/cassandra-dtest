from __future__ import with_statement

import ConfigParser
import copy
import errno
import glob
import logging
import os
import pprint
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import thread
import threading
import time
import traceback
import unittest.case
from collections import OrderedDict
from distutils.version import LooseVersion  # pylint: disable=import-error
from subprocess import CalledProcessError
from unittest import TestCase

import ccmlib.repository
import dse
from ccmlib.cluster import Cluster
from ccmlib.cluster_factory import ClusterFactory
from ccmlib.common import (get_dse_version, get_install_dir_from_cluster_conf,
                           get_version_from_build, is_win)
from dse import ConsistencyLevel
from dse.cluster import EXEC_PROFILE_DEFAULT
from dse.cluster import Cluster as PyCluster
from dse.cluster import ExecutionProfile, NoHostAvailable
from dse.policies import RetryPolicy, WhiteListRoundRobinPolicy
from nose.exc import SkipTest
from six import print_

from plugins.dtestconfig import _CONFIG as CONFIG
from plugins.dtestconfig import GlobalConfigObject
from tools.context import log_filter
from tools.funcutils import merge_dicts, retry_till_success
from tools.misc import (get_auth_provider, get_eager_protocol_version,
                        get_ip_from_node, get_port_from_node)

LOG_SAVED_DIR = "logs"
try:
    os.mkdir(LOG_SAVED_DIR)
except OSError:
    pass

LAST_LOG = os.path.join(LOG_SAVED_DIR, "last")

LAST_TEST_DIR = 'last_test_dir'

DEFAULT_DIR = './'
config = ConfigParser.RawConfigParser()
if len(config.read(os.path.expanduser('~/.apollo-dtest'))) > 0:
    if config.has_option('main', 'default_dir'):
        DEFAULT_DIR = os.path.expanduser(config.get('main', 'default_dir'))
APOLLO_DIR = os.environ.get('APOLLO_DIR', DEFAULT_DIR)

NO_SKIP = os.environ.get('SKIP', '').lower() in ('no', 'false')
DEBUG = os.environ.get('DEBUG', '').lower() in ('yes', 'true')
TRACE = os.environ.get('TRACE', '').lower() in ('yes', 'true')
KEEP_LOGS = os.environ.get('KEEP_LOGS', '').lower() in ('yes', 'true')
KEEP_TEST_DIR = os.environ.get('KEEP_TEST_DIR', '').lower() in ('yes', 'true')
PRINT_DEBUG = os.environ.get('PRINT_DEBUG', '').lower() in ('yes', 'true')
MEMTABLE_TYPE = os.environ.get('MEMTABLE_TYPE', 'heap_buffers').lower()
NUM_TOKENS = os.environ.get('NUM_TOKENS', '256')
RECORD_COVERAGE = os.environ.get('RECORD_COVERAGE', '').lower() in ('yes', 'true')
IGNORE_REQUIRE = os.environ.get('IGNORE_REQUIRE', '').lower() in ('yes', 'true')
DATADIR_COUNT = os.environ.get('DATADIR_COUNT', '3')
ENABLE_ACTIVE_LOG_WATCHING = os.environ.get('ENABLE_ACTIVE_LOG_WATCHING', '').lower() in ('yes', 'true')
RUN_STATIC_UPGRADE_MATRIX = os.environ.get('RUN_STATIC_UPGRADE_MATRIX', '').lower() in ('yes', 'true')

# devault values for configuration from configuration plugin
_default_config = GlobalConfigObject(
    vnodes=True,
)

if CONFIG is None:
    CONFIG = _default_config

DISABLE_VNODES = not CONFIG.vnodes


if os.environ.get('DISABLE_VNODES', '').lower() in ('yes', 'true'):
    print 'DISABLE_VNODES environment variable deprecated. Use `./run_dtests.py --vnodes false` instead.'


CURRENT_TEST = ""

logging.basicConfig(filename=os.path.join(LOG_SAVED_DIR, "dtest.log"),
                    filemode='w',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

LOG = logging.getLogger('dtest')
# set python-driver log level to INFO by default for dtest
logging.getLogger('dse').setLevel(logging.INFO)


def get_sha(repo_dir):
    try:
        output = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo_dir).strip()
        prefix = 'git:'
        local_repo_location = os.environ.get('LOCAL_GIT_REPO')
        if local_repo_location is not None:
            prefix = 'local:{}:'.format(local_repo_location)  # local: slugs take the form 'local:/some/path/to/cassandra/:branch_name_or_sha'
        return "{}{}".format(prefix, output)
    except CalledProcessError as e:
        if re.search('Not a git repository', e.message) is not None:
            # we tried to get a sha, but repo_dir isn't a git repo. No big deal, must just be working from a non-git install.
            return None
        else:
            # git call failed for some unknown reason
            raise


def get_dse_version_from_build(install_dir=None, node_path=None):
    dse_version = get_dse_version_from_build_safe(install_dir=install_dir, node_path=node_path)
    if not dse_version:
        raise Exception("Cannot find DSE version")
    return dse_version


def get_dse_version_from_build_safe(install_dir=None, node_path=None):
    if install_dir is None and node_path is not None:
        install_dir = get_install_dir_from_cluster_conf(node_path)
    if install_dir is not None:
        # Binary cassandra installs will have a 0.version.txt file
        version_file = os.path.join(install_dir, '0.version.txt')
        if os.path.exists(version_file):
            with open(version_file) as f:
                return LooseVersion(f.read().strip())
        # For DSE look for a dse*.jar and extract the version number
        dse_version = get_dse_version(install_dir)
        if (dse_version is not None):
            return LooseVersion(dse_version)
        # Source cassandra installs we can read from build.xml
        build = os.path.join(install_dir, 'build.xml')
        if os.path.exists(build):
            with open(build) as f:
                for line in f:
                    match = re.search('name="base\.dse\.version" value="([0-9.]+)[^"]*"', line)
                    if match:
                        return LooseVersion(match.group(1))
        # if this is a dsedb install from the bdp repo, we just look up the normal dse version
        bdp_dsedb_dir = os.path.join(install_dir, 'dse-db')
        bdp_version_txt = os.path.join(install_dir, 'VERSION.txt')
        if os.path.exists(bdp_dsedb_dir) and os.path.exists(bdp_version_txt):
            with open(bdp_version_txt) as f:
                return LooseVersion(f.read().strip())
    return None


# There are times when we want to know the C* version we're testing against
# before we call Tester.setUp. In the general case, we can't know that -- the
# test method could use any version it wants for self.cluster. However, we can
# get the version from build.xml in the C* repository specified by
# CASSANDRA_VERSION or APOLLO_DIR. This should use the same resolution
# strategy as the actual checkout code in Tester.setUp; if it does not, that is
# a bug.
_cassandra_version_slug = os.environ.get('CASSANDRA_VERSION')
# Prefer CASSANDRA_VERSION if it's set in the environment. If not, use APOLLO_DIR
if _cassandra_version_slug:
    # fetch but don't build the specified C* version
    ccm_repo_cache_dir, _ = ccmlib.repository.setup(_cassandra_version_slug)
    CASSANDRA_VERSION_FROM_BUILD = get_version_from_build(ccm_repo_cache_dir)
    DSE_VERSION_FROM_BUILD = get_dse_version_from_build(ccm_repo_cache_dir)
    CASSANDRA_GITREF = get_sha(ccm_repo_cache_dir)  # will be set None when not a git repo
else:
    CASSANDRA_VERSION_FROM_BUILD = get_version_from_build(APOLLO_DIR)
    DSE_VERSION_FROM_BUILD = get_dse_version_from_build(APOLLO_DIR)
    CASSANDRA_GITREF = get_sha(APOLLO_DIR)


# Determine the location of the libjemalloc jar so that we can specify it
# through environment variables when start Cassandra.  This reduces startup
# time, making the dtests run faster.
def find_libjemalloc():
    if is_win():
        # let the normal bat script handle finding libjemalloc
        return ""

    this_dir = os.path.dirname(os.path.realpath(__file__))
    script = os.path.join(this_dir, "findlibjemalloc.sh")
    try:
        p = subprocess.Popen([script], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if stderr or not stdout:
            return "-"  # tells C* not to look for libjemalloc
        else:
            return stdout
    except Exception as exc:
        print "Failed to run script to prelocate libjemalloc ({}): {}".format(script, exc)
        return ""


CASSANDRA_LIBJEMALLOC = find_libjemalloc()
# copy the initial environment variables so we can reset them later:
initial_environment = copy.deepcopy(os.environ)


class DtestTimeoutError(Exception):
    pass


def reset_environment_vars():
    os.environ.clear()
    os.environ.update(initial_environment)


def warning(msg):
    LOG.warning("{} - {}".format(CURRENT_TEST, msg))
    if PRINT_DEBUG:
        print "WARN: " + msg


def debug(msg):
    LOG.debug("{} - {}".format(CURRENT_TEST, msg))
    if PRINT_DEBUG:
        print msg


debug("Python driver version in use: {}".format(dse.__version__))


class FlakyRetryPolicy(RetryPolicy):
    """
    A retry policy that retries 5 times by default, but can be configured to
    retry more times.
    """

    def __init__(self, max_retries=5):
        self.max_retries = max_retries

    def on_read_timeout(self, *args, **kwargs):
        if kwargs['retry_num'] < self.max_retries:
            debug("Retrying read after timeout. Attempt #" + str(kwargs['retry_num']))
            return (self.RETRY, None)
        else:
            return (self.RETHROW, None)

    def on_write_timeout(self, *args, **kwargs):
        if kwargs['retry_num'] < self.max_retries:
            debug("Retrying write after timeout. Attempt #" + str(kwargs['retry_num']))
            return (self.RETRY, None)
        else:
            return (self.RETHROW, None)

    def on_unavailable(self, *args, **kwargs):
        if kwargs['retry_num'] < self.max_retries:
            debug("Retrying request after UE. Attempt #" + str(kwargs['retry_num']))
            return (self.RETRY, None)
        else:
            return (self.RETHROW, None)


class Runner(threading.Thread):

    def __init__(self, func, endless=False):
        threading.Thread.__init__(self)
        self.__func = func
        self.__error = None
        self.__stopped = False
        self.__endless = endless
        self.daemon = True

    def run(self):
        if not self.__endless:
            try:
                self.__func()
            except Exception as e:
                self.__error = e
        else:
            i = 0
            while True:
                if self.__stopped:
                    return
                try:
                    self.__func(i)
                except Exception as e:
                    self.__error = e
                    return
                i = i + 1

    def stop(self):
        self.__stopped = True
        self.joinAndCheck()

    def joinAndCheck(self):
        self.join()
        self.check()

    def check(self):
        if self.__error is not None:
            raise self.__error  # pylint: disable=raising-bad-type


def make_execution_profile(retry_policy=FlakyRetryPolicy(), consistency_level=ConsistencyLevel.ONE, **kwargs):
    return ExecutionProfile(retry_policy=retry_policy,
                            consistency_level=consistency_level,
                            **kwargs)


class Tester(TestCase):

    maxDiff = None
    allow_log_errors = False  # scan the log of each node for errors after every test.
    cluster_options = None

    def set_node_to_current_version(self, node):
        version = os.environ.get('CASSANDRA_VERSION')
        adir = APOLLO_DIR

        if version:
            node.set_install_dir(version=version)
        else:
            node.set_install_dir(install_dir=adir)

    def init_config(self):
        init_default_config(self.cluster, self.cluster_options)

    def setUp(self):
        self.set_current_tst_name()
        kill_windows_cassandra_procs()
        maybe_cleanup_cluster_from_last_test_file()

        self.test_path = get_test_path()
        self.cluster = create_ccm_cluster(self.test_path, name='test')

        self.maybe_begin_active_log_watch()
        maybe_setup_jacoco(self.test_path)

        self.init_config()
        write_last_test_file(self.test_path, self.cluster)

        set_log_levels(self.cluster)
        self.connections = []
        self.runners = []

    # this is intentionally spelled 'tst' instead of 'test' to avoid
    # making unittest think it's a test method
    def set_current_tst_name(self):
        global CURRENT_TEST
        CURRENT_TEST = self.id()

    def maybe_begin_active_log_watch(self):
        if ENABLE_ACTIVE_LOG_WATCHING:
            if not self.allow_log_errors:
                self.begin_active_log_watch()

    def begin_active_log_watch(self):
        """
        Calls into ccm to start actively watching logs.

        In the event that errors are seen in logs, ccm will call back to _log_error_handler.

        When the cluster is no longer in use, stop_active_log_watch should be called to end log watching.
        (otherwise a 'daemon' thread will (needlessly) run until the process exits).
        """
        # log watching happens in another thread, but we want it to halt the main
        # thread's execution, which we have to do by registering a signal handler
        signal.signal(signal.SIGINT, self._catch_interrupt)
        self._log_watch_thread = self.cluster.actively_watch_logs_for_error(self._log_error_handler, interval=0.25)

    def _log_error_handler(self, errordata):
        """
        Callback handler used in conjunction with begin_active_log_watch.
        When called, prepares exception instance, then will indirectly
        cause _catch_interrupt to be called, which can raise the exception in the main
        program thread.

        @param errordata is a dictonary mapping node name to failure list.
        """
        # in some cases self.allow_log_errors may get set after proactive log checking has been enabled
        # so we need to double-check first thing before proceeding
        if self.allow_log_errors:
            return

        reportable_errordata = OrderedDict()

        for nodename, errors in errordata.items():
            filtered_errors = list(self.__filter_errors(['\n'.join(msg) for msg in errors]))
            if len(filtered_errors) is not 0:
                reportable_errordata[nodename] = filtered_errors

        # no errors worthy of halting the test
        if not reportable_errordata:
            return

        message = "Errors seen in logs for: {nodes}".format(nodes=", ".join(reportable_errordata.keys()))
        for nodename, errors in reportable_errordata.items():
            for error in errors:
                message += "\n{nodename}: {error}".format(nodename=nodename, error=error)

        try:
            debug('Errors were just seen in logs, ending test (if not ending already)!')
            print_("Error details: \n{message}".format(message=message))
            self.test_is_ending  # will raise AttributeError if not present
        except AttributeError:
            self.test_is_ending = True
            self.exit_with_exception = AssertionError("Log error encountered during active log scanning, see stdout")
            # thread.interrupt_main will SIGINT in the main thread, which we can
            # catch to raise an exception with useful information
            thread.interrupt_main()

    """
    Finds files matching the glob pattern specified as argument on
    the given keyspace in all nodes
    """

    def glob_data_dirs(self, path, ks="ks"):
        result = []
        for node in self.cluster.nodelist():
            for data_dir in node.data_directories():
                ks_dir = os.path.join(data_dir, ks, path)
                result.extend(glob.glob(ks_dir))
        return result

    def _catch_interrupt(self, signal, frame):
        """
        Signal handler for registering on SIGINT.

        If called will look for a stored exception and raise it to abort test.
        If a stored exception is not present, this handler has likely caught a
        user interrupt via CTRL-C, and will raise a KeyboardInterrupt.
        """
        try:
            # check if we have a persisted exception to fail with
            raise self.exit_with_exception
        except AttributeError:
            # looks like this was just a plain CTRL-C event
            raise KeyboardInterrupt()

    def copy_logs(self, cluster, directory=None, name=None):
        """Copy the current cluster's log files somewhere, by default to LOG_SAVED_DIR with a name of 'last'"""
        if directory is None:
            directory = LOG_SAVED_DIR
        if name is None:
            name = LAST_LOG
        else:
            name = os.path.join(directory, name)
        if not os.path.exists(directory):
            os.mkdir(directory)
        logs = [(node.name, node.logfilename(), node.debuglogfilename(), node.gclogfilename(), node.compactionlogfilename())
                for node in self.cluster.nodes.values()]
        if len(logs) is not 0:
            basedir = str(int(time.time() * 1000)) + '_' + self.id()
            logdir = os.path.join(directory, basedir)
            os.mkdir(logdir)
            for n, log, debuglog, gclog, compactionlog in logs:
                if os.path.exists(log):
                    self.assertGreaterEqual(os.path.getsize(log), 0)
                    shutil.copyfile(log, os.path.join(logdir, n + ".log"))
                if os.path.exists(debuglog):
                    self.assertGreaterEqual(os.path.getsize(debuglog), 0)
                    shutil.copyfile(debuglog, os.path.join(logdir, n + "_debug.log"))
                if os.path.exists(gclog):
                    self.assertGreaterEqual(os.path.getsize(gclog), 0)
                    shutil.copyfile(gclog, os.path.join(logdir, n + "_gc.log"))
                if os.path.exists(compactionlog):
                    self.assertGreaterEqual(os.path.getsize(compactionlog), 0)
                    shutil.copyfile(compactionlog, os.path.join(logdir, n + "_compaction.log"))
            if os.path.exists(name):
                os.unlink(name)
            if not is_win():
                os.symlink(basedir, name)

    def cql_connection(self, node, keyspace=None, user=None,
                       password=None, compression=True, protocol_version=None, port=None, ssl_opts=None, **kwargs):

        return self._create_session(node, keyspace, user, password, compression,
                                    protocol_version, port=port, ssl_opts=ssl_opts, **kwargs)

    def exclusive_cql_connection(self, node, keyspace=None, user=None,
                                 password=None, compression=True, protocol_version=None, port=None, ssl_opts=None, **kwargs):

        node_ip = get_ip_from_node(node)
        wlrr = WhiteListRoundRobinPolicy([node_ip])

        return self._create_session(node, keyspace, user, password, compression,
                                    protocol_version, port=port, ssl_opts=ssl_opts, load_balancing_policy=wlrr, **kwargs)

    def _create_session(self, node, keyspace, user, password, compression, protocol_version,
                        port=None, ssl_opts=None, execution_profiles=None, schema_timeout=10, **kwargs):

        # up to here, 'node' can be either a single node or a list of nodes

        if isinstance(node, list):
            node_ips = [get_ip_from_node(n) for n in node]
            node = node[0]
        else:
            node_ips = [get_ip_from_node(node)]

        # since here, 'node' refers to the single given node or the first node of the list

        if not port:
            port = get_port_from_node(node)

        if protocol_version is None:
            protocol_version = get_eager_protocol_version(node.cluster.version())

        if user is not None:
            auth_provider = get_auth_provider(user=user, password=password)
        else:
            auth_provider = None

        profiles = {EXEC_PROFILE_DEFAULT: make_execution_profile(**kwargs)
                    } if not execution_profiles else execution_profiles

        cluster = PyCluster(node_ips,
                            auth_provider=auth_provider,
                            compression=compression,
                            protocol_version=protocol_version,
                            port=port,
                            ssl_options=ssl_opts,
                            connect_timeout=10,
                            allow_beta_protocol_version=True,
                            execution_profiles=profiles,
                            max_schema_agreement_wait=schema_timeout)
        session = cluster.connect(wait_for_all_pools=True)

        if keyspace is not None:
            session.set_keyspace(keyspace)

        self.connections.append(session)
        return session

    def node(self, i):
        return self.cluster.nodelist()[i - 1]

    def patient_cql_connection(self, node, keyspace=None,
                               user=None, password=None, timeout=30, compression=True,
                               protocol_version=None, port=None, ssl_opts=None, **kwargs):
        """
        Returns a connection after it stops throwing NoHostAvailables due to not being ready.

        If the timeout is exceeded, the exception is raised.
        """
        if is_win():
            timeout *= 2

        expected_log_lines = ('Control connection failed to connect, shutting down Cluster:', '[control connection] Error connecting to ')
        with log_filter('cassandra.cluster', expected_log_lines):
            session = retry_till_success(
                self.cql_connection,
                node,
                keyspace=keyspace,
                user=user,
                password=password,
                timeout=timeout,
                compression=compression,
                protocol_version=protocol_version,
                port=port,
                ssl_opts=ssl_opts,
                bypassed_exception=NoHostAvailable,
                **kwargs
            )

        return session

    def patient_exclusive_cql_connection(self, node, keyspace=None,
                                         user=None, password=None, timeout=30, compression=True,
                                         protocol_version=None, port=None, ssl_opts=None, **kwargs):
        """
        Returns a connection after it stops throwing NoHostAvailables due to not being ready.

        If the timeout is exceeded, the exception is raised.
        """
        if is_win():
            timeout *= 2

        return retry_till_success(
            self.exclusive_cql_connection,
            node,
            keyspace=keyspace,
            user=user,
            password=password,
            timeout=timeout,
            compression=compression,
            protocol_version=protocol_version,
            port=port,
            ssl_opts=ssl_opts,
            bypassed_exception=NoHostAvailable,
            **kwargs
        )

    @classmethod
    def tearDownClass(cls):
        reset_environment_vars()
        if os.path.exists(LAST_TEST_DIR):
            with open(LAST_TEST_DIR) as f:
                test_path = f.readline().strip('\n')
                name = f.readline()
                try:
                    cluster = ClusterFactory.load(test_path, name)
                    # Avoid waiting too long for node to be marked down
                    if KEEP_TEST_DIR:
                        cluster.stop(gently=RECORD_COVERAGE)
                    else:
                        cluster.remove()
                        os.rmdir(test_path)
                except IOError:
                    # after a restart, /tmp will be emptied so we'll get an IOError when loading the old cluster here
                    pass
            try:
                os.remove(LAST_TEST_DIR)
            except IOError:
                # Ignore - see comment above
                pass

    def tearDown(self):
        # test_is_ending prevents active log watching from being able to interrupt the test
        # which we don't want to happen once tearDown begins
        self.test_is_ending = True

        reset_environment_vars()

        for con in self.connections:
            con.cluster.shutdown()

        for runner in self.runners:
            try:
                runner.stop()
            except Exception:
                pass

        failed = did_fail()
        try:
            if not self.allow_log_errors and self.check_logs_for_errors():
                failed = True
                raise AssertionError('Unexpected error in log, see stdout')
        finally:
            try:
                # save the logs for inspection
                if failed or KEEP_LOGS:
                    self.copy_logs(self.cluster)
            except Exception as e:
                print "Error saving log:", str(e)
            finally:
                log_watch_thread = getattr(self, '_log_watch_thread', None)
                cleanup_cluster(self.cluster, self.test_path, log_watch_thread)

    def check_logs_for_errors(self):
        for node in self.cluster.nodelist():
            errors = list(self.__filter_errors(
                ['\n'.join(msg) for msg in node.grep_log_for_errors()]))
            if len(errors) is not 0:
                for error in errors:
                    print_("Unexpected error in {node_name} log, error: \n{error}".format(node_name=node.name, error=error))
                return True

    def go(self, func, endless=False):
        """
        Runs the given function concurrently in a separate thread.
        The thread is started before the this function returns.

        :param func: the function to be run concurrently in the thread.
                     For endless execution, the function receives the iteration number as the argument.
                     For non-endless executions, the function receives no arguments.
        :param endless: Flag whether the function func shall be executed until explicitly stopped.
        :return: the Runner class instance
        """
        runner = Runner(func, endless=endless)
        self.runners.append(runner)
        runner.start()
        return runner

    def skip(self, msg):
        if not NO_SKIP:
            raise SkipTest(msg)

    def __filter_errors(self, errors):
        """Filter errors, removing those that match self.ignore_log_patterns"""
        if not hasattr(self, 'ignore_log_patterns'):
            self.ignore_log_patterns = []
        for e in errors:
            for pattern in self.ignore_log_patterns:
                if re.search(pattern, e):
                    break
            else:
                yield e

    # Disable docstrings printing in nosetest output
    def shortDescription(self):
        return None

    def get_jfr_jvm_args(self):
        """
        @return The JVM arguments required for attaching flight recorder to a Java process.
        """
        return ["-XX:+UnlockCommercialFeatures", "-XX:+FlightRecorder"]

    def start_jfr_recording(self, nodes):
        """
        Start Java flight recorder provided the cluster was started with the correct jvm arguments.
        """
        for node in nodes:
            p = subprocess.Popen(['jcmd', str(node.pid), 'JFR.start'],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            debug(stdout)
            debug(stderr)

    def dump_jfr_recording(self, nodes):
        """
        Save Java flight recorder results to file for analyzing with mission control.
        """
        for node in nodes:
            p = subprocess.Popen(['jcmd', str(node.pid), 'JFR.dump',
                                  'recording=1', 'filename=recording_{}.jfr'.format(node.address())],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            debug(stdout)
            debug(stderr)


def kill_windows_cassandra_procs():
    # On Windows, forcefully terminate any leftover previously running cassandra processes. This is a temporary
    # workaround until we can determine the cause of intermittent hung-open tests and file-handles.
    if is_win():
        try:
            import psutil
            for proc in psutil.process_iter():
                try:
                    pinfo = proc.as_dict(attrs=['pid', 'name', 'cmdline'])
                except psutil.NoSuchProcess:
                    pass
                else:
                    if (pinfo['name'] == 'java.exe' and '-Dcassandra' in pinfo['cmdline']):
                        print 'Found running cassandra process with pid: ' + str(pinfo['pid']) + '. Killing.'
                        psutil.Process(pinfo['pid']).kill()
        except ImportError:
            debug("WARN: psutil not installed. Cannot detect and kill "
                  "running cassandra processes - you may see cascading dtest failures.")


def get_test_path():
    test_path = tempfile.mkdtemp(prefix='dtest-')

    # ccm on cygwin needs absolute path to directory - it crosses from cygwin space into
    # regular Windows space on wmic calls which will otherwise break pathing
    if sys.platform == "cygwin":
        process = subprocess.Popen(["cygpath", "-m", test_path], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        test_path = process.communicate()[0].rstrip()

    return test_path


# nose will discover this as a test, so we manually make it not a test
get_test_path.__test__ = False


def create_ccm_cluster(test_path, name):
    debug("cluster ccm directory: " + test_path)
    version = os.environ.get('CASSANDRA_VERSION')
    adir = APOLLO_DIR

    if version:
        cluster = Cluster(test_path, name, cassandra_version=version)
    else:
        cluster = Cluster(test_path, name, cassandra_dir=adir)

    if DISABLE_VNODES:
        cluster.set_configuration_options(values={'num_tokens': None})
    else:
        cluster.set_configuration_options(values={'initial_token': None, 'num_tokens': NUM_TOKENS})

    cluster.set_configuration_options(values={'memtable_allocation_type': MEMTABLE_TYPE})

    cluster.set_datadir_count(DATADIR_COUNT)
    cluster.set_environment_variable('CASSANDRA_LIBJEMALLOC', CASSANDRA_LIBJEMALLOC)

    return cluster


def cleanup_cluster(cluster, test_path, log_watch_thread=None):
    with log_filter('cassandra'):  # quiet noise from driver when nodes start going down
        if KEEP_TEST_DIR:
            cluster.stop(gently=RECORD_COVERAGE)
        else:
            # when recording coverage the jvm has to exit normally
            # or the coverage information is not written by the jacoco agent
            # otherwise we can just kill the process
            if RECORD_COVERAGE:
                cluster.stop(gently=True)

            # Cleanup everything:
            try:
                if log_watch_thread:
                    stop_active_log_watch(log_watch_thread)
            finally:
                debug("removing ccm cluster {name} at: {path}".format(name=cluster.name, path=test_path))
                cluster.remove()

                debug("clearing ssl stores from [{0}] directory".format(test_path))
                for filename in ('keystore.jks', 'truststore.jks', 'ccm_node.cer'):
                    try:
                        os.remove(os.path.join(test_path, filename))
                    except OSError as e:
                        # once we port to py3, which has better reporting for exceptions raised while
                        # handling other excpetions, we should just assert e.errno == errno.ENOENT
                        if e.errno != errno.ENOENT:  # ENOENT = no such file or directory
                            raise

                os.rmdir(test_path)
                cleanup_last_test_dir()


def cleanup_last_test_dir():
    if os.path.exists(LAST_TEST_DIR):
        os.remove(LAST_TEST_DIR)


def stop_active_log_watch(log_watch_thread):
    """
    Joins the log watching thread, which will then exit.
    Should be called after each test, ideally after nodes are stopped but before cluster files are removed.

    Can be called multiple times without error.
    If not called, log watching thread will remain running until the parent process exits.
    """
    log_watch_thread.join(timeout=60)


def maybe_cleanup_cluster_from_last_test_file():
    # cleaning up if a previous execution didn't trigger tearDown (which
    # can happen if it is interrupted by KeyboardInterrupt)
    if os.path.exists(LAST_TEST_DIR):
        with open(LAST_TEST_DIR) as f:
            test_path = f.readline().strip('\n')
            name = f.readline()
        try:
            cluster = ClusterFactory.load(test_path, name)
            # Avoid waiting too long for node to be marked down
            cleanup_cluster(cluster, test_path)
        except IOError:
            # after a restart, /tmp will be emptied so we'll get an IOError when loading the old cluster here
            pass


def init_default_config(cluster, cluster_options):
    # the failure detector can be quite slow in such tests with quick start/stop
    phi_values = {'phi_convict_threshold': 5}

    timeout = 10000
    if cluster_options is not None:
        values = merge_dicts(cluster_options, phi_values)
    else:
        values = merge_dicts(phi_values, {
            'read_request_timeout_in_ms': timeout,
            'range_request_timeout_in_ms': timeout,
            'write_request_timeout_in_ms': timeout,
            'truncate_request_timeout_in_ms': timeout,
            'request_timeout_in_ms': timeout
        })

    # No more thrift in 4.0, and start_rpc doesn't exists anymore
    if cluster.version() >= '4' and 'start_rpc' in values:
        del values['start_rpc']

    cluster.set_configuration_options(values)
    debug("Done setting configuration options:\n" + pprint.pformat(cluster._config_options, indent=4))


def write_last_test_file(test_path, cluster):
    with open(LAST_TEST_DIR, 'w') as f:
        f.write(test_path + '\n')
        f.write(cluster.name)


def set_log_levels(cluster):
    if DEBUG:
        cluster.set_log_level("DEBUG")
    if TRACE:
        cluster.set_log_level("TRACE")

    if os.environ.get('DEBUG', 'no').lower() not in ('no', 'false', 'yes', 'true'):
        classes_to_debug = os.environ.get('DEBUG').split(":")
        cluster.set_log_level('DEBUG', None if len(classes_to_debug) == 0 else classes_to_debug)

    if os.environ.get('TRACE', 'no').lower() not in ('no', 'false', 'yes', 'true'):
        classes_to_trace = os.environ.get('TRACE').split(":")
        cluster.set_log_level('TRACE', None if len(classes_to_trace) == 0 else classes_to_trace)


def maybe_setup_jacoco(test_path, cluster_name='test'):
    """Setup JaCoCo code coverage support"""

    if not RECORD_COVERAGE:
        return

    # use explicit agent and execfile locations
    # or look for a cassandra build if they are not specified
    adir = APOLLO_DIR

    agent_location = os.environ.get('JACOCO_AGENT_JAR', os.path.join(adir, 'build/lib/jars/jacocoagent.jar'))
    jacoco_execfile = os.environ.get('JACOCO_EXECFILE', os.path.join(adir, 'build/jacoco/jacoco.exec'))

    if os.path.isfile(agent_location):
        debug("Jacoco agent found at {}".format(agent_location))
        with open(os.path.join(
                test_path, cluster_name, 'cassandra.in.sh'), 'w') as f:

            f.write('JVM_OPTS="$JVM_OPTS -javaagent:{jar_path}=destfile={exec_file}"'
                    .format(jar_path=agent_location, exec_file=jacoco_execfile))

            if os.path.isfile(jacoco_execfile):
                debug("Jacoco execfile found at {}, execution data will be appended".format(jacoco_execfile))
            else:
                debug("Jacoco execfile will be created at {}".format(jacoco_execfile))
    else:
        debug("Jacoco agent not found or is not file. Execution will not be recorded.")


def did_fail():
    if sys.exc_info() == (None, None, None):
        return False

    exc_class, _, _ = sys.exc_info()
    return not issubclass(exc_class, unittest.case.SkipTest)


class ReusableClusterTester(Tester):
    """
    A Tester designed for reusing the same cluster across multiple
    test methods.  This makes test suites with many small tests run
    much, much faster.  However, there are a couple of downsides:

    First, test setup and teardown must be diligent about cleaning
    up any data or schema elements that may interfere with other
    tests.

    Second, errors triggered by one test method may cascade
    into other test failures.  In an attempt to limit this, the
    cluster will be restarted if a test fails or an exception is
    caught.  However, there may still be undetected problems in
    Cassandra that cause cascading failures.
    """

    test_path = None
    cluster = None
    cluster_options = None

    @classmethod
    def setUpClass(cls):
        kill_windows_cassandra_procs()
        maybe_cleanup_cluster_from_last_test_file()
        cls.initialize_cluster()

    def setUp(self):
        self.set_current_tst_name()
        self.connections = []

        # TODO enable active log watching
        # This needs to happen in setUp() and not setUpClass() so that individual
        # test methods can set allow_log_errors and so that error handling
        # only fails a single test method instead of the entire class.
        # The problem with this is that ccm doesn't yet support stopping the
        # active log watcher -- it runs until the cluster is destroyed.  Since
        # we reuse the same cluster, this doesn't work for us.

    def tearDown(self):
        # test_is_ending prevents active log watching from being able to interrupt the test
        self.test_is_ending = True

        failed = did_fail()
        try:
            if not self.allow_log_errors and self.check_logs_for_errors():
                failed = True
                raise AssertionError('Unexpected error in log, see stdout')
        finally:
            try:
                # save the logs for inspection
                if failed or KEEP_LOGS:
                    self.copy_logs(self.cluster)
            except Exception as e:
                print "Error saving log:", str(e)
            finally:
                reset_environment_vars()
                if failed:
                    self.wipe_and_initialize_cluster()

    def wipe_and_initialize_cluster(self):
        cleanup_cluster(self.cluster, self.test_path)
        kill_windows_cassandra_procs()
        self.initialize_cluster()

    @classmethod
    def initialize_cluster(cls):
        """
        This method is responsible for initializing and configuring a ccm
        cluster for the next set of tests.  This can be called for two
        different reasons:
         * A class of tests is starting
         * A test method failed/errored, so the cluster has been wiped

        Subclasses that require custom initialization should generally
        do so by overriding post_initialize_cluster().
        """
        cls.test_path = get_test_path()
        cls.cluster = create_ccm_cluster(cls.test_path, name='test')
        cls.init_config()

        maybe_setup_jacoco(cls.test_path)
        cls.init_config()
        write_last_test_file(cls.test_path, cls.cluster)
        set_log_levels(cls.cluster)

        try:
            cls.post_initialize_cluster()
        except Exception as e:
            try:
                cls.cluster.remove()
            finally:
                raise e

    @classmethod
    def post_initialize_cluster(cls):
        """
        This method is called after the ccm cluster has been created
        and default config options have been applied.  Any custom
        initialization for a test class should generally be done
        here in order to correctly handle cluster restarts after
        test method failures.
        """
        pass

    @classmethod
    def init_config(cls):
        init_default_config(cls.cluster, cls.cluster_options)


class MultiError(Exception):
    """
    Extends Exception to provide reporting multiple exceptions at once.
    """

    def __init__(self, exceptions, tracebacks):
        # an exception and the corresponding traceback should be found at the same
        # position in their respective lists, otherwise __str__ will be incorrect
        self.exceptions = exceptions
        self.tracebacks = tracebacks

    def __str__(self):
        output = "\n****************************** BEGIN MultiError ******************************\n"

        for (exc, tb) in zip(self.exceptions, self.tracebacks):
            output += str(exc)
            output += str(tb) + "\n"

        output += "****************************** END MultiError ******************************"

        return output


def run_scenarios(scenarios, handler, deferred_exceptions=tuple()):
    """
    Runs multiple scenarios from within a single test method.

    "Scenarios" are mini-tests where a common procedure can be reused with several different configurations.
    They are intended for situations where complex/expensive setup isn't required and some shared state is acceptable (or trivial to reset).

    Arguments: scenarios should be an iterable, handler should be a callable, and deferred_exceptions should be a tuple of exceptions which
    are safe to delay until the scenarios are all run. For each item in scenarios, handler(item) will be called in turn.

    Exceptions which occur will be bundled up and raised as a single MultiError exception, either when: a) all scenarios have run,
    or b) on the first exception encountered which is not whitelisted in deferred_exceptions.
    """
    errors = []
    tracebacks = []

    for i, scenario in enumerate(scenarios, 1):
        debug("running scenario {}/{}: {}".format(i, len(scenarios), scenario))

        try:
            handler(scenario)
        except deferred_exceptions as e:
            tracebacks.append(traceback.format_exc(sys.exc_info()))
            errors.append(type(e)('encountered {} {} running scenario:\n  {}\n'.format(e.__class__.__name__, e.message, scenario)))
            debug("scenario {}/{} encountered a deferrable exception, continuing".format(i, len(scenarios)))
        except Exception as e:
            # catch-all for any exceptions not intended to be deferred
            tracebacks.append(traceback.format_exc(sys.exc_info()))
            errors.append(type(e)('encountered {} {} running scenario:\n  {}\n'.format(e.__class__.__name__, e.message, scenario)))
            debug("scenario {}/{} encountered a non-deferrable exception, aborting".format(i, len(scenarios)))
            raise MultiError(errors, tracebacks)

    if errors:
        raise MultiError(errors, tracebacks)


def supports_v5_protocol(cluster_version):
    return cluster_version >= LooseVersion('4.0')
