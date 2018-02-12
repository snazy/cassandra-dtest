import functools
import inspect
import unittest
from distutils.version import LooseVersion
from unittest.case import SkipTest

from nose.plugins.attrib import attr
from nose.tools import assert_in, assert_is_instance

from dtest import DISABLE_VNODES, CASSANDRA_VERSION_FROM_BUILD, get_dse_version_from_build


class since(object):

    def __init__(self, cass_version, max_version=None):
        self.cass_version = LooseVersion(cass_version)
        self.max_version = max_version
        if self.max_version is not None:
            self.max_version = LooseVersion(self.max_version)

    def _skip_msg(self, version):
        if version < self.cass_version:
            return "%s < %s" % (version, self.cass_version)
        if self.max_version and version > self.max_version:
            return "%s > %s" % (version, self.max_version)

    def _wrap_setUpClass(self, cls):
        orig_setUpClass = cls.setUpClass

        @classmethod
        @functools.wraps(cls.setUpClass)
        def wrapped_setUpClass(obj, *args, **kwargs):
            obj.max_version = self.max_version
            version = CASSANDRA_VERSION_FROM_BUILD
            msg = self._skip_msg(version)
            if msg:
                raise SkipTest(msg)
            orig_setUpClass(*args, **kwargs)

        cls.setUpClass = wrapped_setUpClass
        return cls

    def _wrap_function(self, f):
        @functools.wraps(f)
        def wrapped(obj):
            obj.max_version = self.max_version
            version = obj.cluster.version()
            msg = self._skip_msg(version)
            if msg:
                raise SkipTest(msg)
            f(obj)
        return wrapped

    def __call__(self, skippable):
        if inspect.isclass(skippable):
            return self._wrap_setUpClass(skippable)
        return self._wrap_function(skippable)


class since_dse(object):

    def __init__(self, dse_version, max_version=None):
        self.dse_version = LooseVersion(dse_version)
        self.max_version = max_version
        if self.max_version is not None:
            self.max_version = LooseVersion(self.max_version)

    def _skip_msg(self, version):
        if version < self.dse_version:
            return "%s < %s" % (version, self.dse_version)
        if self.max_version and version > self.max_version:
            return "%s > %s" % (version, self.max_version)

    def _wrap_setUpClass(self, cls):
        @classmethod
        @functools.wraps(cls.setUpClass)
        def wrapped_setUpClass(obj, *args, **kwargs):
            raise Exception("We do not have a corresponding DSE_VERSION_FROM_BUILD in dtest/ccm yet")

        cls.setUpClass = wrapped_setUpClass
        return cls

    def _wrap_function(self, f):
        @functools.wraps(f)
        def wrapped(obj):
            obj.max_version = self.max_version
            version = get_dse_version_from_build(obj.cluster.get_install_dir())
            msg = self._skip_msg(version)
            if msg:
                raise SkipTest(msg)
            f(obj)
        return wrapped

    def __call__(self, skippable):
        if inspect.isclass(skippable):
            return self._wrap_setUpClass(skippable)
        return self._wrap_function(skippable)


def no_vnodes():
    """
    Skips the decorated test or test class if using vnodes.
    """
    return unittest.skipIf(not DISABLE_VNODES, 'Test disabled for vnodes')


def known_failure(failure_source, jira_url, flaky=False, notes=''):
    """
    Tag a test as a known failure. Associate it with the URL for a JIRA
    ticket and tag it as flaky or not.

    Valid values for failure_source include: 'cassandra', 'test', 'driver', and
    'systemic'.

    To run all known failures, use the functionality provided by the nosetests
    attrib plugin, using the known_failure attributes:

        # only run tests that are known to fail
        $ nosetests -a known_failure
        # only run tests that are not known to fail
        $ nosetests -a !known_failure
        # only run tests that fail because of cassandra bugs
        $ nosetests -A "'cassandra' in [d['failure_source'] for d in known_failure]"

    Known limitations: a given test may only be tagged once and still work as
    expected with the attrib plugin machinery; if you decorate a test with
    known_failure multiple times, the known_failure attribute of that test
    will have the value applied by the outermost instance of the decorator.
    """
    valid_failure_sources = ('cassandra', 'test', 'systemic', 'driver')

    def wrapper(f):
        assert_in(failure_source, valid_failure_sources)
        assert_is_instance(flaky, bool)

        try:
            existing_failure_annotations = f.known_failure
        except AttributeError:
            existing_failure_annotations = []

        new_annotation = [{'failure_source': failure_source, 'jira_url': jira_url, 'notes': notes, 'flaky': flaky}]

        failure_annotations = existing_failure_annotations + new_annotation

        tagged_func = attr(known_failure=failure_annotations)(f)

        return tagged_func

    return wrapper
