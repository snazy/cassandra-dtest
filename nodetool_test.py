import os
import re

from ccmlib.node import ToolError

from dtest import Tester, debug
from tools.decorators import since


class TestNodetool(Tester):

    def test_decommission_after_drain_is_invalid(self):
        """
        @jira_ticket CASSANDRA-8741

        Running a decommission after a drain should generate
        an unsupported operation message and exit with an error
        code (which we receive as a ToolError exception).
        """
        cluster = self.cluster
        cluster.populate([3]).start()

        node = cluster.nodelist()[0]
        node.drain(block_on_log=True)

        try:
            node.decommission()
            self.assertFalse("Expected nodetool error")
        except ToolError as e:
            self.assertEqual('', e.stderr)
            self.assertTrue('Unsupported operation' in e.stdout)

    def test_sjk(self):
        """
        Verify that SJK generally works.
        """

        cluster = self.cluster
        cluster.populate([1]).start()
        node = cluster.nodelist()[0]

        out, err, _ = node.nodetool('sjk --help')
        debug(out)
        hasPattern = False
        for line in out.split(os.linesep):
            if "    ttop      [Thread Top] Displays threads from JVM process" == line:
                hasPattern = True
        self.assertTrue(hasPattern, "Expected help about SJK ttop")

        out, err, _ = node.nodetool('sjk')
        debug(out)
        hasPattern = False
        for line in out.split(os.linesep):
            if "    ttop      [Thread Top] Displays threads from JVM process" == line:
                hasPattern = True
        self.assertTrue(hasPattern, "Expected help about SJK ttop")

        out, err, _ = node.nodetool('sjk hh -n 10 --live')
        debug(out)
        hasPattern = False
        for line in out.split(os.linesep):
            if re.match('.*Instances.*Bytes.*Type.*', line):
                hasPattern = True
        self.assertTrue(hasPattern, "Expected 'SJK hh' output")

    # DSE 5.1.2
    @since('3.11')
    def test_sequence(self):
        """
        @jira_ticket APOLLO-651

        Verify that command sequences work.

        nodetool sequence options:
            list of commands, one argument (eventually quoted) is one command including its options
            -i <input>      input resource on classpath, file or stdin (use /dev/stdin or "-")
            --stoponerror   Stop on error and do not continue executing the remaining commands
            --failonerror   Let nodetool fail in case of an error
        """

        cluster = self.cluster
        cluster.populate([1]).start()
        node = cluster.nodelist()[0]

        # help

        out, err, _ = node.nodetool('help sequence')
        debug(out)
        lines = out.split(os.linesep)
        self.assertTrue(any(re.match('.*-i <input>, --input <input>', line) for line in lines),
                        'Missing correct output from "nodetool help sequence"')
        self.assertTrue(any(re.match('.*<commands>', line) for line in lines),
                        'Missing correct output from "nodetool help sequence"')

        # 0 commands

        out, err, _ = node.nodetool('sequence')
        debug(out)
        lines = out.split(os.linesep)
        self.assertTrue(any(re.match('.*Executing 0 commands.*', line) for line in lines),
                        'Missing "Executing 0 commands" output from "nodetool sequence" invocation without arguments')
        self.assertTrue(any(re.match('.*Out of 0 commands, 0 completed successfully, 0 failed.*', line) for line in lines),
                        'Missing "Out of 0 commands, 0 completed successfully, 0 failed." output from "nodetool sequence" invocation without arguments')

        # 4 commands via command line

        out, err, _ = node.nodetool('sequence status : gettimeout read : sjk hh -n 5 --live : info')
        debug(out)
        lines = out.split(os.linesep)
        # nodetool sequence header line
        self.assertTrue(any(re.match('.*Executing 4 commands.*', line) for line in lines),
                        'Missing "Executing 4 commands." output from "nodetool sequence" invocation with 4 commands')
        # a line from 'status'
        self.assertTrue(any(re.match('.*State=Normal/Leaving/Joining/Moving.*', line) for line in lines),
                        'Missing output for "status" output from "nodetool sequence" invocation with 4 commands')
        # a line from 'gettimeout'
        self.assertTrue(any(re.match('.*Current timeout for type read: .*', line) for line in lines),
                        'Missing output for "gettimeout" output from "nodetool sequence" invocation with 4 commands')
        # a line from 'info'
        self.assertTrue(any(re.match('.*Native Transport active.*', line) for line in lines),
                        'Missing output for "info" output from "nodetool sequence" invocation with 4 commands')
        # a line from 'sjk ...'
        self.assertTrue(any(re.match('.*Instances          Bytes  Type.*', line) for line in lines),
                        'Missing output for "sjk hh" output from "nodetool sequence" invocation with 4 commands')
        # nodetool sequence summary line
        self.assertTrue(any(re.match('.*Out of 4 commands, 4 completed successfully, 0 failed.*', line) for line in lines),
                        'Missing summary line from "nodetool sequence" invocation with 4 commands')

        # --stoponerror

        out, err, _ = node.nodetool('sequence --stoponerror status : gettimeout eternity : sjk hh -n 5 --live : info')
        debug(out)
        lines = out.split(os.linesep)
        # nodetool sequence header line
        self.assertTrue(any(re.match('.*Executing 4 commands.*', line) for line in lines),
                        'Missing "Executing 4 commands." output from "nodetool sequence" invocation with 4 commands, --stoponerror')
        # a line from 'status'
        self.assertTrue(any(re.match('.*State=Normal/Leaving/Joining/Moving.*', line) for line in lines),
                        'Missing output for "status" output from "nodetool sequence" invocation with 4 commands, --stoponerror')
        # error line from 'gettimeout'
        self.assertTrue(any(re.match('.*Timeout type requires one of.*', line) for line in lines),
                        'Missing output for "gettimeout" output from "nodetool sequence" invocation with 4 commands, --stoponerror')
        # no line from 'info'
        self.assertFalse(any(re.match('.*Native Transport active.*', line) for line in lines),
                         'Missing output for "info" output from "nodetool sequence" invocation with 4 commands, --stoponerror')
        # no line from 'sjk ...'
        self.assertFalse(any(re.match('.*Instances          Bytes  Type.*', line) for line in lines),
                         'Missing output for "sjk hh" output from "nodetool sequence" invocation with 4 commands, --stoponerror')
        # nodetool sequence summary line
        self.assertTrue(any(re.match('.*Out of 4 commands, 1 completed successfully, 1 failed.*', line) for line in lines),
                        'Missing summary line from "nodetool sequence" invocation with 4 commands, --stoponerror')

        # --failonerror

        with self.assertRaises(ToolError):
            node.nodetool('sequence --failonerror status : gettimeout eternity : sjk hh -n 5 --live : info')

        # execute 'support-standard' (contains 35+ commands)

        out, err, _ = node.nodetool('sequence -i support-standard')
        debug(out)
        lines = out.split(os.linesep)
        # nodetool sequence header line
        self.assertTrue(any(re.match('.*Executing 3[0-9] commands.*', line) for line in lines),
                        'Missing "Executing 3? commands." output from "nodetool sequence -i support-standard"')
        # a line from 'gettimeout'
        self.assertTrue(any(re.match('.*Current timeout for type read.*', line) for line in lines),
                        'Missing output for "gettimeout" output from "nodetool sequence -i support-standard"')
        # a line from 'status'
        self.assertTrue(any(re.match('.*State=Normal/Leaving/Joining/Moving.*', line) for line in lines),
                        'Missing output for "status" output from "nodetool sequence"')
        # a line from 'info'
        self.assertTrue(any(re.match('.*Native Transport active.*', line) for line in lines),
                        'Missing output for "info" output from "nodetool sequence"')
        # a line from 'ring'
        self.assertTrue(any(re.match('.*Address.*Rack.*Status.*State.*Load.*Owns.*Token', line) for line in lines),
                        'Missing output for "ring" output from "nodetool sequence"')
        # a line from 'toppartitions 2000'
        self.assertTrue(any(re.match('.*Nothing recorded during sampling period.*', line) for line in lines),
                        'Missing output for "toppartitions" output from "nodetool sequence"')
        # nodetool sequence summary line
        self.assertTrue(any(re.match('.*Out of 3[0-9] commands, 3[0-9] completed successfully, [0-9] failed.*', line) for line in lines),
                        'Missing summary line from "nodetool sequence -i support-standard"')

    def test_correct_dc_rack_in_nodetool_info(self):
        """
        @jira_ticket CASSANDRA-10382

        Test that nodetool info returns the correct rack and dc
        """

        cluster = self.cluster
        cluster.populate([2, 2])
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.GossipingPropertyFileSnitch'})

        for i, node in enumerate(cluster.nodelist()):
            with open(os.path.join(node.get_conf_dir(), 'cassandra-rackdc.properties'), 'w') as snitch_file:
                for line in ["dc={}".format(node.data_center), "rack=rack{}".format(i % 2)]:
                    snitch_file.write(line + os.linesep)

        cluster.start()

        for i, node in enumerate(cluster.nodelist()):
            out, err, _ = node.nodetool('info')
            self.assertEqual(0, len(err), err)
            debug(out)
            for line in out.split(os.linesep):
                if line.startswith('Data Center'):
                    self.assertTrue(line.endswith(node.data_center),
                                    "Expected dc {} for {} but got {}".format(node.data_center, node.address(), line.rsplit(None, 1)[-1]))
                elif line.startswith('Rack'):
                    rack = "rack{}".format(i % 2)
                    self.assertTrue(line.endswith(rack),
                                    "Expected rack {} for {} but got {}".format(rack, node.address(), line.rsplit(None, 1)[-1]))

    @since('3.4')
    def test_nodetool_timeout_commands(self):
        """
        @jira_ticket CASSANDRA-10953

        Test that nodetool gettimeout and settimeout work at a basic level
        """
        cluster = self.cluster
        cluster.populate([1]).start()
        node = cluster.nodelist()[0]

        types = ('read', 'range', 'write', 'counterwrite', 'cascontention',
                 'truncate', 'streamingsocket', 'misc')

        # read all of the timeouts, make sure we get a sane response
        for timeout_type in types:
            out, err, _ = node.nodetool('gettimeout {}'.format(timeout_type))
            self.assertEqual(0, len(err), err)
            debug(out)
            self.assertRegexpMatches(out, r'.* \d+ ms')

        # set all of the timeouts to 123
        for timeout_type in types:
            _, err, _ = node.nodetool('settimeout {} 123'.format(timeout_type))
            self.assertEqual(0, len(err), err)

        # verify that they're all reported as 123
        for timeout_type in types:
            out, err, _ = node.nodetool('gettimeout {}'.format(timeout_type))
            self.assertEqual(0, len(err), err)
            debug(out)
            self.assertRegexpMatches(out, r'.* 123 ms')

    def test_meaningless_notice_in_status(self):
        """
        @jira_ticket CASSANDRA-10176

        nodetool status don't return ownership when there is more than one user keyspace
        define (since they likely have different replication infos making ownership
        meaningless in general) and shows a helpful notice as to why it does that.
        This test checks that said notice is only printed is there is indeed more than
        one user keyspace.
        """
        cluster = self.cluster
        cluster.populate([3]).start()

        node = cluster.nodelist()[0]

        notice_message = r'effective ownership information is meaningless'

        # Do a first try without any keypace, we shouldn't have the notice
        out, err, _ = node.nodetool('status')
        self.assertEqual(0, len(err), err)
        self.assertNotRegexpMatches(out, notice_message)

        session = self.patient_cql_connection(node)
        session.execute("CREATE KEYSPACE ks1 WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1}")

        # With 1 keyspace, we should still not get the notice
        out, err, _ = node.nodetool('status')
        self.assertEqual(0, len(err), err)
        self.assertNotRegexpMatches(out, notice_message)

        session.execute("CREATE KEYSPACE ks2 WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1}")

        # With 2 keyspaces with the same settings, we should not get the notice
        out, err, _ = node.nodetool('status')
        self.assertEqual(0, len(err), err)
        self.assertNotRegexpMatches(out, notice_message)

        session.execute("CREATE KEYSPACE ks3 WITH replication = { 'class':'SimpleStrategy', 'replication_factor':3}")

        # With a keyspace without the same replication factor, we should get the notice
        out, err, _ = node.nodetool('status')
        self.assertEqual(0, len(err), err)
        self.assertRegexpMatches(out, notice_message)

    @since('4.0')
    def test_set_get_batchlog_replay_throttle(self):
        """
        @jira_ticket CASSANDRA-13614

        Test that batchlog replay throttle can be set and get through nodetool
        """
        cluster = self.cluster
        cluster.populate(2)
        node = cluster.nodelist()[0]
        cluster.start()

        # Test that nodetool help messages are displayed
        self.assertTrue('Set batchlog replay throttle' in node.nodetool('help setbatchlogreplaythrottle').stdout)
        self.assertTrue('Print batchlog replay throttle' in node.nodetool('help getbatchlogreplaythrottle').stdout)

        # Set and get throttle with nodetool, ensuring that the rate change is logged
        node.nodetool('setbatchlogreplaythrottle 2048')
        self.assertTrue(len(node.grep_log('Updating batchlog replay throttle to 2048 KB/s, 1024 KB/s per endpoint',
                                          filename='debug.log')) > 0)
        self.assertTrue('Batchlog replay throttle: 2048 KB/s' in node.nodetool('getbatchlogreplaythrottle').stdout)
