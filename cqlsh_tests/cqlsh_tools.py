import csv
import random

import dse
from nose.tools import assert_items_equal


class DummyColorMap(object):

    def __getitem__(self, *args):
        return ''


def csv_rows(filename, delimiter=None):
    """
    Given a filename, opens a csv file and yields it line by line.
    """
    reader_opts = {}
    if delimiter is not None:
        reader_opts['delimiter'] = delimiter
    with open(filename, 'rb') as csvfile:
        for row in csv.reader(csvfile, **reader_opts):
            yield row


def assert_csvs_items_equal(filename1, filename2):
    with open(filename1, 'r') as x, open(filename2, 'r') as y:
        assert_items_equal(list(x.readlines()), list(y.readlines()))


def random_list(gen=None, n=None):
    if gen is None:
        def gen():
            return random.randint(-1000, 1000)
    if n is None:
        def length():
            return random.randint(1, 5)
    else:
        def length():
            return n

    return [gen() for _ in range(length())]


def write_rows_to_csv(filename, data):
    with open(filename, 'wb') as csvfile:
        writer = csv.writer(csvfile)
        for row in data:
            writer.writerow(row)
        csvfile.close


def deserialize_date_fallback_int(byts, protocol_version):
    timestamp_ms = dse.marshal.int64_unpack(byts)
    try:
        return dse.util.datetime_from_timestamp(timestamp_ms / 1000.0)
    except OverflowError:
        return timestamp_ms


def monkeypatch_driver():
    """
    Monkeypatches the `dse` driver module in the same way
    that cqlsh does. Returns a dictionary containing the original values of
    the monkeypatched names.
    """
    cache = {'BytesType_deserialize': dse.cqltypes.BytesType.deserialize,
             'DateType_deserialize': dse.cqltypes.DateType.deserialize,
             'support_empty_values': dse.cqltypes.CassandraType.support_empty_values}

    dse.cqltypes.BytesType.deserialize = staticmethod(lambda byts, protocol_version: bytearray(byts))
    dse.cqltypes.DateType.deserialize = staticmethod(deserialize_date_fallback_int)
    dse.cqltypes.CassandraType.support_empty_values = True

    if hasattr(dse, 'deserializers'):
        cache['DesDateType'] = dse.deserializers.DesDateType  # pylint: disable=no-member
        del dse.deserializers.DesDateType  # pylint: disable=no-member

    return cache


def unmonkeypatch_driver(cache):
    """
    Given a dictionary that was used to cache parts of `dse` for
    monkeypatching, restore those values to the `dse` module.
    """
    dse.cqltypes.BytesType.deserialize = staticmethod(cache['BytesType_deserialize'])
    dse.cqltypes.DateType.deserialize = staticmethod(cache['DateType_deserialize'])
    dse.cqltypes.CassandraType.support_empty_values = cache['support_empty_values']

    if hasattr(dse, 'deserializers'):
        dse.deserializers.DesDateType = cache['DesDateType']  # pylint: disable=no-member
