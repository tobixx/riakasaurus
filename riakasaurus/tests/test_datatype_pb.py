#!/usr/bin/env python
"""
riakasaurus trial test file.
riakasaurus _must_ be on your PYTHONPATH

"""

import sys,os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),'../../'))
import json
import random
from twisted.trial import unittest
from twisted.python import log
from twisted.internet import defer, reactor

VERBOSE = False

from riakasaurus import riak
from riakasaurus import transport
from riakasaurus.datatypes import Map,Set,Counter

# uncomment to activate logging
import sys
log.startLogging(sys.stderr)

RIAK_CLIENT_ID = 'TEST'
BUCKET_PREFIX = 'riakasaurus.tests.'

JAVASCRIPT_SUM = """
function(v) {
  x = v.reduce(function(a,b){ return a + b }, 0);
  return [x];
}
"""


def sleep(secs):
    d = defer.Deferred()
    reactor.callLater(secs, d.callback, None)
    return d


def randint():
    """Generate nice random int for our test."""
    return random.randint(1, 999999)


class Tests(unittest.TestCase):
    """
    trial unit tests.
    """

    test_keys = ['foo', 'foo1', 'foo2', 'foo3', 'bar', 'baz', 'ba_foo1',
                 'blue_foo1']

    def setUp(self):
        self.client = riak.RiakClient(client_id=RIAK_CLIENT_ID,transport=transport.PBCTransport,port = 8087)
        self.counter_bucket = self.client.bucket('test_counter','counters')
        self.set_bucket = self.client.bucket('test_set','sets')
        self.map_bucket = self.client.bucket('test_map','maps')

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.counter_bucket.purge_keys()
        yield self.set_bucket.purge_keys()
        yield self.map_bucket.purge_keys()
        yield self.client.transport.quit()

    @defer.inlineCallbacks
    def test_counter(self):
        counter = yield self.counter_bucket.fetch_datatype('test_counter1')
        if counter.value == 0:
            yield c.increment()
            yield c.update()
