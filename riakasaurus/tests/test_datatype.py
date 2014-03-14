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
        self.client = riak.RiakClient(client_id=RIAK_CLIENT_ID)
        self.counter_bucket = self.client.bucket('test_counter','counters')
        self.set_bucket = self.client.bucket('test_set','sets')
        self.map_bucket = self.client.bucket('test_map','maps')

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.counter_bucket.purge_keys()
        yield self.set_bucket.purge_keys()
        yield self.map_bucket.purge_keys()

    @defer.inlineCallbacks
    def test_counter(self):
        counter = yield self.counter_bucket.fetch_datatype('test_counter1')
        print 'before counter %s' %counter
        if counter.value == 0:
            counter.increment(10)
            res = yield counter.update()
            print 'now counter %s' %counter
            counter.increment(10)
            res = yield counter.update()
            print 'now counter %s' %counter
            counter = yield self.counter_bucket.fetch_datatype('test_counter1')
            print counter.__class__

    @defer.inlineCallbacks
    def test_set(self):
        set = yield self.set_bucket.fetch_datatype('test_set1')
        print 'before set %s' %set
        if not set.value:
            set.add('Jason Terry')
            set.add('Dirk Nowizki')
            set.add('Jason Kidd')
            set.add('Tyson Chandler')
            set.add('J.J Bareau')
            res = yield set.update()
            print 'now set %s' %set
            set.discard('Tyson Chandler')
            set.discard('J.J Bareau')
            res = yield set.update()
            print 'now set %s' %set
            set.discard('Jason Terry')
            set.discard('Jason Kidd')
            res = yield set.update()
            print 'now set %s' %set
            set = yield self.set_bucket.fetch_datatype('test_set1')
            print set

    @defer.inlineCallbacks
    def test_map(self):
        map = yield self.map_bucket.fetch_datatype('test_map1')
        print 'before map %s' %map
        if not map.value:
            map.counters['test_counter'].increment(10)
            map.registers['test_register'].set('Mavericks')
            map.counters['test_counter'].decrement(5)
            map.flags['test_flag'].enable()
            map.sets['test_set'].add('Jason Terry')
            map.sets['test_set'].add('Dirk Nowizki')
            map.sets['test_set'].add('Jason Kidd')
            map.sets['test_set'].add('Tyson Chandler')
            map.sets['test_set'].add('J.J Bareau')
            map.maps['offseason_trade'].counters['year'].increment(2010)
            map.maps['offseason_trade'].sets['trade_out'].add('J.J Bareau')
            map.maps['offseason_trade'].sets['trade_out'].add('Jason Terry')
            map.maps['offseason_trade'].sets['trade_out'].add('Tyson Chandler')
            map.maps['offseason_trade'].maps['test_inner_map'].registers['inner_text'].set('test_text')
            yield map.update()
            print "now map %s" %map.value
            del map.maps['offseason_trade']
            yield map.update()
            print "now map %s" %map.value
            map = yield self.map_bucket.fetch_datatype('test_map1')
            print 'final value %s' %map.value
