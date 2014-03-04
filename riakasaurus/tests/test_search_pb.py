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

    @defer.inlineCallbacks
    def setUp(self):
        self.client = riak.RiakClient(client_id=RIAK_CLIENT_ID,transport=transport.PBCTransport,port = 8087)
        self.bucket_name = BUCKET_PREFIX + self.id().rsplit('.', 1)[-1]
        self.bucket = self.client.bucket(self.bucket_name)
        yield self.client.create_search_index('test_index')
        index = yield self.client.get_search_index('test_index')
        yield self.bucket.set_search_index('test_index')

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.bucket.purge_keys()
        yield self.client.transport.quit()

    @defer.inlineCallbacks
    def test_riak_search(self):
        """Test searching buckets"""
        log.msg("*** riak_search")

        obj1 = self.bucket.new("foo1", {"foo": "test1"})
        yield obj1.store()

        keys = yield self.bucket.search('foo:test1')
        print keys

#        self.assertTrue(keys.get_key() == u'foo1')

        #yield obj1.delete()

    #@defer.inlineCallbacks
    #def test_solr_search_from_bucket(self):
        #yield self.bucket.new("user", {"username": "roidrage"}).store()
        #results = yield self.bucket.search("username:roidrage")
        #self.assertEquals(1, len(results["docs"]))

    #@defer.inlineCallbacks
    #def test_solr_search_with_params_from_bucket(self):
        #yield self.bucket.new("user", {"username": "roidrage"}).store()
        #result = yield self.bucket.search("username:roidrage", wt="xml")

        #self.assertEquals(1, len(result["docs"]))

    #@defer.inlineCallbacks
    #def test_solr_search_with_params(self):
        #yield self.bucket.new("user", {"username": "roidrage"}).store()
        #result = yield self.client.solr().search(self.bucket_name,
                                                  #"username:roidrage",
                                                  #wt="xml")

        #self.assertEquals(1, len(result["docs"]))

    #@defer.inlineCallbacks
    #def test_solr_search(self):
        #yield self.bucket.new("user", {"username": "roidrage"}).store()
        #results = yield self.client.solr().search(self.bucket_name,
                                                  #"username:roidrage")
        #self.assertEquals(1, len(results["docs"]))

    #@defer.inlineCallbacks
    #def test_add_document_to_index(self):
        #yield self.client.solr().add(self.bucket_name,
                                     #{"id": "doc", "username": "tony"})
        #results = yield self.client.solr().search(self.bucket_name,
                                                  #"username:tony")

        #self.assertEquals("tony",
                          #results["docs"][0]["username"])

#    @defer.inlineCallbacks
    #def test_add_multiple_documents_to_index(self):
        #yield self.client.solr().add(self.bucket_name,
                                     #{"id": "dizzy", "username": "dizzy"},
                                     #{"id": "russell", "username": "russell"})
        #yield sleep(2)  # Eventual consistency is annoying
        #results = yield self.client.solr().search(self.bucket_name,
                                                  #"username:russell OR"
                                                  #" username:dizzy")
        #self.assertEquals(2, len(results["docs"]))

    #@defer.inlineCallbacks
    #def test_delete_documents_from_search_by_id(self):
        #yield self.client.solr().add(self.bucket_name,
                                     #{"id": "dizzy", "username": "dizzy"},
                                     #{"id": "russell", "username": "russell"})
        #yield self.client.solr().delete(self.bucket_name, docs=["dizzy"])
        #results = yield self.client.solr().search(self.bucket_name,
                                                  #"username:russell OR"
                                                  #" username:dizzy")
        ## This test fails at eventual consistency...
        ##self.assertEquals(1, len(results["docs"]))

    #@defer.inlineCallbacks
    #def test_delete_documents_from_search_by_query(self):
        #yield self.client.solr().add(self.bucket_name,
                                     #{"id": "dizzy", "username": "dizzy"},
                                     #{"id": "russell", "username": "russell"})
        #yield self.client.solr().delete(self.bucket_name,
                                        #queries=["username:dizzy",
                                                 #"username:russell"])
        #results = yield self.client.solr().search(self.bucket_name,
                                                  #"username:russell OR"
                                                  #" username:dizzy")
        ## This test fails at eventual consistency...
        ##self.assertEquals(0, len(results["docs"]))

