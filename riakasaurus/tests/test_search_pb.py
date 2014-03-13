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
        s = yield self.client.list_search_indexes()
        try:
            schema = yield self.client.get_search_schema('test_schema')
        except:
            schema_content = '''<?xml version="1.0" encoding="UTF-8" ?>
<schema name="test_schema" version="1.5">
  <fields>
    <field name="count" type="int" indexed="true" />
    <field name="class" type="_yz_str" indexed="true" />
    <!-- Needed by Yokozuna -->
    <field name="_yz_id" type="_yz_str" indexed="true" stored="true" required="true" />
    <field name="_version_" type="long" indexed="true" stored="true"/>
    <field name="_yz_ed" type="_yz_str" indexed="true" stored="true"/>
    <field name="_yz_rt" type="_yz_str" indexed="true" stored="true"/>
    <field name="_yz_err" type="_yz_str" indexed="true"/>
    <field name="_yz_pn" type="_yz_str" indexed="true" stored="true"/>
    <field name="_yz_fpn" type="_yz_str" indexed="true" stored="true"/>
    <field name="_yz_vtag" type="_yz_str" indexed="true" stored="true"/>
    <field name="_yz_node" type="_yz_str" indexed="true" stored="true"/>
    <field name="_yz_rb" type="_yz_str" indexed="true" stored="true"/>
    <field name="_yz_rk" type="_yz_str" indexed="true" stored="true"/>
  </fields>

  <uniqueKey>_yz_id</uniqueKey>

  <types>
    <fieldType name="_yz_str" class="solr.StrField" sortMissingLast="true" />
    <fieldType name="int" class="solr.TrieIntField" precisionStep="0" positionIncrementGap="0"/>
    <fieldType name="long" class="solr.TrieLongField" precisionStep="0" positionIncrementGap="0"/>

    <!-- A text field that only splits on whitespace for exact matching of words -->
    <fieldType name="text_ws" class="solr.TextField" positionIncrementGap="100">
      <analyzer>
        <tokenizer class="solr.WhitespaceTokenizerFactory"/>
      </analyzer>
    </fieldType>
  </types>
</schema>'''
            yield self.client.create_search_schema("test_schema",schema_content)
        try:
            indexes = yield self.client.get_search_index('test_index')
        except:
            print 'Index test_index not existed'
            yield self.client.create_search_index('test_index',schema='test_schema')
        current_index = yield self.bucket.get_search_index()
        if current_index != 'test_index':
            yield self.bucket.set_search_index('test_index')
        self.map_bucket = self.client.bucket('test_map','maps')
        yield self.client.create_search_index('test_map_index')
        yield self.map_bucket.set_search_index('test_map_index')

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.map_bucket.purge_keys()
        yield self.bucket.purge_keys()
        yield self.client.transport.quit()
        #yield self.client.delete_search_index('test_index')
        #print 'Deleted index test_index' 

    @defer.inlineCallbacks
    def test_list_search_index(self):
        res = yield self.client.list_search_indexes()
        print res

#    @defer.inlineCallbacks
    #def test_delete_index(self):
        #current_index = yield self.bucket.get_search_index()
        #print 'Current index is %s' %current_index
        #yield self.bucket.set_search_index('_dont_index_')
        #current_index = yield self.bucket.get_search_index()
        #print 'Current index is %s' %current_index
        #properties = yield self.bucket.get_properties()
        #print 'properties is %s' %properties
        #yield self.client.delete_search_index('test_index')
        #print 'Deleted index test_index' 


    @defer.inlineCallbacks
    def test_riak_search(self):
        """Test searching buckets"""
        log.msg("*** riak_search")
        count = 20
        for i in xrange(count):
            obj1 = self.bucket.new("foo%d" %i, {"count": "%d" %(count-i)})
            obj1.add_meta_data('x-riak-meta-yz-tags','x-riak-meta-class')
            obj1.add_meta_data('x-riak-meta-class','test')
            yield obj1.store()
        yield sleep(1)
        keys = yield self.bucket.search('class:test',rows=1)
        self.assertEqual(keys['num_found'],count)
        keys_sorted = yield self.bucket.search('class:test AND count:[10 TO 15]',rows=3,sort='count desc') #seems like fl have bug with pbc now
        self.assertEqual(keys_sorted['num_found'],6)
        self.assertEqual(int(keys_sorted['docs'][0]['count']),15)

    @defer.inlineCallbacks
    def test_riak_map_search(self):
        """Test searching bucket maps"""
        log.msg("*** riak_search")
        count = 20
        for i in xrange(count):
            obj1 = yield self.map_bucket.fetch_datatype("foo%d" %i)
            obj1.counters["count"].increment(count-i)
            obj1.registers["class"].set('test')
            yield obj1.update()
        yield sleep(1)
        keys = yield self.map_bucket.search('class_register:test',rows=1)
        self.assertEqual(keys['num_found'],count)
        keys_sorted = yield self.map_bucket.search('class_register:test AND count_counter:[10 TO 15]',rows=3,sort='count_counter desc') #seems like fl have bug with pbc now
        self.assertEqual(keys_sorted['num_found'],6)
        self.assertEqual(int(keys_sorted['docs'][0]['count_counter']),15)


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

