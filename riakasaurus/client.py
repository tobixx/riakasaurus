"""
.. module:: client.py

RiakClient class

"""

import random
import base64
import urllib
import json
from twisted.internet import defer

from riakasaurus import mapreduce, bucket
from riakasaurus.search import RiakSearch

from riakasaurus import transport
from twisted.python import log


class RiakClient(object):
    """
    The RiakClient object holds information necessary to connect to
    Riak.
    """
    def __init__(self, host='127.0.0.1', port=8098,
                mapred_prefix='mapred',
                client_id=None, r_value="default", w_value="default",
                dw_value="default", transport=transport.HTTPTransport,
                request_timeout=None):
        """
        Construct a new RiakClient object.

        If a client_id is not provided, generate a random one.
        """
        self._host = host
        self._port = port
        self._mapred_prefix = mapred_prefix
        if client_id:
            self._client_id = client_id
        else:
            self._client_id = 'py_' + base64.b64encode(
                                      str(random.randint(1, 1073741824)))
        self._r = r_value
        self._w = w_value
        self._dw = dw_value

        self._rw = "default"
        self._pr = "default"
        self._pw = "default"

        self._encoders = {'application/json': json.dumps,
                          'text/json': json.dumps}
        self._decoders = {'application/json': json.loads,
                          'text/json': json.loads}
        self._solr = None

        self.request_timeout = request_timeout

        self.transport = transport(self)

    def setRequestTimeout(self, timeout):
        self.request_timeout = timeout

    def get_transport(self):
        return self.transport

    def create_search_index(self, index, schema=None, n_val=None):
        """
        create_search_index(index, schema, n_val)

        Create a search index of the given name, and optionally set
        a schema. If no schema is set, the default will be used.

        :param index: the name of the index to create
        :type index: string
        :param schema: the schema that this index will follow
        :type schema: string, None
        :param n_val: this indexes N value
        :type n_val: integer, None
        """
        return self.transport.create_search_index(index, schema, n_val)

    @defer.inlineCallbacks
    def get_search_index(self, index):
        """
        Returns a yokozuna search index or None.
        """
        try:
            res = yield self.transport.get_search_index(index)
        except:
            defer.returnValue( None )

    def list_search_indexes(self):
        """
        Lists all yokozuna search indexes.
        """
        return self.transport.list_search_indexes()

    def delete_search_index(self, index):
        """
        Deletes a yokozuna search index.
        """
        return self.transport.delete_search_index(index)

    def create_search_schema(self, schema, content):
        """
        Creates a yokozuna search schema.
        """
        return self.transport.create_search_schema(schema,content)

    @defer.inlineCallbacks
    def get_search_schema(self, schema):
        """
        Returns a yokozuna search schema.
        """
        try:
            res = yield self.transport.get_search_schema(schema)
            defer.returnValue(res)
        except:
            defer.returnValue( None )

    def get_r(self):
        """
        Get the R-value setting for this RiakClient. (default 2)
        :returns: integer representing current r-value
        .. todo:: remove accessor
        """
        return self._r

    def get_rw(self):
        """
        Get the RW-value for this ``RiakClient`` instance. (default "quorum")

        :rtype: integer
        """
        return self._rw

    def set_rw(self, rw):
        """
        Set the RW-value for this ``RiakClient`` instance. See :func:`set_r`
        for a description of how these values are used.

        :param rw: The RW value.
        :type rw: integer
        :rtype: self
        """
        self._rw = rw
        return self

    def get_pw(self):
        """
        Get the PW-value setting for this ``RiakClient``. (default 0)

        :rtype: integer
        """
        return self._pw

    def set_pw(self, pw):
        """
        Set the PW-value for this ``RiakClient`` instance. See :func:`set_r`
        for a description of how these values are used.

        :param pw: The W value.
        :type pw: integer
        :rtype: self
        """
        self._pw = pw
        return self

    def get_pr(self):
        """
        Get the PR-value setting for this ``RiakClient``. (default 0)

        :rtype: integer
        """
        return self._pr

    def set_pr(self, pr):
        """
        Set the PR-value for this ``RiakClient`` instance. See :func:`set_r`
        for a description of how these values are used.

        :param pr: The PR value.
        :type pr: integer
        :rtype: self
        """
        self._pr = pr
        return self

    def set_r(self, r):
        """
        Set the R-value for this RiakClient. This value will be used
        for any calls to get(...) or get_binary(...) where where 1) no
        R-value is specified in the method call and 2) no R-value has
        been set in the RiakBucket.
        @param integer r - The R value.
        @return self
        .. todo:: remove accessor
        """
        self._r = r
        return self

    def get_w(self):
        """
        Get the W-value setting for this RiakClient. (default 2)
        @return integer
        .. todo:: remove accessor
        """
        return self._w

    def set_w(self, w):
        """
        Set the W-value for this RiakClient. See set_r(...) for a
        description of how these values are used.
        @param integer w - The W value.
        @return self
        .. todo:: remove accessor
        """
        self._w = w
        return self

    def get_dw(self):
        """
        Get the DW-value for this ClientOBject. (default 2)
        @return integer
        .. todo:: remove accessor
        """
        return self._dw

    def set_dw(self, dw):
        """
        Set the DW-value for this RiakClient. See set_r(...) for a
        description of how these values are used.
        @param integer dw - The DW value.
        @return self
        .. todo:: remove accessor
        """
        self._dw = dw
        return self

    def get_client_id(self):
        """
        Get the client_id for this RiakClient.
        @return string
        .. todo:: remove accessor
        """
        return self._client_id

    def set_client_id(self, client_id):
        """
        Set the client_id for this RiakClient. Should not be called
        unless you know what you are doing.
        @param string client_id - The new client_id.
        @return self
        .. todo:: remove accessor
        """
        self._client_id = client_id
        return self

    def get_encoder(self, content_type):
        """
        Get the encoding function for the provided content type.
        """
        if content_type in self._encoders:
            return self._encoders[content_type]

    def set_encoder(self, content_type, encoder):
        """
        Set the encoding function for the provided content type.

        :param encoder:
        :type encoder: function
        """
        self._encoders[content_type] = encoder
        return self

    def get_decoder(self, content_type):
        """
        Get the decoding function for the provided content type.
        """
        if content_type in self._decoders:
            return self._decoders[content_type]

    def set_decoder(self, content_type, decoder):
        """
        Set the decoding function for the provided content type.

        :param decoder:
        :type decoder: function
        """
        self._decoders[content_type] = decoder
        return self

    def bucket(self, name,bucket_type = 'default'):
        """
        Get the bucket by the specified name. Since buckets always exist,
        this will always return a RiakBucket.
        :returns: RiakBucket instance.
        """
        return bucket.RiakBucket(self, name,bucket_type)

    def is_alive(self):
        """
        Check if the Riak server for this RiakClient is alive.
        :returns: True if alive -- via deferred.
        """

        return self.transport.ping()

    def add(self, *args):
        """
        Start assembling a Map/Reduce operation.
        see RiakMapReduce.add()
        :returns: RiakMapReduce
        """
        mr = mapreduce.RiakMapReduce(self)
        return mr.add(*args)

    def search(self, *args,**kwargs):
        """
        Start assembling a Map/Reduce operation for Riak Search
        see RiakMapReduce.search()
        """
        mr = mapreduce.RiakMapReduce(self)
        return mr.search(*args,**kwargs)

    def link(self, args):
        """
        Start assembling a Map/Reduce operation.
        see RiakMapReduce.link()
        :returns: RiakMapReduce
        """
        mr = mapreduce.RiakMapReduce(self)
        return mr.link(*args)

    def list_buckets(self,bucket_type = 'default'):
        """
        Retrieve a list of all buckets.

        :returns: list -- via deferred
        """

        return self.transport.get_buckets(bucket_type)

    def index(self, *args):
        """
        Start assembling a Map/Reduce operation based on secondary
        index query results.

        :rtype: :class:`RiakMapReduce`
        """
        mr = mapreduce.RiakMapReduce(self)
        return mr.index(*args)

    def map(self, *args):
        """
        Start assembling a Map/Reduce operation.
        see RiakMapReduce.map()
        :returns: RiakMapReduce
        """
        mr = mapreduce.RiakMapReduce(self)
        return mr.map(*args)

    def reduce(self, *args):
        """
        Start assembling a Map/Reduce operation.
        see RiakMapReduce.reduce()
        :returns: RiakMapReduce
        """
        mr = mapreduce.RiakMapReduce(self)
        return mr.reduce(*args)

    def solr(self):
        if self._solr is None:
            self._solr = RiakSearch(self)
        return self._solr

    def fulltext_search(self,index,query,**kwargs):
        return self.transport.search(index,query,**kwargs)

    def get_counter(self, bucket, key, r=None, pr=None,
                    basic_quorum=None, notfound_ok=None):
        """
        get_counter(bucket, key, r=None, pr=None, basic_quorum=None,\
                    notfound_ok=None)

        Gets the value of a counter.

        .. note:: This request is automatically retried :attr:`RETRY_COUNT`
           times if it fails due to network error.

        :param bucket: the bucket of the counter
        :type bucket: RiakBucket
        :param key: the key of the counter
        :type key: string
        :param r: the read quorum
        :type r: integer, string, None
        :param pr: the primary read quorum
        :type pr: integer, string, None
        :param basic_quorum: whether to use the "basic quorum" policy
           for not-founds
        :type basic_quorum: bool
        :param notfound_ok: whether to treat not-found responses as successful
        :type notfound_ok: bool
        :rtype: integer
        """
        return self.transport.get_counter(bucket, key, r=r, pr=pr)

    def update_counter(self, bucket, key, value, w=None, dw=None, pw=None,
                       returnvalue=False):
        """
        update_counter(bucket, key, value, w=None, dw=None, pw=None,\
                       returnvalue=False)

        Updates a counter by the given value. This operation is not
        idempotent and so should not be retried automatically.

        :param bucket: the bucket of the counter
        :type bucket: RiakBucket
        :param key: the key of the counter
        :type key: string
        :param value: the amount to increment or decrement
        :type value: integer
        :param w: the write quorum
        :type w: integer, string, None
        :param dw: the durable write quorum
        :type dw: integer, string, None
        :param pw: the primary write quorum
        :type pw: integer, string, None
        :param returnvalue: whether to return the updated value of the counter
        :type returnvalue: bool
        """
        if type(value) not in (int, long):
            raise TypeError("Counter update amount must be an integer")
        if value == 0:
            raise ValueError("Cannot increment counter by 0")
        return self.transport.update_counter(bucket, key, value,
                                            w=w, dw=dw, pw=pw,
                                            returnvalue=returnvalue)




if __name__ == "__main__":
    pass
