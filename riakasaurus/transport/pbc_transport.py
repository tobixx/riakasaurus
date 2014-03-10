from zope.interface import implements

from twisted.protocols.basic import LineReceiver
LineReceiver.MAX_LENGTH = 1024 * 1024 * 64

from twisted.internet import defer, reactor, protocol
from twisted.python import log
import logging
import traceback

from distutils.version import LooseVersion

import time
import json
# MD_ resources
from riakasaurus.metadata import *

from riakasaurus.riak_index_entry import RiakIndexEntry
from riakasaurus.mapreduce import RiakLink
from riakasaurus import exceptions

# protobuf
from riakasaurus.transport import transport, pbc

LOGLEVEL_DEBUG = 1
LOGLEVEL_TRANSPORT = 2
LOGLEVEL_TRANSPORT_VERBOSE = 4



class StatefulTransport(object):
    def __init__(self,factory):
        self.__transport = None
        self.__state = 'idle'
        self.__created = time.time()
        self.__used = time.time()
        self.__factory=factory

    def __repr__(self):
        return '<StatefulTransport idle=%.2fs state=\'%s\' transport=%s>' % (
            time.time() - self.__used, self.__state, self.__transport)

    def __enter__(self):
        return self.getTransport()

    def __exit__(self, exc_type, exc_value, traceback):
        self.setIdle()

    def isActive(self):
        return self.__state == 'active'

    def setActive(self):
        self.__state = 'active'
        self.__factory.active_count+=1
        self.__used = time.time()

    def isIdle(self):
        return self.__state == 'idle'

    def setIdle(self):
        self.__state = 'idle'
        self.__factory.active_count-=1
        self.__used = time.time()

    def setTransport(self, transport):
        self.__transport = transport

    def getTransport(self):
        return self.__transport

    def age(self):
        return time.time() - self.__used

    def isDisconnected(self):
        transport = self.getTransport()
        return transport and transport.isDisconnected()


class PBCTransport(transport.FeatureDetection):
    """ Protocoll buffer transport for Riak """

    implements(transport.ITransport)

    debug = 0
    logToLevel = logging.INFO
    MAX_TRANSPORTS = 100
    MAX_IDLETIME = 5 * 60     # in seconds
    # how often (in seconds) the garbage collection should run
    # XXX Why the hell do we even have to override GC?
    GC_TIME = 120
    POOL_RETRY = 30

    def __init__(self, client):
        self._prefix = client._prefix
        self.host = client._host
        self.port = client._port
        self.client = client
        self._client_id = None
        self._transports = []    # list of transports, empty on start
        self.active_count = 0
        self._gc = reactor.callLater(self.GC_TIME, self._garbageCollect)
        self.timeout = client.request_timeout

    def setTimeout(self, t):
        self.timeout = t

    @defer.inlineCallbacks
    def _getFreeTransport(self,retry = None):
        foundOne = False
        retry = self.POOL_RETRY if retry == None else retry
        # Discard disconnected transports.
        self._transports = [x for x in self._transports if not x.isDisconnected()]

        for stp in self._transports:
            if stp.isIdle():
                stp.setActive()
                stp.getTransport().setTimeout(self.timeout)
                foundOne = True
                if self.debug & LOGLEVEL_TRANSPORT_VERBOSE:
                    log.msg("[%s] aquired idle transport[%d]: %s" % (
                            self.__class__.__name__,
                            len(self._transports), stp
                        ), logLevel=self.logToLevel)
                defer.returnValue(stp)
        if not foundOne:
            if len(self._transports) >= self.MAX_TRANSPORTS:
                if retry > 0:
                    d = defer.Deferred()
                    d.addCallback(self._getFreeTransport)
                    reactor.callLater(0.1, d.callback,retry -1 )
                    conn = yield d
                    defer.returnValue(conn)
                else:
                    raise Exception("too many transports, aborting")
            # nothin free, create a new protocol instance, append
            # it to self._transports and return it

            # insert a placeholder into self._transports to avoid race
            # conditions with the MAX_TRANSPORTS check above.
            stp = StatefulTransport(self)
            stp.setActive()
            idx = len(self._transports)
            self._transports.append(stp)

            # create the transport and use it to configure the placeholder.
            try:
                transport = yield pbc.RiakPBCClient().connect(self.host, self.port)
                stp.setTransport(transport)
                if self.timeout:
                    transport.setTimeout(self.timeout)
                if self.debug & LOGLEVEL_TRANSPORT:
                    log.msg("[%s] allocate new transport[%d]: %s" % (
                            self.__class__.__name__, idx, stp
                        ), logLevel=self.logToLevel)
                defer.returnValue(stp)
            except Exception:
                self._transports.remove(stp)
                raise


    @defer.inlineCallbacks
    def _garbageCollect(self):
        self._gc = reactor.callLater(self.GC_TIME, self._garbageCollect)
        for idx, stp in enumerate(self._transports):
            if (stp.isIdle() and stp.age() > self.MAX_IDLETIME):
                yield stp.getTransport().quit()
                if self.debug & LOGLEVEL_TRANSPORT:
                    log.msg("[%s] expire idle transport[%d] %s" % (
                            self.__class__.__name__,
                            idx,
                            stp
                        ), logLevel=self.logToLevel)

                    log.msg("[%s] %s" % (
                            self.__class__.__name__,
                            self._transports
                        ), logLevel=self.logToLevel)

                self._transports.remove(stp)
            elif self.timeout and stp.isActive() and stp.age() > self.timeout:
                yield stp.getTransport().quit()
                if self.debug & LOGLEVEL_TRANSPORT:
                    log.msg("[%s] expire timeouted transport[%d] %s" % (
                            self.__class__.__name__,
                            idx,
                            stp
                        ), logLevel=self.logToLevel)

                    log.msg("[%s] %s" % (
                            self.__class__.__name__,
                            self._transports
                        ), logLevel=self.logToLevel)

                self._transports.remove(stp)

    @defer.inlineCallbacks
    def quit(self):
        self._gc.cancel()      # cancel the garbage collector

        for stp in self._transports:
            if self.debug & LOGLEVEL_DEBUG:
                log.msg("[%s] transport[%d].quit() %s" % (
                        self.__class__.__name__,
                        len(self._transports),
                        stp
                    ), logLevel=self.logToLevel)

            yield (stp.getTransport() and stp.getTransport().quit())

    def __del__(self):
        """on shutdown, close all transports"""
        self.quit()

    def put(self, robj, w=None, dw=None, pw=None, return_body=True,
            if_none_match=False):

        ret = self.__put(robj, w, dw, pw,
                         return_body=return_body, if_none_match=if_none_match)

        #even if we don't want body, we still need to get the defer to wait the process is being finished here
#        if return_body:
            #return ret
        #else:
            #return None
        return ret

    def put_new(self, robj, w=None, dw=None, pw=None, return_body=True,
                if_none_match=False):
        ret = self.__put(robj, w, dw, pw,
                         return_body=return_body, if_none_match=if_none_match)
#        if return_body:
            #return ret
        #else:
            #return (ret[0], None, None)
        return ret

    @defer.inlineCallbacks
    def __put(self, robj, w=None, dw=None, pw=None, return_body=True,
              if_none_match=False):
        # std kwargs
        kwargs = {
                'w': w,
                'dw': dw,
                'pw': pw,
                'return_body': return_body,
                'if_none_match': if_none_match
            }
        # vclock
        vclock = robj.vclock() or None

        payload = {
                'value': robj.get_encoded_data(),
                'content_type': robj.get_content_type(),
            }

        # links
        links = robj.get_links()
        if links:
            payload['links'] = []
            for l in links:
                payload['links'].append(
                    (l.get_bucket(), l.get_key(), l.get_tag())
                )

        # usermeta
        if robj.get_usermeta():
            payload['usermeta'] = []
            for key, value in robj.get_usermeta().iteritems():
                payload['usermeta'].append((key, value))

        # indexes
        if robj.get_indexes():
            payload['indexes'] = []
            for index in robj.get_indexes():
                payload['indexes'].append(
                    (index.get_field(), index.get_value())
                )

        # aquire transport, fire, release
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.put(robj.get_bucket().get_name(),
                                      robj.get_key(),
                                      payload,
                                      vclock,
                                      **kwargs
                                      )
        defer.returnValue(self.parseRpbGetResp(ret))

    @defer.inlineCallbacks
    def get(self, robj, r=None, pr=None, vtag=None):

        # ***FIXME*** whats vtag for? ignored for now

        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.get(robj.get_bucket().get_name(),
                                      robj.get_key(),
                                      r=r,
                                      pr=pr)

        defer.returnValue(self.parseRpbGetResp(ret))

    @defer.inlineCallbacks
    def head(self, robj, r=None, pr=None, vtag=None):
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.get(robj.get_bucket().get_name(),
                                      robj.get_key(),
                                      r=r,
                                      pr=pr,
                                      head=True)

        defer.returnValue(self.parseRpbGetResp(ret))

    @defer.inlineCallbacks
    def delete(self, robj, rw=None, r=None, w=None, dw=None, pr=None, pw=None):
        """
        Delete an object.
        """
        # We could detect quorum_controls here but HTTP ignores
        # unknown flags/params.
        kwargs = {'rw': rw, 'r': r, 'w': w, 'dw': dw, 'pr': pr, 'pw': pw}
        headers = {}

        ts = yield self.tombstone_vclocks()
        if ts and robj.vclock() is not None:
            kwargs['vclock'] = robj.vclock()

        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.delete(robj.get_bucket().get_name(),
                                         robj.get_key(),
                                         **kwargs
                                         )

        defer.returnValue(ret)

    @defer.inlineCallbacks
    def search(self, index, query, **params):
        """
        Performs a search query.
        """
        if index is None:
            index = 'search'

        options = {'wt': 'json'}
        if 'op' in params:
            op = params.pop('op')
            options['q.op'] = op

        if 'start' in params:
            start = int(params.pop('start'))
            options['start']=start

        if 'rows' in params:
            rows = int(params.pop('rows'))
            options['rows']=rows

        if 'sort' in params:
            sort = params.pop('sort')
            options['sort']=sort

        if 'filter' in params:
            fil = params.pop('filter')
            options['filter']=fil
        if 'presort' in params:
            presort = params.pop('presort')
            options['presort']=presort
        if 'df' in params:
            df = params.pop('df')
            options['df']=df
        if 'fl' in params:
            fl = params.pop('fl')
            options['fl']=fl

        options.update(params)
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.search(index,query,**options)

        ret = self.parseRpbSearchResp(ret) #didn't really parse it, just annotates resp structure
        defer.returnValue(ret)


    @defer.inlineCallbacks
    def mapred(self, inputs, query, timeout=None):
        """
        Run a MapReduce query.
        """
        plm = yield self.phaseless_mapred()
        if not plm and (query is None or len(query) is 0):
            raise Exception('Phase-less MapReduce is not supported'
                            'by this Riak node')
        # Construct the job, optionally set the timeout...
        job = {'inputs': inputs, 'query': query}
        if timeout is not None:
            job['timeout'] = timeout
        content = self.encodeJson(job)
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.mapred(content)
        ret = self.parseRpbMapReduceResp(ret)
        defer.returnValue(ret)

    @defer.inlineCallbacks
    def get_buckets(self):
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.getBuckets()
        defer.returnValue([x for x in ret.buckets])

    @defer.inlineCallbacks
    def server_version(self):
        if not self._s_version:
            self._s_version = yield self._server_version()
        defer.returnValue(LooseVersion(self._s_version))




    @defer.inlineCallbacks
    def _server_version(self):
        with (yield self._getFreeTransport()) as transport:
            stats = yield transport.getServerInfo()

        if stats is not None:
            if self.debug % LOGLEVEL_DEBUG:
                log.msg("[%s] fetched server version: %s" % (
                    self.__class__.__name__,
                    stats.server_version
                ), logLevel=self.logToLevel)
            defer.returnValue(stats.server_version)
        else:
            defer.returnValue("0.14.0")

    @defer.inlineCallbacks
    def ping(self):
        """
        Check server is alive
        """
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.ping()
        defer.returnValue(ret == True)

    @defer.inlineCallbacks
    def get_counter(self, bucket, key, **params):
        counters = yield self.counters()
        if not counters:
            raise NotImplementedError("Counters are not supported")

        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.get_counter(bucket,key,**params)
            defer.returnValue(ret)

    @defer.inlineCallbacks
    def update_counter(self, bucket, key, value, **params):
        counters = yield self.counters()
        if not counters:
            raise NotImplementedError("Counters are not supported")
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.update_counter(bucket,key,value,**params)
            defer.returnValue(ret)


    @defer.inlineCallbacks
    def fetch_datatype(self, bucket, key, r=None, pr=None,
                       basic_quorum=None, notfound_ok=None, timeout=None,
                       include_context=None):
        """
        fetch_datatype(bucket, key, r=None, pr=None, basic_quorum=None,
                       notfound_ok=None, timeout=None, include_context=None)

        Fetches the value of a Riak Datatype.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

        :param bucket: the bucket of the datatype, which must belong to a
          :class:`~riak.BucketType`
        :type bucket: RiakBucket
        :param key: the key of the datatype
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
        :param timeout: a timeout value in milliseconds
        :type timeout: int
        :param include_context: whether to return the opaque context
          as well as the value, which is useful for removal operations
          on sets and maps
        :type include_context: bool
        :rtype: a subclass of :class:`~riak.datatypes.Datatype`
        """

        with (yield self._getFreeTransport()) as transport:
            result = yield transport.fetch_datatype(bucket, key, r=r, pr=pr,
                                              basic_quorum=basic_quorum,
                                              notfound_ok=notfound_ok,
                                              timeout=timeout,
                                              include_context=include_context)
            defer.returnValue(result)

    @defer.inlineCallbacks
    def update_datatype(self, datatype, bucket, key=None, w=None, dw=None,
                        pw=None, return_body=None, timeout=None,
                        include_context=None):
        """
        Updates a Riak Datatype. This operation is not idempotent and
        so will not be retried automatically.

        :param datatype: the datatype to update
        :type datatype: a subclass of :class:`~riak.datatypes.Datatype`
        :param bucket: the bucket of the datatype, which must belong to a
          :class:`~riak.BucketType`
        :type bucket: RiakBucket
        :param key: the key of the datatype
        :type key: string, None
        :param w: the write quorum
        :type w: integer, string, None
        :param dw: the durable write quorum
        :type dw: integer, string, None
        :param pw: the primary write quorum
        :type pw: integer, string, None
        :param timeout: a timeout value in milliseconds
        :type timeout: int
        :param include_context: whether to return the opaque context
          as well as the value, which is useful for removal operations
          on sets and maps
        :type include_context: bool
        :rtype: a subclass of :class:`~riak.datatypes.Datatype`, bool
        """
        with (yield self._getFreeTransport()) as transport:
            result = yield transport.update_type(datatype, bucket, key=key, w=w,
                                           dw=dw, pw=pw,
                                           return_body=return_body,
                                           timeout=timeout,
                                           include_context=include_context)
            defer.returnValue(result)
            #if return_body and result:
                #defer.returnValue( TYPES[result[0]](result[1], result[2]) )
            #else:
                #defer.returnValue( result )

    @defer.inlineCallbacks
    def set_bucket_props(self, bucket, props):
        """
        Set bucket properties
        """
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.setBucketProperties(bucket.get_name(), **props)
        defer.returnValue(ret == True)

    @defer.inlineCallbacks
    def get_bucket_props(self, bucket):
        """
        get bucket properties
        """
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.getBucketProperties(bucket.get_name())
        res = {}
        attrs = ['n_val', 'allow_mult','last_write_wins','precommit','has_precommit','postcommit','has_postcommit','chash_keyfun','linkfun','old_vclock','young_vclock','big_vclock','small_vclock','pr','r','w','pw','dw','rw','basic_quorum','notfound_ok','backend','search','repl','search_index','datatype']
        for a in attrs:
            if hasattr(ret.props,a):
                res[a]=getattr(ret.props,a)
        defer.returnValue(res)

    @defer.inlineCallbacks
    def get_keys(self, bucket):
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.getKeys(bucket.get_name())
        defer.returnValue(ret)

    @defer.inlineCallbacks
    def get_index(self, bucket, index, startkey, endkey=None,return_terms=False, max_results=None, continuation=None):
        '''
        message RpbIndexResp {
            repeated bytes keys = 1;
            repeated RpbPair results = 2;
            optional bytes continuation = 3;
            optional bool done = 4;
        }
        return (results,continuation)
        '''
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.get_index(bucket, index, startkey, endkey=endkey,return_terms=return_terms, max_results=max_results, continuation=continuation)
            results = []
            if not return_terms or not endkey:
                for resp in ret:
                    if resp.keys:
                        results.extend(resp.keys)
            else:
                for resp in ret:
                    if resp.results:
                        for pair in resp.results:
                            results.append((pair.key,pair.value))
            if max_results:
                defer.returnValue((results,resp.continuation))
            else:
                defer.returnValue((results,None))


    @defer.inlineCallbacks
    def create_search_index(self, index, schema=None, n_val=None):
        if not (yield self.pb_search_admin()):
            raise NotImplementedError("Yokozuna administration is not "
                                      "supported for this version")
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.create_search_index(index, schema=schema, n_val=n_val)
            defer.returnValue(ret)

    @defer.inlineCallbacks
    def get_search_index(self, index):
        if not (yield self.pb_search_admin()):
            raise NotImplementedError("Yokozuna administration is not "
                                      "supported for this version")
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.get_search_index(index)
            defer.returnValue(ret)


    @defer.inlineCallbacks
    def delete_search_index(self, index):
        if not (yield self.pb_search_admin()):
            raise NotImplementedError("Yokozuna administration is not "
                                      "supported for this version")
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.delete_search_index(index)
            defer.returnValue(True)

    @defer.inlineCallbacks
    def list_search_indexes(self):
        if not (yield self.pb_search_admin()):
            raise NotImplementedError("Yokozuna administration is not "
                                      "supported for this version")
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.list_search_indexes()
            ret = [index for index in ret.index]
            defer.returnValue(ret)

    @defer.inlineCallbacks
    def create_search_schema(self, schema, content):
        if not (yield self.pb_search_admin()):
            raise NotImplementedError("Yokozuna administration is not "
                                      "supported for this version")
        with (yield self._getFreeTransport()) as transport:
            ret = yield transport.create_search_schema(schema,content)
            defer.returnValue(True)

    @defer.inlineCallbacks
    def get_search_schema(self, schema):
        if not (yield self.pb_search_admin()):
            raise NotImplementedError("Yokozuna administration is not "
                                      "supported for this version")
        with (yield self._getFreeTransport()) as transport:
            resp = yield transport.get_search_schema(schema)
            result = {}
            result['name'] = resp.schema.name
            result['content'] = resp.schema.content
            defer.returnValue(result)



    def parseRpbSearchResp(self,res):
        '''
        adatpor for a RpbSearchResp message
        message RpbSearchResp  
        // RbpPair is a generic key/value pair datatype used for other message types
        message RpbPair {
          required bytes key = 1;
          optional bytes value = 2;
        }
        message RpbSearchDoc {
          repeated RpbPair fields = 1;
        }
        message RpbSearchQueryResp {
          repeated RpbSearchDoc docs      = 1;
          optional float        max_score = 2;
          optional uint32       num_found = 3;
        }
        '''
        result = {}
        attributes = ['max_score','num_found']
        for prop in attributes:
            result[prop]=getattr(res,prop,0)
        result['docs']=[]
        docs=getattr(res,'docs',[])
        for doc in docs:
            tc={}
            for field in getattr(doc,'fields',[]):
                tc[getattr(field,'key')]=getattr(field,'value')
            result['docs'].append(tc)
        return result

    def parseRpbMapReduceResp(self,res):
        '''
        adatpor for a RpbMapRedResp message
        res is a list of RpbMapRedResp,each result referes to a phase of 
        message RpbMapRedResp {
            optional uint32 phase = 1;
            optional bytes response = 2;
            optional bool done = 3;
        }
        '''
        ret = []
        for result in res:
            if result.response:
                try:
                    res = self.decodeJson(result.response)
                    for o in res:
                        ret.append(o)
#                    if len(res)>1:
                        #print res
                except Exception,e:
                    log.err( e )
                    log.err('Error parse mapred ressult')
                    log.err( traceback.format_exc())
                    ret.append(result.response)
        return ret

    def parseRpbGetResp(self, res):
        """
        adaptor for a RpbGetResp message
        message RpbGetResp {
           repeated RpbContent content = 1;
           optional bytes vclock = 2;
           optional bool unchanged = 3;
        }
        """
        if res == True:         # empty response
            return None
        vclock = res.vclock
        resList = []
        for content in res.content:
            # iterate over RpbContent field
            metadata = {MD_USERMETA: {}, MD_INDEX: []}
            data = content.value
            if content.HasField('content_type'):
                metadata[MD_CTYPE] = content.content_type

            if content.HasField('charset'):
                metadata[MD_CHARSET] = content.charset

            if content.HasField('content_encoding'):
                metadata[MD_ENCODING] = content.content_encoding

            if content.HasField('vtag'):
                metadata[MD_VTAG] = content.vtag

            if content.HasField('last_mod'):
                metadata[MD_LASTMOD] = content.last_mod

            if content.HasField('deleted'):
                metadata[MD_DELETED] = content.deleted

            if len(content.links):
                metadata[MD_LINKS] = []
                for l in content.links:
                    metadata[MD_LINKS].append(
                        RiakLink(l.bucket, l.key, l.tag)
                    )

            if len(content.usermeta):
                metadata[MD_USERMETA] = {}
                for md in content.usermeta:
                    metadata[MD_USERMETA][md.key] = md.value

            if len(content.indexes):
                metadata[MD_INDEX] = []
                for ie in content.indexes:
                    metadata[MD_INDEX].append(RiakIndexEntry(ie.key, ie.value))
            resList.append((metadata, data))
        return vclock, resList

    def decodeJson(self, s):
        return self.client.get_decoder('application/json')(s)

    def encodeJson(self, s):
        return self.client.get_encoder('application/json')(s)

    # def deferred_sleep(self,secs):
    #     """
    #     fake deferred sleep

    #     @param secs: time to sleep
    #     @type secs: float
    #     """
    #     d = defer.Deferred()
    #     reactor.callLater(secs, d.callback, None)
    #     return d
