from zope.interface import implements

from twisted.protocols.basic import LineReceiver
LineReceiver.MAX_LENGTH = 1024 * 1024 * 64

from twisted.internet import defer, reactor, protocol, error
from twisted.web.http_headers import Headers
from twisted.web import client
client._HTTP11ClientFactory.noisy = False
from twisted.web.client import Agent
from twisted.web.iweb import IBodyProducer
from twisted.python import log

# MD_ resources
from riakasaurus.metadata import *

from riakasaurus.riak_index_entry import RiakIndexEntry
from riakasaurus.mapreduce import RiakLink
from riakasaurus.transport import transport
from riakasaurus import exceptions

from distutils.version import LooseVersion
from cStringIO import StringIO
from xml.etree import ElementTree

import urllib
import re
import csv
import time

from riakasaurus.datatypes import TYPES
from riakasaurus.datatypes import Set,Map,Counter #top class crdt
from riakasaurus.datatypes import Flag,Register #secondary class crdt
from riakasaurus.datatypes import Datatype
from riakasaurus.datatypes import new
import traceback

MAX_LINK_HEADER_SIZE = 8192 - 8


class BodyReceiver(protocol.Protocol):
    """ Simple buffering consumer for body objects """
    def __init__(self, finished):
        self.finished = finished
        self.buffer = StringIO()

    def dataReceived(self, buffer):
        self.buffer.write(buffer)

    def connectionLost(self, reason):
        self.buffer.seek(0)
        self.finished.callback(self.buffer)


class StringProducer(object):
    """
    Body producer for t.w.c.Agent
    """
    implements(IBodyProducer)

    def __init__(self, body):
        self.body = body
        self.length = len(body)

    def startProducing(self, consumer):
        consumer.write(self.body)
        return defer.succeed(None)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass


class XMLSearchResult(object):
    # Match tags that are document fields
    fieldtags = ['str', 'int', 'date']

    def __init__(self):
        # Results
        self.num_found = 0
        self.max_score = 0.0
        self.docs = []

        # Parser state
        self.currdoc = None
        self.currfield = None
        self.currvalue = None

    def start(self, tag, attrib):
        if tag == 'result':
            self.num_found = int(attrib['numFound'])
            self.max_score = float(attrib['maxScore'])
        elif tag == 'doc':
            self.currdoc = {}
        elif tag in self.fieldtags and self.currdoc is not None:
            self.currfield = attrib['name']

    def end(self, tag):
        if tag == 'doc' and self.currdoc is not None:
            self.docs.append(self.currdoc)
            self.currdoc = None
        elif tag in self.fieldtags and self.currdoc is not None:
            if tag == 'int':
                self.currvalue = int(self.currvalue)
            self.currdoc[self.currfield] = self.currvalue
            self.currfield = None
            self.currvalue = None

    def data(self, data):
        if self.currfield:
            # riak_solr_output adds NL + 6 spaces
            data = data.rstrip()
            if self.currvalue:
                self.currvalue += data
            else:
                self.currvalue = data

    def close(self):
        return {
            'num_found': self.num_found,
            'max_score': self.max_score,
            'docs': self.docs
        }


class HTTPTransport(transport.FeatureDetection):

    implements(transport.ITransport)

    """ HTTP Transport for Riak """
    def __init__(self, client, prefix=None):
        self.host = client._host
        self.port = client._port
        self.client = client
        self._client_id = None

    def http_response(self, response):
        def haveBody(body):
            headers = {"http_code": response.code}
            for key, val in response.headers.getAllRawHeaders():
                headers[key.lower()] = val[0]

            return headers, body.read()

        if response.length:
            d = defer.Deferred()
            response.deliverBody(BodyReceiver(d))
            return d.addCallback(haveBody)
        else:
            return haveBody(StringIO(""))

    def abort_request(self, agent):
        """Called to abort request on timeout"""
        if not agent.called:
            t = self.client.request_timeout
            agent.cancel()

    def http_request(self, method, path, headers={}, body=None):
        url = "http://%s:%s%s" % (self.host, self.port, path)

        h = {}
        for k, v in headers.items():
            if not isinstance(v, list):
                h[k.lower()] = [v]
            else:
                h[k.lower()] = v

        # content-type must always be set
        if not 'content-type' in h.keys():
            h['content-type'] = ['application/json']

        if body:
            bodyProducer = StringProducer(body)
        else:
            bodyProducer = None

        requestAgent = Agent(reactor).request(
                method, str(url), Headers(h), bodyProducer)

        if self.client.request_timeout:
            # Start request timer
            t = self.client.request_timeout
            timeout = reactor.callLater(t,
                self.abort_request, requestAgent)

            def timeoutProxy(request):
                if timeout.active():
                    timeout.cancel()
                return self.http_response(request)

            def requestAborted(failure):
                failure.trap(defer.CancelledError,
                             error.ConnectingCancelledError)

                raise exceptions.RequestTimeout(
                    "Request took longer than %s seconds" % t)

            requestAgent.addCallback(timeoutProxy).addErrback(requestAborted)
        else:
            requestAgent.addCallback(self.http_response)

        return requestAgent

    def build_rest_path(self, bucket=None, key=None, params=None, prefix=None):
        """
        Given a RiakClient, RiakBucket, Key, LinkSpec, and Params,
        construct and return a URL.
        """
        # Build 'http://hostname:port/prefix/bucket'
        path = '' if not prefix else '/%s' %prefix

        # Add '.../bucket'
        if bucket is not None:
            path += '/types/%s/buckets/%s' %(urllib.quote_plus(bucket.bucket_type),urllib.quote_plus(bucket.name))

        # Add '.../key'
        if key is not None:
            path += '/keys/%s' %urllib.quote_plus(key)

        # Add query parameters.
        if params:
            s = ''
            for key in params.keys():
                if params[key] is not None:
                    if s != '':
                        s += '&'
                    s += urllib.quote_plus(key) + '='
                    s += urllib.quote_plus(str(params[key]))

            path += '?' + s

        # Return.
        return path

    def decodeJson(self, s):
        return self.client.get_decoder('application/json')(s)

    def encodeJson(self, s):
        return self.client.get_encoder('application/json')(s)

    @defer.inlineCallbacks
    def get_keys(self, bucket):
        params = {
            'props': 'true',
            'keys': 'true'
        }
        prefix = 'types/%s/buckets/%s/keys' %(bucket.bucket_type,bucket.name)
        url = self.build_rest_path(prefix = prefix, params=params)
        headers, encoded_props = yield self.http_request('GET', url)
        if headers['http_code'] == 200:
            props = self.decodeJson(encoded_props)
        else:
            raise Exception('Error getting bucket properties.')

        defer.returnValue(props['keys'])

    @defer.inlineCallbacks
    def set_bucket_props(self, bucket, props):
        """
        Set bucket properties
        """
        prefix = 'types/%s/buckets/%s/props' %(bucket.bucket_type,bucket.name)
        url = self.build_rest_path(prefix = prefix)
        headers = {'Content-Type': 'application/json'}
        content = self.encodeJson({'props': props})

        #Run the request...
        headers, response = yield self.http_request(
            'PUT', url, headers, content)

        # Handle the response...
        if (response is None):
            raise Exception('Error setting bucket properties.')

        # Check the response value...
        status = headers['http_code']

        if (status != 204):
            raise Exception('Error setting bucket properties.')

        defer.returnValue(response)

    def set_client_id(self, client_id):
        self._client_id = client_id

    def get_client_id(self):
        return self._client_id

    @defer.inlineCallbacks
    def ping(self):
        """
        Check server is alive over HTTP
        """
        response = yield self.http_request('GET', '/ping')
        res = (response is not None) and (response[1] == 'OK')
        defer.returnValue(res)

    @defer.inlineCallbacks
    def stats(self):
        """
        Gets performance statistics and server information
        """
        # TODO: use resource detection
        response = yield self.http_request(
            'GET', '/stats', {'Accept': 'application/json'})

        if response[0]['http_code'] is 200:
            defer.returnValue(self.decodeJson(response[1]))
        else:
            defer.returnValue(None)

    # FeatureDetection API - private
    @defer.inlineCallbacks
    def _server_version(self):
        stats = yield self.stats()
        if stats is not None:
            defer.returnValue(stats['riak_kv_version'])
        # If stats is disabled, we can't assume the Riak version
        # is >= 1.1. However, we can assume the new URL scheme is
        # at least version 1.0
        elif 'riak_kv_wm_buckets' in (yield self.get_resources()):
            defer.returnValue("1.0.0")
        else:
            defer.returnValue("0.14.0")

    @defer.inlineCallbacks
    def get_resources(self):
        """
        Gets a JSON mapping of server-side resource names to paths
        :rtype dict
        """
        response = yield self.http_request(
            'GET', '/', {'Accept': 'application/json'})

        if response[0]['http_code'] is 200:
            defer.returnValue(self.decodeJson(response[1]))
        else:
            defer.returnValue({})

    @defer.inlineCallbacks
    def get(self, robj, r=None, pr=None, vtag=None):
        """
        Get a bucket/key from the server
        """
        # We could detect quorum_controls here but HTTP ignores
        # unknown flags/params.
        params = {'r': r, 'pr': pr}

        if vtag is not None:
            params['vtag'] = vtag

        url = self.build_rest_path(robj.get_bucket(), robj.get_key(),
                                   params=params)
        response = yield self.http_request('GET', url)
        defer.returnValue(
            self.parse_body(response, [200, 300, 404])
        )

    @defer.inlineCallbacks
    def head(self, robj, r=None, pr=None, vtag=None):
        """
        Get metadata for a bucket/key from the server, basically
        the same as get() but retrieves no data
        """
        params = {'r': r, 'pr': pr}

        if vtag is not None:
            params['vtag'] = vtag
        url = self.build_rest_path(robj.get_bucket(), robj.get_key(),
                                   params=params)

        response = yield self.http_request('HEAD', url)
        defer.returnValue(
            self.parse_body(response, [200, 300, 404])
        )

    def put(self, robj, w=None, dw=None, pw=None, return_body=True,
            if_none_match=False):
        """
        Serialize put request and deserialize response
        """
        # We could detect quorum_controls here but HTTP ignores
        # unknown flags/params.
        params = {
            'returnbody': str(return_body).lower(),
            'w': w,
            'dw': dw,
            'pw': pw
        }
        url = self.build_rest_path(bucket=robj.get_bucket(),
                                   key=robj.get_key(),
                                   params=params)
        headers = self.build_put_headers(robj)

        # TODO: use a more general 'prevent_stale_writes' semantics,
        # which is a superset of the if_none_match semantics.
        if if_none_match:
            headers["If-None-Match"] = "*"
        content = robj.get_encoded_data()
        return self.do_put(
            url, headers, content, return_body, key=robj.get_key())

    @defer.inlineCallbacks
    def do_put(self, url, headers, content, return_body=False, key=None):
        if key is None:
            response = yield self.http_request('POST', url, headers, content)
        else:
            response = yield self.http_request('PUT', url, headers, content)

        if return_body:
            defer.returnValue(self.parse_body(response, [200, 201, 300]))
        else:
            self.check_http_code(response, [204])
            defer.returnValue(None)

    @defer.inlineCallbacks
    def put_new(self, robj, w=None, dw=None, pw=None, return_body=True,
                if_none_match=False):
        """Put a new object into the Riak store, returning its (new) key."""
        # We could detect quorum_controls here but HTTP ignores
        # unknown flags/params.
        params = {
            'returnbody': str(return_body).lower(),
            'w': w,
            'dw': dw,
            'pw': pw
        }
        url = self.build_rest_path(bucket=robj.get_bucket(), params=params)
        headers = self.build_put_headers(robj)
        # TODO: use a more general 'prevent_stale_writes' semantics,
        # which is a superset of the if_none_match semantics.
        if if_none_match:
            headers["If-None-Match"] = "*"
        content = robj.get_encoded_data()
        response = yield self.http_request('POST', url, headers, content)
        location = response[0]['location']
        idx = location.rindex('/')
        key = location[idx + 1:]
        if return_body:
            vclock, [(metadata, data)] = self.parse_body(response, [201])
            defer.returnValue((key, vclock, metadata))
        else:
            self.check_http_code(response, [201])
            defer.returnValue((key, None, None))

    @defer.inlineCallbacks
    def get_counter(self, bucket, key, **options):
        counters = yield self.counters()
        if not counters:
            raise NotImplementedError("Counters are not supported")

        url = '/buckets/%s/counters/%s' %(bucket,key)
        headers, data = response = yield self.get_request(url)

        self.check_http_code(response, [200, 404])
        if status == 200:
            defer.returnValue( long(data.strip()) )
        elif status == 404:
            defer.returnValue( None )

    @defer.inlineCallbacks
    def update_counter(self, bucket, key, amount, **options):
        counters = yield self.counters()
        if not counters:
            raise NotImplementedError("Counters are not supported")
        return_value = 'returnvalue' in options and options['returnvalue']
        content_type  = 'text/plain'
        url = '/buckets/%s/counters/%s' %(bucket,key)

        headers, body = response = yield self.post_request(url, str(amount),content_type = content_type)
        status = response[0]['http_code']

        if return_value and status == 200:
            defer.returnValue( long(body.strip()) )
        elif status == 204:
            defer.returnValue( True )
        else:
            self.check_http_code(response, [200, 204])

    @defer.inlineCallbacks
    def delete(self, robj, rw=None, r=None, w=None, dw=None, pr=None,
               pw=None):
        """
        Delete an object.
        """
        # We could detect quorum_controls here but HTTP ignores
        # unknown flags/params.
        params = {'rw': rw, 'r': r, 'w': w, 'dw': dw, 'pr': pr, 'pw': pw}
        headers = {}
        url = self.build_rest_path(robj.get_bucket(), robj.get_key(),
                                   params=params)
        ts = yield self.tombstone_vclocks()
        if ts and robj.vclock() is not None:
            headers['X-Riak-Vclock'] = robj.vclock()
        response = yield self.http_request('DELETE', url, headers)
        self.check_http_code(response, [204, 404])
        defer.returnValue(self)

    @defer.inlineCallbacks
    def get_buckets(self,bucket_type = 'default'):
        """
        Fetch a list of all buckets
        """
        params = {'buckets': 'true'}
        prefix = '/types/%s/buckets' %bucket_type
        url = self.build_rest_path(None, prefix = prefix,params=params)
        response = yield self.http_request('GET', url)

        headers, encoded_props = response[0:2]
        if headers['http_code'] == 200:
            props = self.decodeJson(encoded_props)
        else:
            raise Exception('Error getting buckets.')

        defer.returnValue(props['buckets'])

    @defer.inlineCallbacks
    def get_bucket_props(self, bucket):
        """
        Get properties for a bucket
        """
        # Run the request...
        prefix = 'types/%s/buckets/%s/props' %(bucket.bucket_type,bucket.name)
        url = self.build_rest_path(prefix = prefix)
        response = yield self.http_request('GET', url)

        headers = response[0]
        encoded_props = response[1]
        if headers['http_code'] == 200:
            props = self.decodeJson(encoded_props)
            defer.returnValue(props['props'])
        else:
            raise Exception('Error getting bucket properties.')

    @defer.inlineCallbacks
    def reset_bucket_props(self, bucket):
        """
        Reset properties for a bucket
        """
        rbp = yield self.has_reset_bucket_props_api()
        if not rbp:
            raise Exception('Resetting of bucket properties is '
                            'not supported by this Riak node')

        # Run the request...
        prefix = 'types/%s/buckets/%s/props' %(bucket.bucket_type,bucket.name)
        url = self.build_rest_path(prefix = prefix)
        response = yield self.http_request('DELETE', url)

        headers = response[0]
        if headers['http_code'] != 204:
            raise Exception('Error resetting bucket properties.')

    @defer.inlineCallbacks
    def mapred(self, inputs, query, timeout=None):
        """
        Run a MapReduce query.
        """
        plm = yield self.phaseless_mapred()
        if not plm and (query is None or len(query) is 0):
            raise Exception('Phase-less MapReduce is not supported '
                            'by this Riak node')

        # Construct the job, optionally set the timeout...
        job = {'inputs': inputs, 'query': query}
        if timeout is not None:
            job['timeout'] = timeout
        content = self.encodeJson(job)

        # Do the request...
        url = "/" + self.client._mapred_prefix
        headers = {'Content-Type': 'application/json'}
        response = yield self.http_request('POST', url, headers, content)

        # Make sure the expected status code came back...
        status = response[0]['http_code']
        if status != 200:
            raise Exception(
                'Error running MapReduce operation. Headers: %s Body: %s' %
                (repr(response[0]), repr(response[1]))
            )

        result = self.decodeJson(response[1])
        defer.returnValue(result)

    @defer.inlineCallbacks
    def get_index(self, bucket, index, startkey, endkey=None,bucket_type='default',
            return_terms=False, max_results=None,continuation=None):
        """
        Performs a secondary index query.
        """
        # TODO: use resource detection
        params = {'return_terms': return_terms, 'max_results': max_results,
                  'continuation': continuation}
        p = {}
        for k,v in params.iteritems():
            if isinstance(v,bool):
                p[k]=str(v).lower()
            elif v == None:
                continue
            else:
                p[k]=v
        query_params = urllib.urlencode(p) if p else ''
        segments = ["types",bucket_type,"buckets", bucket, "index", index, str(startkey)]
        if endkey:
            segments.append(str(endkey))
        uri = '/%s' % ('/'.join(segments))
        if p:
            uri="%s?%s" %(uri,urllib.urlencode(p))
        headers, data = response = yield self.get_request(uri)
        self.check_http_code(response, [200])
        jsonData = self.decodeJson(data)
        defer.returnValue(jsonData)
        #defer.returnValue(jsonData[u'keys'][:])

    @defer.inlineCallbacks
    def create_search_schema(self, schema,content):
#        if not (yield self.pb_search_admin()):
            #raise NotImplementedError("Yokozuna administration is not "
                                      #"supported for this version")
        url = '/search/schema/%s' %urllib.quote_plus(schema)
        content_type =  'application/xml'
        headers, body = response = yield self.put_request(url, content ,content_type = content_type)
        defer.returnValue(True)

    @defer.inlineCallbacks
    def get_search_schema(self, schema):
#        if not (yield self.pb_search_admin()):
            #raise NotImplementedError("Yokozuna administration is not "
                                      #"supported for this version")
        url = '/search/schema/%s' %urllib.quote_plus(schema)
        headers,body = response = yield self.get_request(url)
        self.check_http_code(response, [200])
        defer.returnValue(body)

    @defer.inlineCallbacks
    def create_search_index(self, index, schema=None, n_val=None):
#        if not (yield self.pb_search_admin()):
            #raise NotImplementedError("Yokozuna administration is not "
                                      #"supported for this version")
        url = '/search/index/%s' %urllib.unquote_plus(index)
        req = {}
        if schema:
            req['schema'] = schema
        if n_val:
            req['n_val'] = n_val
        data = self.encodeJson(req)
        headers,body = response = yield self.put_request(url,data)
        self.check_http_code(response, [409,204]) #returns 409 if already existed
        defer.returnValue(True)

    @defer.inlineCallbacks
    def delete_search_index(self, index):
#        if not (yield self.pb_search_admin()):
            #raise NotImplementedError("Yokozuna administration is not "
                                      #"supported for this version")
        url = '/search/index/%s' %index
        yield self.http_request('DELETE',url)
        self.check_http_code(response, [200]) #need some header test here
        defer.returnValue(True)

    @defer.inlineCallbacks
    def get_search_index(self, index):
        if not (yield self.pb_search_admin()):
            raise NotImplementedError("Yokozuna administration is not "
                                      "supported for this version")
        url = '/search/index/%s' %urllib.unquote_plus(index)
        headers,body = response = yield self.get_request(url)
        self.check_http_code(response, [200])
        data = self.decodeJson(body)
        defer.returnValue(data)

    @defer.inlineCallbacks
    def list_search_indexes(self):
#        if not (yield self.pb_search_admin()):
            #raise NotImplementedError("Yokozuna administration is not "
                                      #"supported for this version")
        url = '/search/index'
        headers,body = response = yield self.get_request(url)
        self.check_http_code(response, [200])
        data = self.decodeJson(body)
        defer.returnValue(data)


    @defer.inlineCallbacks
    def fetch_datatype(self, bucket, key,*args,**kwargs):
        """
        fetch_datatype(bucket, key, r=None, pr=None, basic_quorum=None,
                       notfound_ok=None, timeout=None, include_context=None)

        Fetches the value of a Riak Datatype.

        .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.

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
        url = 'types/%s/buckets/%s/datatypes/%s' %(bucket.bucket_type,bucket.name,key)
        req = {}
        datatype = yield bucket.get_property('datatype')
        for attr in ['r','pr','basic_quorum','notfound_ok','include_context']:
            if kwargs.get(attr,''):
                req.__setitem__(attr,kwargs[attr])
        header,body = response = yield self.get_request(url,req)
        self.check_http_code(response, [200,404])
        if header['http_code'] == 404:
            result = new(datatype,bucket,key)
            defer.returnValue(result)
        else:
            data = self.decodeJson(body)
            data = self.decode_datatype(data,bucket,key)
            defer.returnValue(data)

    def parse_json_datatype(self,obj):
        objs = {}
        for k,v in obj.iteritems():
            if k.endswith('_set'):
                objs[(k,'set')]=frozenset(v)
            elif k.endswith('_register'):
                objs[(k,'register')]=v
            elif k.endswith('_counter'):
                objs[(k,'counter')]=long(v)
            elif k.endswith('_flag'):
                objs[(k,'flag')]=v
            elif k.endswith('_map'):
                objs[(k,'map')]=parse_json_datatype(v)
        return objs

    def decode_datatype(self,json,bucket,key):
        dt = json['type']
        value = json['value']
        if dt == 'counter':
            return Counter(value,bucket=bucket,key=key)
        elif dt == 'set':
            return Set(frozenset(value),bucket=bucket,key=key)
        elif dt == 'map':
            value = self.parse_json_datatype(value)
            return Map(value,bucket = bucket,key = key)


    def encode_operation(self,datatype):
        if isinstance(datatype,Counter):
            key = 'increment' if datatype.to_op() > 0 else 'decrement'
            op = { key : datatype.to_op() } 
        elif isinstance(datatype,Register):
            op = {'assign' : datatype.to_op()}
        elif isinstance(datatype,Flag):
            op = 'enable' if True else 'disable'
        elif isinstance(datatype,Set):
            op = {}
            if datatype.to_op() and datatype.to_op().get('adds',[]):
                op['add_all'] = datatype.to_op()['adds']
            if datatype.to_op() and datatype.to_op().get('removes',[]):
                op['remove_all'] = datatype.to_op()['removes']
            return op
        elif isinstance(datatype,Map):
            op = {
                    'add' : [ '%s_%s' %(k,t) for k,t in datatype._adds ],
                    'remove' : [ '%s_%s' %(k,t) for k,t in datatype._removes ],
                    'update' : {},
                  }
            for (k,t),d in datatype._updates.iteritems():
                op['update'][ '%s_%s' %( k,t ) ] = self.encode_operation(d ) 
        else:
            raise Exception("Unknown datatype")
        return op

    @defer.inlineCallbacks
    def update_datatype(self, datatype, *args,**kwargs):
        """
        Updates a Riak Datatype. This operation is not idempotent and
        so will not be retried automatically.

        :param datatype: the datatype to update
        :type datatype: a subclass of :class:`~riak.datatypes.Datatype`
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
        prefix = 'types/%s/buckets/%s/datatypes/%s' %(datatype.bucket.bucket_type,datatype.bucket.name,datatype.key)
        req = {}
#        if datatype.context:
            #req.context = datatype.context
        for attr in ['w','dw','pw','timeout','return_body','include_context']:
            if kwargs.get(attr,''):
                req.__setitem__(attr,kwargs[attr])
        url = self.build_rest_path(prefix = prefix,params = req)
        op = self.encodeJson(self.encode_operation(datatype))
        header,body = response = yield self.post_request(url,op)
        self.check_http_code(response, [204])

    @defer.inlineCallbacks
    def search(self, index, query, **params):
        """
        Performs a search query.
        """
        if index is None:
            index = 'search'

        options = {'q': query, 'wt': 'json'}
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
        # TODO: use resource detection
        uri = "/solr/%s/select" % index
        headers, data = response = yield self.get_request(uri, options)
        self.check_http_code(response, [200])
        if 'json' in headers['content-type']:
            results = self.decodeJson(data)
            defer.returnValue(self._normalize_json_search_response(results))
        elif 'xml' in headers['content-type']:
            defer.returnValue(self._normalize_xml_search_response(data))
        else:
            raise ValueError("Could not decode search response")

    def check_http_code(self, response, expected_statuses):
        status = response[0]['http_code']
        if not status in expected_statuses:
            m = 'Expected status %s, received %s : %s' % (
                str(expected_statuses),
                str(status),
                response[1]
            )
            raise Exception(m)

    def parse_body(self, response, expected_statuses):
        """
        Given the output of RiakUtils.http_request and a list of
        statuses, populate the object. Only for use by the Riak client
        library.
        @return self
        """
        # If no response given, then return.
        if response is None:
            return self

        # Make sure expected code came back
        self.check_http_code(response, expected_statuses)

        # Update the object...
        headers = response[0]
        data = response[1]
        status = headers['http_code']

        # Check if the server is down(status==0)
        if not status:
            ### we need the host/port that was used.
            m = 'Could not contact Riak Server: http://$HOST:$PORT !'
            raise RiakError(m)

        # If 404(Not Found), then clear the object.
        if status == 404:
            return None

        # If 300(Siblings), then return the list of siblings
        elif status == 300:
            # Parse and get rid of 'Siblings:' string in element 0
            siblings = data.strip().split('\n')
            siblings.pop(0)
            return siblings

        # Parse the headers...
        vclock = None
        metadata = {MD_USERMETA: {}, MD_INDEX: []}
        links = []
        for header, value in headers.iteritems():
            if header == 'content-type':
                metadata[MD_CTYPE] = value
            elif header == 'charset':
                metadata[MD_CHARSET] = value
            elif header == 'content-encoding':
                metadata[MD_ENCODING] = value
            elif header == 'etag':
                metadata[MD_VTAG] = value
            elif header == 'link':
                self.parse_links(links, headers['link'])
            elif header == 'last-modified':
                metadata[MD_LASTMOD] = value
            elif header.startswith('x-riak-meta-'):
                metadata[MD_USERMETA][
                    header.replace('x-riak-meta-', '')
                ] = value
            elif header.startswith('x-riak-index-'):
                field = header.replace('x-riak-index-', '')
                reader = csv.reader([value], skipinitialspace=True)
                for line in reader:
                    for token in line:
                        rie = RiakIndexEntry(field, token)
                        metadata[MD_INDEX].append(rie)
            elif header == 'x-riak-vclock':
                vclock = value
            elif header == 'x-riak-deleted':
                metadata[MD_DELETED] = True
        if links:
            metadata[MD_LINKS] = links

        return vclock, [(metadata, data)]

    def to_link_header(self, link):
        """
        Convert this RiakLink object to a link header string. Used internally.
        """
        bucket = link.get_bucket()
        key = "/types/%s/buckets/%s/keys/%s" %(bucket.bucket_type,bucket.name,link.get_key())
        header = '<%s>; riaktag="%s"' %(key,urllib.quote_plus(link.get_tag()))
        return header

    def parse_links(self, links, linkHeaders):
        """
        Private.
        @return self
        """
        for linkHeader in linkHeaders.strip().split(','):
            linkHeader = linkHeader.strip()

            linktag = re.compile(
                "</([^/]+)/([^/]+)/([^/]+)>; ?riaktag=\"([^\']+)\"")

            bucket = re.compile(
                "</(buckets)/([^/]+)/keys/([^/]+)>; ?riaktag=\"([^\']+)\"")

            matches = linktag.match(linkHeader) or bucket.match(linkHeader)

            if matches is not None:
                link = RiakLink(urllib.unquote_plus(matches.group(2)),
                                urllib.unquote_plus(matches.group(3)),
                                urllib.unquote_plus(matches.group(4)))
                links.append(link)
        return self

    def add_links_for_riak_object(self, robject, headers):
        links = robject.get_links()
        if links:
            current_header = ''
            for link in links:
                header = self.to_link_header(link)
                if len(current_header + header) > MAX_LINK_HEADER_SIZE:
                    current_header = ''

                if current_header != '':
                    header = ', ' + header

                current_header += header

            headers['Link'] = current_header

        return headers

    def get_request(self, uri=None, params=None):
        url = self.build_rest_path(bucket=None, params=params, prefix=uri)

        return self.http_request('GET', url)



    def put_request(self, uri=None, body=None, params=None,
                     content_type="application/json"):
        uri = self.build_rest_path(prefix=uri, params=params)
        return self.http_request(
            'PUT', uri, {'Content-Type': content_type}, body)

    def post_request(self, uri=None, body=None, params=None,
                     content_type="application/json"):
        uri = self.build_rest_path(prefix=uri, params=params)
        return self.http_request(
            'POST', uri, {'Content-Type': content_type}, body)

    # Utility functions used by Riak library.

    def build_put_headers(self, robj):
        """Build the headers for a POST/PUT request."""

        # Construct the headers...
        headers = {
            'Accept': 'text/plain, */*; q=0.5',
            'Content-Type': robj.get_content_type(),
            'X-Riak-ClientId': self._client_id
        }

        # Add the vclock if it exists...
        if robj.vclock() is not None:
            headers['X-Riak-Vclock'] = robj.vclock()

        # Create the header from metadata
        links = self.add_links_for_riak_object(robj, headers)

        for key, value in robj.get_usermeta().iteritems():
            headers['X-Riak-Meta-%s' % key] = value

        for rie in robj.get_indexes():
            key = 'X-Riak-Index-%s' % rie.get_field()
            if key in headers:
                headers[key] += ", " + rie.get_value()
            else:
                headers[key] = rie.get_value()

        return headers

    def _normalize_json_search_response(self, json):
        """
        Normalizes a JSON search response so that PB and HTTP have the
        same return value
        """
        result = {}
        if u'response' in json:
            result['num_found'] = json[u'response'][u'numFound']
            result['max_score'] = float(json[u'response'].get(u'maxScore',0))
            docs = json['response']['docs']
#            for doc in json[u'response'][u'docs']:
                #resdoc = {u'id': doc[u'_yz_rk']}
                #if u'fields' in doc:
                    #for k, v in doc[u'fields'].iteritems():
                        #resdoc[k] = v
                #docs.append(resdoc)
            result['docs'] = docs
        return result

    def _normalize_xml_search_response(self, xml):
        """
        Normalizes an XML search response so that PB and HTTP have the
        same return value
        """
        target = XMLSearchResult()
        parser = ElementTree.XMLParser(target=target)
        parser.feed(xml)
        return parser.close()

    @classmethod
    def build_headers(cls, headers):
        return ['%s: %s' % (header, value)
                for header, value in headers.iteritems()]

    @classmethod
    def parse_http_headers(cls, headers):
        """
        Parse an HTTP Header string into an asssociative array of
        response headers.
        """
        retVal = {}
        fields = headers.split("\n")
        for field in fields:
            matches = re.match("([^:]+):(.+)", field)
            if matches is None:
                continue
            key = matches.group(1).lower()
            value = matches.group(2).strip()
            if key in retVal.keys():
                if  isinstance(retVal[key], list):
                    retVal[key].append(value)
                else:
                    retVal[key] = [retVal[key]].append(value)
            else:
                retVal[key] = value
        return retVal


