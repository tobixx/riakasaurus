from twisted.protocols.basic import Int32StringReceiver
from twisted.internet.protocol import ClientFactory
from twisted.internet.defer import Deferred
from twisted.internet import defer, reactor
from twisted.python.failure import Failure

from struct import pack, unpack

from pprint import pformat

# generated code from *.proto message definitions
from riakasaurus.transport.pbc import riak_kv_pb2, riak_pb2,riak_search_pb2,riak_yokozuna_pb2,riak_dt_pb2
from riakasaurus import exceptions
import dt_codec

## Protocol codes
MSG_CODE_ERROR_RESP = 0
MSG_CODE_PING_REQ = 1
MSG_CODE_PING_RESP = 2
MSG_CODE_GET_CLIENT_ID_REQ = 3
MSG_CODE_GET_CLIENT_ID_RESP = 4
MSG_CODE_SET_CLIENT_ID_REQ = 5
MSG_CODE_SET_CLIENT_ID_RESP = 6
MSG_CODE_GET_SERVER_INFO_REQ = 7
MSG_CODE_GET_SERVER_INFO_RESP = 8
MSG_CODE_GET_REQ = 9
MSG_CODE_GET_RESP = 10
MSG_CODE_PUT_REQ = 11
MSG_CODE_PUT_RESP = 12
MSG_CODE_DEL_REQ = 13
MSG_CODE_DEL_RESP = 14
MSG_CODE_LIST_BUCKETS_REQ = 15
MSG_CODE_LIST_BUCKETS_RESP = 16
MSG_CODE_LIST_KEYS_REQ = 17
MSG_CODE_LIST_KEYS_RESP = 18
MSG_CODE_GET_BUCKET_REQ = 19
MSG_CODE_GET_BUCKET_RESP = 20
MSG_CODE_SET_BUCKET_REQ = 21
MSG_CODE_SET_BUCKET_RESP = 22
MSG_CODE_MAPRED_REQ = 23
MSG_CODE_MAPRED_RESP = 24
MSG_CODE_INDEX_REQ = 25
MSG_CODE_INDEX_RESP = 26
MSG_CODE_SEARCH_QUERY_REQ = 27
MSG_CODE_SEARCH_QUERY_RESP = 28
MSG_CODE_RESET_BUCKET_REQ = 29
MSG_CODE_RESET_BUCKET_RESP = 30
MSG_CODE_COUNTER_UPDATE_REQ = 50
MSG_CODE_COUNTER_UPDATE_RESP = 51
MSG_CODE_COUNTER_GET_REQ = 52
MSG_CODE_COUNTER_GET_RESP = 53
MSG_CODE_YOKOZUNA_INDEX_GET_REQ = 54
MSG_CODE_YOKOZUNA_INDEX_GET_RESP = 55
MSG_CODE_YOKOZUNA_INDEX_PUT_REQ = 56
MSG_CODE_YOKOZUNA_INDEX_DELETE_REQ = 57
MSG_CODE_YOKOZUNA_SCHEMA_GET_REQ = 58
MSG_CODE_YOKOZUNA_SCHEMA_GET_RESP = 59
MSG_CODE_YOKOZUNA_SCHEMA_PUT_REQ = 60
MSG_CODE_DATATYPE_FETCH_REQ = 80
MSG_CODE_DATATYPE_FETCH_RESP = 81
MSG_CODE_DATATYPE_UPDATE_REQ = 82
MSG_CODE_DATATYPE_UPDATE_RESP = 83

## Inject PBC classes

RpbGetClientIdResp = riak_kv_pb2.RpbGetClientIdResp
RpbSetClientIdReq = riak_kv_pb2.RpbSetClientIdReq
RpbListBucketsReq = riak_kv_pb2.RpbListBucketsReq
RpbGetReq = riak_kv_pb2.RpbGetReq
RpbGetResp = riak_kv_pb2.RpbGetResp
RpbPutReq = riak_kv_pb2.RpbPutReq
RpbPutResp = riak_kv_pb2.RpbPutResp
RpbDelReq = riak_kv_pb2.RpbDelReq
RpbListBucketsResp = riak_kv_pb2.RpbListBucketsResp
RpbListKeysReq = riak_kv_pb2.RpbListKeysReq
RpbListKeysResp = riak_kv_pb2.RpbListKeysResp
RpbGetBucketReq = riak_pb2.RpbGetBucketReq
RpbGetBucketResp = riak_pb2.RpbGetBucketResp
RpbSetBucketReq = riak_pb2.RpbSetBucketReq
RpbResetBucketReq = riak_pb2.RpbResetBucketReq
RpbMapRedReq = riak_kv_pb2.RpbMapRedReq
RpbMapRedResp = riak_kv_pb2.RpbMapRedResp

RpbCounterGetReq = riak_kv_pb2.RpbCounterGetReq
RpbCounterGetResp = riak_kv_pb2.RpbCounterGetResp
RpbCounterUpdateReq = riak_kv_pb2.RpbCounterUpdateReq
RpbCounterUpdateResp = riak_kv_pb2.RpbCounterUpdateResp

RpbSearchQueryReq = riak_search_pb2.RpbSearchQueryReq
RpbSearchQueryResp = riak_search_pb2.RpbSearchQueryResp

RpbIndexReq = riak_kv_pb2.RpbIndexReq
RpbIndexResp = riak_kv_pb2.RpbIndexResp
RpbContent = riak_kv_pb2.RpbContent
RpbLink = riak_kv_pb2.RpbLink
RpbBucketProps = riak_pb2.RpbBucketProps

RpbErrorResp = riak_pb2.RpbErrorResp
RpbGetServerInfoResp = riak_pb2.RpbGetServerInfoResp
RpbPair = riak_pb2.RpbPair

RpbYokozunaIndex = riak_yokozuna_pb2.RpbYokozunaIndex
RpbYokozunaIndexPutReq = riak_yokozuna_pb2.RpbYokozunaIndexPutReq

RpbYokozunaIndexGetReq = riak_yokozuna_pb2.RpbYokozunaIndexGetReq
RpbYokozunaIndexGetResp = riak_yokozuna_pb2.RpbYokozunaIndexGetResp

RpbYokozunaIndexDeleteReq = riak_yokozuna_pb2.RpbYokozunaIndexDeleteReq
RpbYokozunaIndexPutReq = riak_yokozuna_pb2.RpbYokozunaIndexPutReq

RpbYokozunaSchema = riak_yokozuna_pb2.RpbYokozunaSchema
RpbYokozunaSchemaGetReq = riak_yokozuna_pb2.RpbYokozunaSchemaGetReq
RpbYokozunaSchemaGetResp = riak_yokozuna_pb2.RpbYokozunaSchemaGetResp
RpbYokozunaSchemaPutReq = riak_yokozuna_pb2.RpbYokozunaSchemaPutReq


DtFetchReq = riak_dt_pb2.DtFetchReq
DtFetchResp = riak_dt_pb2.DtFetchResp
DtUpdateReq = riak_dt_pb2.DtUpdateReq
DtUpdateResp = riak_dt_pb2.DtUpdateResp



def toHex(s):
    lst = []
    for ch in s:
        hv = hex(ord(ch)).replace('0x', '')
        if len(hv) == 1:
            hv = '0' + hv
        lst.append(hv + ' ')

    return reduce(lambda x, y: x + y, lst)


class RiakPBC(Int32StringReceiver):

    MAX_LENGTH = 9999999

    riakResponses = {
        MSG_CODE_ERROR_RESP: RpbErrorResp,
        MSG_CODE_GET_CLIENT_ID_RESP: RpbGetClientIdResp,
        MSG_CODE_GET_RESP: RpbGetResp,
        MSG_CODE_PUT_RESP: RpbPutResp,
        MSG_CODE_LIST_KEYS_RESP: RpbListKeysResp,
        MSG_CODE_LIST_BUCKETS_RESP: RpbListBucketsResp,
        MSG_CODE_GET_BUCKET_RESP: RpbGetBucketResp,
        MSG_CODE_COUNTER_GET_RESP : RpbCounterGetResp,
        MSG_CODE_COUNTER_UPDATE_RESP : RpbCounterUpdateResp,
        MSG_CODE_GET_SERVER_INFO_RESP: RpbGetServerInfoResp,
        MSG_CODE_INDEX_RESP: RpbIndexResp,
        MSG_CODE_SEARCH_QUERY_RESP: RpbSearchQueryResp,
        MSG_CODE_MAPRED_RESP:RpbMapRedResp,
        MSG_CODE_YOKOZUNA_INDEX_GET_RESP:RpbYokozunaIndexGetResp,
        MSG_CODE_YOKOZUNA_SCHEMA_GET_RESP:RpbYokozunaSchemaGetResp,
        MSG_CODE_YOKOZUNA_INDEX_GET_RESP:RpbYokozunaIndexGetResp,
        MSG_CODE_YOKOZUNA_SCHEMA_GET_RESP:RpbYokozunaSchemaGetResp,
        MSG_CODE_DATATYPE_FETCH_RESP:DtFetchResp,
        MSG_CODE_DATATYPE_UPDATE_RESP:DtUpdateResp,
    }

    PBMessageTypes = {
        0: 'ERROR_RESP',
        1: 'PING_REQ',
        2: 'PING_RESP',
        3: 'GET_CLIENT_ID_REQ',
        4: 'GET_CLIENT_ID_RESP',
        5: 'SET_CLIENT_ID_REQ',
        6: 'SET_CLIENT_ID_RESP',
        7: 'GET_SERVER_INFO_REQ',
        8: 'GET_SERVER_INFO_RESP',
        9: 'GET_REQ',
        10: 'GET_RESP',
        11: 'PUT_REQ',
        12: 'PUT_RESP',
        13: 'DEL_REQ',
        14: 'DEL_RESP',
        15: 'LIST_BUCKETS_REQ',
        16: 'LIST_BUCKETS_RESP',
        17: 'LIST_KEYS_REQ',
        18: 'LIST_KEYS_RESP',
        19: 'GET_BUCKET_REQ',
        20: 'GET_BUCKET_RESP',
        21: 'SET_BUCKET_REQ',
        22: 'SET_BUCKET_RESP',
        23: 'MAPRED_REQ',
        24: 'MAPRED_RESP',
        25: 'INDEX_REQ',
        26: 'INDEX_RESP',
        27: 'SEARCH_QUERY_REQ',
        28: 'SEARCH_QUERY_RESP',
        55: 'MSG_CODE_YOKOZUNA_INDEX_GET_RESP',
        59: 'MSG_CODE_YOKOZUNA_SCHEMA_GET_RESP',
    }

    nonMessages = (
        MSG_CODE_PING_RESP,
        MSG_CODE_DEL_RESP,
        MSG_CODE_SET_BUCKET_RESP,
        MSG_CODE_SET_CLIENT_ID_RESP,
        MSG_CODE_RESET_BUCKET_RESP
    )

    rwNums = {
        'one': 4294967295 - 1,
        'quorum': 4294967295 - 2,
        'all': 4294967295 - 3,
        'default': 4294967295 - 4,
    }

    timeout = None
    timeoutd = None
    disconnected = False
    debug = 0

    # ------------------------------------------------------------------
    # Server Operations .. setClientId, getClientId, getServerInfo, ping
    # ------------------------------------------------------------------
    def setClientId(self, clientId):
        code = pack('B', MSG_CODE_SET_CLIENT_ID_REQ)
        request = RpbSetClientIdReq()
        request.client_id = clientId
        return self.__send(code, request)

    def getClientId(self):
        code = pack('B', MSG_CODE_GET_CLIENT_ID_REQ)
        return self.__send(code)

    def getServerInfo(self):
        code = pack('B', MSG_CODE_GET_SERVER_INFO_REQ)
        return self.__send(code)

    def ping(self):
        code = pack('B', MSG_CODE_PING_REQ)
        return self.__send(code)

    # ------------------------------------------------------------------
    # Object/Key Operations .. get(fetch), put(store), delete
    # ------------------------------------------------------------------
    def get(self, bucket, key, **kwargs):
        code = pack('B', MSG_CODE_GET_REQ)
        request = RpbGetReq()
        request.bucket = bucket
        request.key = key
        request.type = kwargs.pop('bucket_type','default')
        if 'r' in kwargs:
            request.r = self._resolveNums(kwargs['r'])
        if 'pr' in kwargs:
            request.pr = self._resolveNums(kwargs['pr'])
        if 'basic_quorum' in kwargs:
            request.basic_quorum = kwargs['basic_quorum']
        if 'notfound_ok' in kwargs:
            request.notfound_ok = kwargs['notfound_ok']
        if 'if_modified' in kwargs:
            request.if_modified = kwargs['if_modified']
        if 'head' in kwargs:
            request.head = kwargs['head']
        if 'deletedvclock' in kwargs:
            request.deletedvclock = kwargs['deletedvclock']

        return self.__send(code, request)

    def get_index(self, bucket, index, startkey, endkey=None,return_terms=False, max_results=None, continuation=None,bucket_type = 'default'):
        code = pack('B', MSG_CODE_INDEX_REQ)
        req = RpbIndexReq(bucket=bucket, index=index,type=bucket_type)
        self.__indexResultList = []
        if endkey:
            req.qtype = RpbIndexReq.range
            req.range_min = str(startkey)
            req.range_max = str(endkey)
            req.return_terms = return_terms if isinstance(return_terms,bool) else False
        else:
            req.qtype = RpbIndexReq.eq
            req.key = str(startkey)
        if max_results:
            req.max_results = max_results
        if continuation:
            req.continuation = continuation
        req.stream = True
        d = self.__send(code, req)
        d.addCallback(lambda resp: resp)
        return d

    def create_search_index(self, index, schema=None, n_val=None):
        code = pack('B', MSG_CODE_YOKOZUNA_INDEX_PUT_REQ)
        idx = RpbYokozunaIndex(name=index)
        if schema:
            idx.schema = schema
        if n_val:
            idx.n_val = n_val
        req = RpbYokozunaIndexPutReq(index=idx)
        d = self.__send(code, req)
        d.addCallback(lambda resp:resp)
        return d

    def get_search_index(self, index):
        code = pack('B', MSG_CODE_YOKOZUNA_INDEX_GET_REQ)
        req = RpbYokozunaIndexGetReq(name=index)
        d = self.__send(code, req)
        d.addCallback(lambda resp:resp)
        return d

    def delete_search_index(self, index):
        code = pack('B', MSG_CODE_YOKOZUNA_INDEX_DELETE_REQ)
        req = RpbYokozunaIndexDeleteReq(name=index)
        d = self.__send(code, req)
        d.addCallback(lambda resp:resp)
        return d

    def list_search_indexes(self):
        code = pack('B', MSG_CODE_YOKOZUNA_INDEX_GET_REQ)
        req = RpbYokozunaIndexGetReq()
        d = self.__send(code, req)
        d.addCallback(lambda resp:resp)
        return d

    def create_search_schema(self, schema, content):
        code = pack('B', MSG_CODE_YOKOZUNA_SCHEMA_PUT_REQ)
        scma = RpbYokozunaSchema(name=schema, content=content)
        req = RpbYokozunaSchemaPutReq(schema=scma)
        d = self.__send(code, req)
        d.addCallback(lambda resp:resp)
        return d

    def get_search_schema(self, schema):
        code = pack('B', MSG_CODE_YOKOZUNA_SCHEMA_GET_REQ)
        req = RpbYokozunaSchemaGetReq(name=schema)
        d = self.__send(code,req)
        d.addCallback(lambda resp:resp)
        return d

    def put_new(self, bucket, key, content, vclock=None, **kwargs):
        return put(bucket, key, content, vclock, kwargs)

    def get_counter(self, bucket, key , **params):
        code = pack('B', MSG_CODE_COUNTER_GET_REQ)
        req = RpbCounterGetReq(bucket = bucket, key = key)
        if params.get('r') is not None:
            req.r = int(params['r'])
        if params.get('pr') is not None:
            req.pr = int(params['pr'])
        if params.get('basic_quorum') is not None:
            req.basic_quorum = params['basic_quorum']
        if params.get('notfound_ok') is not None:
            req.notfound_ok = params['notfound_ok']

        d = self.__send(code, req)
        d.addCallback(lambda resp: resp.value if resp.HasField('value') else None)
        return d

    def update_counter(self,bucket,key,value,**params):
        code = pack('B', MSG_CODE_COUNTER_UPDATE_REQ)
        req = RpbCounterUpdateReq(bucket = bucket, key = key, amount = long(value))
        if params.get('w') is not None:
            req.w = self.int(params['w'])
        if params.get('dw') is not None:
            req.dw = self.int(params['dw'])
        if params.get('pw') is not None:
            req.pw = self.int(params['pw'])
        if params.get('returnvalue') is not None:
            req.returnvalue = params['returnvalue']
        d = self.__send(code, req)
        d.addCallback(lambda resp: resp.value if resp.HasField('value') else True)
        return d


    def fetch_datatype(self, bucket, key,bucket_type,*args,**kwargs):
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
        code = pack('B', MSG_CODE_DATATYPE_FETCH_REQ)
        req = DtFetchReq()
        req.bucket = bucket
        req.key = key
        req.type = bucket_type
        for attr in ['r','pr','basic_quorum','notfound_ok','timeout','include_context']:
            if kwargs.get(attr,''):
                setattr(req,attr,kwargs[attr])
        d = self.__send(code, req)
        d.addCallback(lambda resp: resp) #need to parse and init the result here
        return d

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
        code = pack('B', MSG_CODE_DATATYPE_UPDATE_REQ)
        req = DtUpdateReq()
        req.bucket=datatype.bucket.name
        req.key=datatype.key
        req.type = datatype.bucket.bucket_type
        if datatype.context:
            req.context = datatype.context
        for attr in ['w','dw','pw','timeout','return_body','include_context']:
            if kwargs.get(attr,''):
                setattr(req,attr,kwargs[attr])
        op = dt_codec.encode_operation(datatype,req)
        d = self.__send(code, req)
        d.addCallback(lambda resp: resp) #need to parse and init the result here
        return d


    def put(self, bucket, key, content, vclock=None, **kwargs):
        code = pack('B', MSG_CODE_PUT_REQ)
        request = RpbPutReq()
        request.bucket = bucket
        request.key = key
        request.type = kwargs.pop('bucket_type','default')

        if isinstance(content, str):
            request.content.value = content
        else:
            # assume its a dict
            request.content.value = content['value']  # mandatory

            if 'content_type' in content:
                request.content.content_type = content['content_type']
            if 'charset' in content:
                request.content.charset = content['charset']
            if 'content_encoding' in content:
                request.content.content_encoding = content['content_encoding']
            if 'vtag' in content:
                request.content.vtag = content['vtag']
            if 'last_mod' in content:
                request.content.last_mod = content['last_mod']
            if 'last_mod_usecs' in content:
                request.content.last_mod_usecs = content['last_mod_usecs']
            if 'deleted' in content:
                request.content.deleted = content['deleted']

            # write links if there are any
            if 'links' in content and isinstance(content['links'], list):
                for l in content['links']:
                    link = request.content.links.add()
                    link.bucket, link.key, link.tag = l

            # usermeta
            if 'usermeta' in content and isinstance(content['usermeta'], list):
                for l in content['usermeta']:
                    usermeta = request.content.usermeta.add()
                    usermeta.key, usermeta.value = l

            # indexes
            if 'indexes' in content and isinstance(content['indexes'], list):
                for l in content['indexes']:
                    indexes = request.content.indexes.add()
                    indexes.key, indexes.value = l

        for i in ['w', 'dw', 'pw']:
            if i in kwargs:
                setattr(request, i, self._resolveNums(kwargs[i]))

        params = [
            'if_modified', 'if_not_modified', 'if_none_match',
            'return_head', 'return_body'
        ]

        for i in params:
            if i in kwargs:
                setattr(request, i, kwargs[i])

        if vclock:
            request.vclock = vclock

        return self.__send(code, request)

    def delete(self, bucket, key, **kwargs):
        code = pack('B', MSG_CODE_DEL_REQ)
        request = RpbDelReq()
        request.bucket = bucket
        request.key = key
        request.type = kwargs.pop('bucket_type','default')

        if 'vclock' in kwargs and kwargs['vclock']:
            request.vclock = kwargs['vclock']

        for prop in ['rw', 'r', 'w', 'pr', 'pw', 'dw']:
            if prop in kwargs:
                setattr(request, prop, self._resolveNums(kwargs[prop]))

        return self.__send(code, request)

    def reset_bucket_props(self,bucket):
        code = pack('B', MSG_CODE_RESET_BUCKET_REQ)
        request = RpbResetBucketReq()
        request.bucket = bucket.name
        request.type = bucket.bucket_type
        return self.__send(code,request)

    def search(self, index, query, **kwargs):

        code = pack('B', MSG_CODE_SEARCH_QUERY_REQ)
        request = RpbSearchQueryReq()
        request.index=index
        if isinstance(query,unicode):
            query=query.encode('utf-8')
        request.q=query
        for prop in ['op','sort','filter','presort','df']:
            if prop in kwargs:
                setattr(request,prop,kwargs[prop])
        for prop in ['start','rows']:
            if prop in kwargs:
                setattr(request, prop, self._resolveNums(kwargs[prop]))
        fl = kwargs.get('fl','')
        if fl:
            if isinstance(fl,str) or isinstance(fl,unicode):
                fl=[fl]
            for f in fl:
                request.fl.append(str(f))
        return self.__send(code, request)

    def mapred(self, request,content_type='application/json'):
        self.__mapredList = []
        if content_type != 'application/json':
            raise Exception("Only json request is implemented")
        code = pack('B', MSG_CODE_MAPRED_REQ)
        req = RpbMapRedReq()
        req.request=request
        req.content_type=content_type
        return self.__send(code, req)




    # ------------------------------------------------------------------
    # Bucket Operations .. getKeys, getBuckets, get/set Bucket properties
    # ------------------------------------------------------------------
    def getKeys(self, bucket,bucket_type = 'default'):
        """
        operates different than the other messages, as it returns more than
        one respone .. see stringReceived() for handling
        """
        code = pack('B', MSG_CODE_LIST_KEYS_REQ)
        request = RpbListKeysReq()
        request.bucket = bucket
        request.type = bucket_type
        self.__keyList = []
        return self.__send(code, request)

    def getBuckets(self,bucket_type = 'default'):
        """
        operates different than the other messages, as it returns more than
        one respone .. see stringReceived() for handling
        """
        code = pack('B', MSG_CODE_LIST_BUCKETS_REQ)
        req = RpbListBucketsReq()
        req.type = bucket_type
        return self.__send(code,req)

    def getBucketProperties(self, bucket,bucket_type = 'default'):
        code = pack('B', MSG_CODE_GET_BUCKET_REQ)
        request = RpbGetBucketReq()
        request.bucket = bucket
        #need to add version test here to do backport support
        request.type = bucket_type
        return self.__send(code,request)

    def setBucketProperties(self, bucket, bucket_type = 'default',**kwargs):
        code = pack('B', MSG_CODE_SET_BUCKET_REQ)
        request = RpbSetBucketReq()
        request.bucket = bucket
        #need to add version test here to do backport support
        request.type = bucket_type
        for k, v in kwargs.items():
            if k in ['n_val', 'allow_mult','last_write_wins','precommit','has_precommit','postcommit','has_postcommit','chash_keyfun','linkfun','old_vclock','young_vclock','big_vclock','small_vclock','pr','r','w','pw','dw','rw','basic_quorum','notfound_ok','backend','search','repl','search_index','datatype']:
                setattr(request.props, k, v)
            else:
                raise exceptions.RiakPBCException(
                    "Property not implemented: %s" % k)
        return self.__send(code, request)

    # ------------------------------------------------------------------
    # helper functions, message parser
    # ------------------------------------------------------------------
    def connectionMade(self):
        """
        return the protocol instance to the factory so it
        can be used directly
        """
        self.factory.connected.callback(self)

    def connectionLost(self, reason):
        self.disconnected = True

    def setTimeout(self, t):
        self.timeout = t

    def __send(self, code, request=None):
        """
        helper method for logging, sending and returning the deferred
        """
        if self.debug:
            print "[%s] %s %s" % (
                self.__class__.__name__,
                request.__class__.__name__,
                str(request).replace('\n', ' ')
            )
        if request:
            msg = code + request.SerializeToString()
        else:
            msg = code
        self.sendString(msg)
        self.factory.d = Deferred()
        if self.timeout:
            self.timeoutd = reactor.callLater(self.timeout,
                                              self._triggerTimeout)

        return self.factory.d

    def _triggerTimeout(self):
        if not self.factory.d.called:
            try:
                self.factory.d.errback(exceptions.RequestTimeout('timeout'))
            except Exception, e:
                print "Unable to handle Timeout: %s" % e

    def stringReceived(self, data):
        """
        messages contain as first byte a message type code that is used
        to map to the correct message type in self.riakResponses

        messages that dont have a body to parse return True, those are
        listed in self.nonMessages
        """
        if self.timeoutd and self.timeoutd.active():
            self.timeoutd.cancel()  # stop timeout from beeing raised

        def returnOrRaiseException(msg):
            exc = exceptions.RiakPBCException(msg)
            if self.factory.d.called:
                raise exc
            else:
                self.factory.d.errback(Failure(exc))

        # decode messagetype
        code = unpack('B', data[:1])[0]
        if self.debug:
            print "[%s] stringReceived code %s" % (self.__class__.__name__,
                self.PBMessageTypes[code])

        if code not in self.riakResponses and code not in self.nonMessages:
            returnOrRaiseException('unknown messagetype: %d' % code)

        elif code in self.nonMessages:
            # for instance ping doesnt have a message, so we just return True
            if self.debug:
                print "[%s] stringReceived empty message type %s" % (
                    self.__class__.__name__, self.PBMessageTypes[code])
            if not self.factory.d.called:
                self.factory.d.callback(True)
            return
        elif code == MSG_CODE_ERROR_RESP:
            response = self.riakResponses[code]()
            response.ParseFromString(data[1:])
            returnOrRaiseException('%s (%d)' % (
                response.errmsg, response.errcode)
            )
        elif code == MSG_CODE_MAPRED_RESP:
            # listKeys is special as it returns multiple response messages
            # each message can contain multiple keys
            # the last message contains a optional field "done"
            # so collect all the messages until the last one, then call the
            # callback
            response = RpbMapRedResp()
            response.ParseFromString(data[1:])
            if self.debug:
                print "[%s] %s %s" % (
                        self.__class__.__name__,
                        response.__class__.__name__,
                        str(response).replace('\n', ' ')
                    )

            self.__mapredList.append(response)
            if response.HasField('done') and response.done:
                if not self.factory.d.called:
                    self.factory.d.callback(self.__mapredList)
                    self.__mapredList = []

        elif code == MSG_CODE_INDEX_RESP:
            response = RpbIndexResp()
            response.ParseFromString(data[1:])
            if self.debug:
                print "[%s] %s %s" % (
                        self.__class__.__name__,
                        response.__class__.__name__,
                        str(response).replace('\n', ' ')
                    )

            self.__indexResultList.append(response)
            if response.HasField('done') and response.done:
                if not self.factory.d.called:
                    self.factory.d.callback(self.__indexResultList)
                    self.__indexResultList = []


        elif code == MSG_CODE_LIST_KEYS_RESP:
            # listKeys is special as it returns multiple response messages
            # each message can contain multiple keys
            # the last message contains a optional field "done"
            # so collect all the messages until the last one, then call the
            # callback
            response = RpbListKeysResp()
            response.ParseFromString(data[1:])
            if self.debug:
                print "[%s] %s %s" % (
                        self.__class__.__name__,
                        response.__class__.__name__,
                        str(response).replace('\n', ' ')
                    )

            self.__keyList.extend([x for x in response.keys])
            if response.HasField('done') and response.done:
                if not self.factory.d.called:
                    self.factory.d.callback(self.__keyList)
                    self.__keyList = []
        else:
            # normal handling, pick the message code, call ParseFromString()
            # on it, and return the message
            response = self.riakResponses[code]()
            if len(data) > 1:
                # if there's data, parse it, otherwise return empty object
                response.ParseFromString(data[1:])
                if self.debug:
                    print "[%s] %s %s" % (
                        self.__class__.__name__,
                        response.__class__.__name__,
                        str(response).replace('\n', ' ')
                    )
            if not self.factory.d.called:
                self.factory.d.callback(response)

    def _resolveNums(self, val):
        if isinstance(val, str):
            val = val.lower()
            if val in self.rwNums:
                return self.rwNums[val]
            else:
                raise exceptions.RiakPBCException('invalid value %s' % (val))
        else:
            return val

    def isDisconnected(self):
        return self.disconnected

    @defer.inlineCallbacks
    def quit(self):
        yield self.transport.loseConnection()


class RiakPBCClientFactory(ClientFactory):
    protocol = RiakPBC
    noisy = False

    def __init__(self):
        self.d = Deferred()
        self.connected = Deferred()

    def clientConnectionFailed(self, connector, reason):
        self.connected.errback(reason)


class RiakPBCClient(object):
    def connect(self, host, port):
        factory = RiakPBCClientFactory()
        reactor.connectTCP(host, port, factory)
        return factory.connected
