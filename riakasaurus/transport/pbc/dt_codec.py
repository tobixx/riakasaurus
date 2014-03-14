from riakasaurus.datatypes import TYPES
from riakasaurus.datatypes import Set,Map,Counter #top class crdt
from riakasaurus.datatypes import Flag,Register #secondary class crdt
from riakasaurus.datatypes import Datatype
import traceback
from riakasaurus.transport.pbc import riak_dt_pb2


CounterOp = riak_dt_pb2.CounterOp
SetOp = riak_dt_pb2.SetOp
MapOp = riak_dt_pb2.MapOp
DtOp = riak_dt_pb2.DtOp

MapField = riak_dt_pb2.MapField
MapEntry = riak_dt_pb2.MapEntry
MapUpdate = riak_dt_pb2.MapUpdate


def encode_operation(datatype,update_req):
    try:
        dt_type = DATATYPE_CLASS_DICT[datatype.__class__.__name__]
        op = DATATYPE_ENCODE_DICT[dt_type](datatype,update_req=update_req)
        return op
    except Exception,e:
        print e
        print traceback.format_exc()


def decode_counter_value(dt_value):
    return long(dt_value)
def decode_set_value(dt_value):
    return frozenset([k for k in dt_value])

def decode_flag_value(dt_value):
    return bool(dt_value)
def decode_register_value(dt_value):
    return str(dt_value)

def decode_map_value(dt_value):
    return decode_map_object(dt_value)

DECODE_FUNC = {
        1 : decode_counter_value,
        2 : decode_set_value,
        3 : decode_register_value,
        4 : decode_flag_value,
        5 : decode_map_value
}

MAP_ENTRY_CODEC = {
        1 : "counter_value" ,
        2 : "set_value"     ,
        3 : "register_value"  ,
        4 : "flag_value"    ,
        5 : "map_value"
}

MAP_ENTRY_TYPE = {
        1 : "counter",
        2 : "set",
        3 : "register",
        4 : "flag",
        5 : "map"
}

def decode_map_entry(map_entry):
    datatype = map_entry.field.type
    value = getattr(map_entry,MAP_ENTRY_CODEC[datatype])
    name = map_entry.field.name
    decode_value = DECODE_FUNC[datatype](value)
    return ((name,MAP_ENTRY_TYPE[datatype]),decode_value)

def decode_map_object(value):
    ret = []
    for v in value: #traverse each map_entry instance
        entry = decode_map_entry(v)
        ret.append(entry)
    return dict(ret)

def decode_dtfetch_response(res,bucket=None,key=None):
    if res.type == 1:
        ret = Counter(res.value.counter_value,context = res.context,bucket = bucket,key = key)
    elif res.type == 2:
        value = frozenset([k for k in res.value.set_value])
        ret = Set(value,context = res.context,bucket = bucket,key = key)
    elif res.type == 3:
        value = decode_map_object( res.value.map_value )
        ret = Map(value,context = res.context,bucket = bucket,key = key)
    return ret


def encode_map_update_operation(op,map_update):
    (action,(key,dt_type),op) = op
    if action != 'update':
        raise Exception("Encode action error,have to be udpate")
    field = map_update.field
    field.name = key
    field.type = DATATYPE_FIELD_DICT[dt_type]
    if dt_type == 'flag':
        map_update.flag_op = 1 if op else 2
    elif dt_type == 'register':
        map_update.register_op = op
    else:
        update_op = getattr(map_update,DATATYPE_MAPFIELD_DICT[dt_type])
        DATATYPE_ENCODE_DICT[dt_type](op,map_update_op = update_op)
    return map_update

def encode_counter_op(datatype,map_update_op = None,update_req = None):
    if isinstance(datatype,Datatype):
        incr = datatype.to_op()
    else:
        incr = datatype
    if incr == 0 or incr == None:
        raise Exception("Counter update cannot be 0")
    if not map_update_op:
        op = update_req.op
        o = op.counter_op
        o.increment = long(incr)
        return op
    else:
        o = map_update_op
        o.increment = incr
        return map_update_op

def encode_set_op(datatype,map_update_op = None,update_req=None):
    if isinstance(datatype,Datatype):
        changes = datatype.to_op()
    else:
        changes = datatype
    add_op = changes.get('adds',[])
    remove_op = changes.get('removes',[])
    if not map_update_op:
        op = update_req.op
        o = op.set_op
        for i in add_op:
            o.adds.append(i)
        for i in remove_op:
            o.removes.append(i)
        return op
    else:
        o = map_update_op
        for i in add_op:
            o.adds.append(i)
        for i in remove_op:
            o.removes.append(i)
        return map_update_op


def encode_map_op(datatype,map_update_op = None,update_req=None):
    if isinstance(datatype,Datatype):
        changes = datatype.to_op()
    else:
        changes = datatype
    if not map_update_op:
        op = update_req.op
        map_op = op.map_op
        for o in changes:
            if len(o) == 2:
                op_name,(key,op_type) = o
                #op_name can be add/remove
                #op_type can be map/set/counter/flag/register
                map_field = getattr(map_op,DATATYPE_OP_DICT[op_name]).add()
                map_field.type = DATATYPE_FIELD_DICT[op_type]
                map_field.name = key
            elif len(o) == 3:
                map_update = map_op.updates.add()
                #op_name should be update
                #op_content maybe need to do derival
                map_update = encode_map_update_operation(o,map_update)
        return op
    else:
        map_op = map_update_op
        for o in changes:
            if len(o) == 2:
                op_name,(key,op_type) = o
                #op_name can be add/remove
                #op_type can be map/set/counter/flag/register
                map_field = getattr(map_op,DATATYPE_OP_DICT[op_name]).add()
                map_field.type = DATATYPE_FIELD_DICT[op_type]
                map_field.name = key
            elif len(o) == 3:
                map_update = map_op.updates.add()
                #op_name should be update
                #op_content maybe need to do derival
                map_update = encode_map_update_operation(o,map_update)
        return map_update_op


DATATYPE_FIELD_DICT = {
    'counter'  : 1,
    'set'      : 2,
    'register' : 3,
    'flag'     : 4,
    'map'      : 5
}
DATATYPE_OP_DICT = {
        'add' : 'adds',
        'remove' : 'removes',
        'update' : 'updates',
}
DATATYPE_CLASS_DICT = {
        'Counter' : 'counter',
        'Set' : 'set',
        'Map' : 'map',
        'Flag' : 'flag',
        'Register' : 'register',
}

DATATYPE_MAPFIELD_DICT = {
        'counter' : 'counter_op',
        'set' : 'set_op',
        'map' : 'map_op',
        'flag' : 'flag_op',
        'register' : 'register_op',
}
DATATYPE_ENCODE_DICT = {
        'counter' : encode_counter_op,
        'set' : encode_set_op,
        'map' : encode_map_op,
}


