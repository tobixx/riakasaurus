from .datatype import Datatype
from .counter import Counter
from .flag import Flag
from .register import Register
from .set import Set
from .map import Map, TYPES

__all__ = ['Datatype', 'Flag', 'Counter', 'Register', 'Set', 'Map', 'TYPES']

DataType = {
        'counter' : Counter,
        'set' : Set,
        'map' : Map,
    }
def new(datatype,bucket,key):
    return DataType[datatype](bucket = bucket,key = key)
