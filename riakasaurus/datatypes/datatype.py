DATATYPE_BUCKET_TYPE = {
    'Counter' : 'counters',
    'Set' : 'sets',
    'Map' : 'maps',
}

from twisted.internet import defer

class Datatype(object):
    """
    Base class for all convergent datatype wrappers. You will not use
    this class directly, but it does define some methods are common to
    all datatype wrappers.
    """


    def __init__(self, value=None, context='',bucket = None, key = None):
        if value is not None:
            self._raise_if_badtype(value)
            self._value = self._coerce_value(value)
        else:
            #fetch_datatype here,init if exists,otherwise create new
            self._value = value
        self._context = context
        self._bucket = bucket
        self._key = key
        self._type_error_msg = "Invalid value type"
        self._is_top_class = True if self.__class__.__name__ in DATATYPE_BUCKET_TYPE.keys() else False


    @property
    def value(self):
        """
        The pure, immutable value of this datatype, as a Python value,
        which is unique for each datatype.

        .. note: Do not use this property to mutate data, as it will
           not have any effect. Use the methods of the individual type
           to effect changes. This value is guaranteed to be
           independent of any internal data representation.
        """
        return self._value

    @property
    def bucket(self):
        return self._bucket

    @property
    def key(self):
        return self._key

    @property
    def client(self):
        return self._bucket.client

    @property
    def context(self):
        """
        The opaque context for this type, if it was previously fetched.

        :rtype: str
        """
        return self._context[:]

    def to_op(self):
        """
        Extracts the mutation operation from this datatype, if any.
        Each type must implement this method, returning the
        appropriate operation, or `None` if there is no queued
        mutation.
        """
        raise NotImplementedError

    @property
    def dirty_value(self):
        """
        A view of the value that includes local modifications. Each
        type must implement the getter for this property.

        .. note: Like :attr:`value`, mutating the value of this
           property will have no effect. Use the methods of the
           individual type to effect changes.
        """
        raise NotImplementedError

    def _check_type(self,new_value):
        """
        Checks that initial values of the type are appropriate. Each
        type must implement this method.

        :rtype: bool
        """
        raise NotImplementedError

    def _coerce_value(self,new_value):
        """
        Coerces the input value into the internal representation for
        the type. Datatypes will usually override this method.
        """
        return new_value

    def _raise_if_badtype(self, new_value):
        if not self._check_type(new_value):
            raise TypeError(self._type_error_msg)

    def __str__(self):
        return str(self.value)

    def _reinit_object(self):
        raise NotImplementedError

    @defer.inlineCallbacks
    def update(self,lazy=True,*args,**kwargs):
        '''
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
        '''
        #update here first
        if self.to_op():
            if self._is_top_class:
                yield self.bucket.update_datatype(self,*args,**kwargs)
            #reinit the value
            #if lazy is false, will update the object here
            self._reinit_object()
        else:
            pass 
