from collections import Sized
from riakasaurus.datatypes.datatype import Datatype
from twisted.internet import defer


class Register(Sized, Datatype):
    """
    A convergent datatype that represents an opaque string that is set
    with last-write-wins semantics, and may only be embedded in
    :class:`~riak.datatypes.Map` instances.
    """

    def __init__(self,*args,**kwargs):
        self._new_value = None
        self._type_error_msg = "Registers can only be strings"
        super(Register,self).__init__(*args,**kwargs)
        self._value = "" if self._value==None else self._value

    @Datatype.dirty_value.getter
    def dirty_value(self):
        """
        Returns the value of the register with local mutation applied.

        :rtype: str
        """
        if self._new_value is not None:
            return self._new_value[:]
        else:
            return self._value

    @Datatype.value.getter
    def value(self):
        """
        Returns a copy of the original value of the register.

        :rtype: str
        """
        return self._value[:]

    def to_op(self):
        """
        Extracts the mutation operation from the register.

        :rtype: str, None
        """
        if self._new_value is not None:
            return self._new_value

    def set(self, new_value):
        """
        Assigns a new value to the register.

        :param new_value: the new value for the register
        :type new_value: str
        """
        self._raise_if_badtype(new_value)
        self._new_value = new_value

    def __len__(self):
        return len(self.value)

    def _check_type(self,new_value):
        return isinstance(new_value, basestring)


    def _reinit_object(self):
        self._value = self._coerce_value(self.dirty_value)
        self._new_value = None
