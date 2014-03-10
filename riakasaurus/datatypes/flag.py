from riakasaurus.datatypes.datatype import Datatype


class Flag(Datatype):
    """
    A convergent datatype that represents a boolean value that can be
    enabled or disabled, and may only be embedded in :class:`Map`
    instances.
    """

    def __init__(self,*args,**kwargs):
        super(Flag,self).__init__(*args,**kwargs)
        self._op = None
        self._value = False if self._value==None else self._value
        self._type_error_msg = "Flags can only be booleans"

    @Datatype.dirty_value.getter
    def dirty_value(self):
        """
        Gets the value of the flag with local mutations applied.

        :rtype: bool
        """
        if self._op is not None:
            return self._op
        else:
            return self.value

    def enable(self):
        """
        Turns the flag on, effectively setting its value to 'True'.
        """
        self._op = True

    def disable(self):
        """
        Turns the flag off, effectively setting its value to 'False'.
        """
        self._op = False

    def to_op(self):
        """
        Extracts the mutation operation from the flag.

        :rtype: bool, None
        """
        return self._op

    def _check_type(self,new_value):
        return isinstance(new_value, bool)
