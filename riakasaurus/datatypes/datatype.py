class Datatype(object):
    """
    Base class for all convergent datatype wrappers. You will not use
    this class directly, but it does define some methods are common to
    all datatype wrappers.
    """


    def __init__(self, value=None, context=None):
        if value is not None:
            self._raise_if_badtype(value)
            self._value = self._coerce_value(value)
        else:
            self._value = value
        self._context = context
        self._type_error_msg = "Invalid value type"

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
