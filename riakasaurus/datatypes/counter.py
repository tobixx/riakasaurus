from riakasaurus.datatypes.datatype import Datatype
from twisted.internet import defer


class Counter(Datatype):
    """
    A convergent datatype that represents a counter which can be
    incremented or decremented. This type can stand on its own or be
    embedded within a :class:`~riak.datatypes.Map`.
    """

    def __init__(self,*args,**kwargs):
        self._increment = 0
        self._type_error_msg = "Counters can only be integers"
        super(Counter,self).__init__(*args,**kwargs)
        self._value = 0 if self._value==None else long(self._value)

    @Datatype.dirty_value.getter
    def dirty_value(self):
        """
        Gets the value of the counter with local increments applied.
        :rtype: int
        """
        return self.value + self._increment

    def to_op(self):
        """
        Extracts the mutation operation from the counter
        :rtype: int, None
        """
        if not self._increment == 0:
            return self._increment

    def increment(self, amount=1):
        """
        Increments the counter by one or the given amount.
        :param amount: the amount to increment the counter
        :type amount: int
        """
        self._raise_if_badtype(amount)
        self._increment += amount

    def decrement(self, amount=1):
        """
        Decrements the counter by one or the given amount.
        :param amount: the amount to decrement the counter
        :type amount: int
        """
        self._raise_if_badtype(amount)
        self._increment -= amount

    def _check_type(self,new_value):
        return isinstance(new_value, int) or isinstance(new_value,long)

    def _reinit_object(self):
        self._value = self._coerce_value(self.dirty_value)
        self._increment = 0
