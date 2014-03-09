from riakasaurus.datatypes.datatype import Datatype


class Counter(Datatype):
    """
    A convergent datatype that represents a counter which can be
    incremented or decremented. This type can stand on its own or be
    embedded within a :class:`~riak.datatypes.Map`.
    """
    _increment = 0
    _type_error_msg = "Counters can only be integers"

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

    @classmethod
    def _check_type(new_value):
        return isinstance(new_value, int)
