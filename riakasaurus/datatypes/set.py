import collections
from riak.datatypes.datatype import Datatype


class Set(collections.Set, Datatype):
    _value = frozenset()
    _adds = set()
    _removes = set()
    _type_error_msg = "Sets can only be iterables of strings"

    @Datatype.dirty_value.getter
    def dirty_value(self):
        """
        Returns a representation of the set with local mutations
        applied.

        :rtype: frozenset
        """
        return frozenset((self.value - self._removes) | self._adds)

    def to_op(self):
        """
        Extracts the modification operation from the set.

        :rtype: dict, None
        """
        if not self._adds and not self._removes:
            return None
        changes = {}
        if self._adds:
            changes['adds'] = list(self._adds)
        if self._removes:
            changes['removes'] = list(self._removes)
        return changes

    # collections.Set API, operates only on the immutable version
    def __contains__(self, element):
        return element in self.value

    def __iter__(self):
        return iter(self.value)

    def __len__(self):
        return len(self.value)

    # Sort of like collections.MutableSet API, without the additional
    # methods.
    def add(self, element):
        """
        Adds an element to the set.

        .. note: You may add elements that already exist in the set.
           This may be used as an "assertion" that the element is a
           member.

        :param element: the element to add
        :type element: str
        """
        self._check_element(element)
        self._adds.add(element)

    def discard(self, element):
        """
        Removes an element from the set.

        .. note: You may remove elements from the set that are not
           present. If the Riak server does not find the element in
           the set, an error may be returned to the client. For
           safety, always submit removal operations with a context.

        :param element: the element to remove
        :type element: str
        """
        self._check_element(element)
        self._removes.add(element)

    @classmethod
    def _coerce_value(new_value):
        return frozenset(new_value)

    @classmethod
    def _check_value(new_value):
        if not isinstance(new_value, collections.Iterable):
            return False
        for element in new_value:
            if not isinstance(element, basestring):
                return False
        return True

    @classmethod
    def _check_element(element):
        if not isinstance(element, basestring):
            raise TypeError("Set elements can only be strings")
