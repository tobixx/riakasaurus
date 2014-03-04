from riakasaurus.transport import HTTPTransport

class RiakSearch(object):
    """
    A wrapper around Riak Search-related client operations. See
    :func:`RiakClient.solr`.
    """

    def __init__(self, client, **unused_args):
        self._client = client

    def add(self, index, *docs):
        """
        Adds documents to a fulltext index. Shortcut and backwards
        compatibility for :func:`RiakClientOperations.fulltext_add`.
        """
        self._client.fulltext_add(index, docs=docs)

    index = add

    def delete(self, index, docs=None, queries=None):
        """
        Removes documents from a fulltext index. Shortcut and backwards
        compatibility for :func:`RiakClientOperations.fulltext_delete`.
        """
        self._client.fulltext_delete(index, docs=docs, queries=queries)

    remove = delete

    def search(self, index, query, **params):
        """
        Searches a fulltext index. Shortcut and backwards
        compatibility for :func:`RiakClientOperations.fulltext_search`.
        """
        return self._client.fulltext_search(index, query, **params)

    select = search
