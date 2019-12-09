class LimitClause:
    """
    Used for storing limit related information in SQL statements

    Attributes:
        limit (int): Used for storing limit

        offset (int): used for storing offset
    """

    def __init__(self, limit: int = None, offset: int = None):
        self._limit = limit
        self._offset = offset

    @property
    def limit(self):
        return self._limit

    @property
    def offset(self):
        return self._offset

    def __eq__(self, other):
        if not isinstance(other, LimitClause):
            return False
        return self.offset == other.offset and self.limit == other.limit
