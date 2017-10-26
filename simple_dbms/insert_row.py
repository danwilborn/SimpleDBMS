from data_output_stream import DataOutputStream
from column import Column


class InsertRow:
    # Constants for special offsets
    # The field with this offset is a primary key.
    IS_PKEY = -1

    # The field with this offset has a null value.
    IS_NULL = -2

    def __init__(self, table, values):
        """
        Constructs an InsertRow object for a row containing the specified
        values that is to be inserted in the specified table.
        :param table:
        :param values:
        """
        self._table = table
        self._values = values

        # These objects will be created by the marshall() method.
        self._key = None
        self._data = None

    def marshall(self):
        """
        Takes the collection of values for this InsertRow
        and marshalls them into a key/data pair.
        :return:
        """

    def get_key(self):
        """
        Returns the key in the key/data pair for this row.
        :return: the key
        """
        return self._key

    def get_data(self):
        """
        Returns the data item in the key/data pair for this row.
        :return: the data
        """
        return self._data
