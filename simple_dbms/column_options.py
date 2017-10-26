class ColumnOptions:
    def __init__(self, nil, pkey):
        """
        Constructs a ColumnOptions object with the specified options.
        :param nil: is column not allowed to store null values?
        :param pkey: is this column the primary key
        """
        self.nil = nil
        self.pkey = pkey

    def is_not_null(self):
        """
        Returns a boolean value indicating whether the column with these
        column options is not allowed to store null values.
        :return: true if the column not allowed to store null values, and
                false otherwise
        """
        return self.nil

    def is_primary_key(self):
        """
        Returns a boolean value indicating whether the column with these
        column options is the primary key.
        :return: true if the column is the primary key, and false otherwise
        """
        return self.pkey
