from compare_term import CompareTerm


class Column(CompareTerm, object):
    # A column of type INTEGER
    INTEGER = 0

    # A column of type REAL
    REAL = 1

    # A column of type CHAR
    CHAR = 2

    # A column of type VARCHAR
    VARCHAR = 3

    MINTYPE = 0
    MAXTYPE = 3

    # The print width of an INTEGER column.
    MAX_WIDTH_INTEGER = 11

    # The print width of a REAL column.
    MAX_WIDTH_REAL = 11

    def __init__(self, name=None, table=None, type_=None, length=None, not_null=False, has_primary_key=False,
                 update_val=None, should_update=None, index=None, table_iter=None):
        super(Column, self).__init__()
        self.name = name
        self.table = table
        self.type = type_
        self.length = length
        self.not_null = not_null
        self.has_primary_key = has_primary_key
        self.update_val = update_val
        self.should_update = should_update
        self.index = index  # the index of the column in its table
        self.table_iter = table_iter

    @classmethod
    def from_name(cls, name):
        """
        Constructs a Column object with the specified name from the
        specified table.
        :param name: the name of the column
        :return:
        """
        return cls(name=name)

    @classmethod
    def from_name_table(cls, name, t):
        """
        Constructs a Column object with the specified name from the
        specified table.
        :param name: the name of the column
        :param t: the table to which the column belongs
        :return:
        """
        return cls(name=name, table=t)

    @classmethod
    def from_column(cls, col):
        """
        Constructs a Column object that has the same initial field
        values as the specified column.  Creating a "copy" in this way
        allows us to have a column appear more than once in a cross
        product, if we are crossing a table with itself.
        :param col: the column we want to copy
        :return:
        """
        cls(col.name, col.table, col.type, col.length, col.not_null, col.has_primary_key, col.index, col.update_val,
            col.table_iter)

    def name_matches(self, other, other_table):
        """
        Determines if the column represented by the called object has
        the same name as the column represented by the two parameters.
        Note that we need to specify the other table, because aliases may
        not be reflected in the Table object associated with the Column object.
        :param other: the other column whose name we are comparing against
        :param other_table: the other column's table
        :return: true if the names match, and false otherwise
        """
        # First, check to see if the names of the columns match.
        if self.name != other.name:
            return False

        # If a table wasn't specified for one or both of the columns,
        # then we assume that having the same column name indicates a
        # match.
        if self.table is None or other_table is None:
            return True

        # Compare table names -- or aliases, if present.
        tname1 = alias1 = self.table.get_range_variable()
        tname2 = alias2 = other_table.get_range_variable()
        if alias1 is None:
            tname1 = self.table.get_name()
        if alias2 is None:
            tname2 = other_table.get_name()

        return tname1 == tname2

    def adjust_value(self, val):
        """
        Returns an adjusted value for this column, based on its type
        and length.  This method performs any necessary type convertions
        and length adjustments to ensure that the value is valid for this
        column, and it will throw an <code>IllegalArgumentException</code>
        if the value cannot be converted (e.g., if a string is specified
        for an integer column).
        :param val: the unadjusted value
        :return: the adjusted value
        """
        inte = False

        try:
            val = int(val)
            inte = True
        except:
            pass

        if not inte:
            try:
                val = float(val)
            except:
                pass

        adjusted_val = val
        val_str = None

        if self.is_primary_key() and val is None:
            raise "null value specified for primary key column " + self.name

        if val is None:
            if self.is_not_null():
                raise "null value specified for not-null column " + self.name
            else:
                return val

        if self.type == Column.INTEGER:
            if not isinstance(val, int):
                raise str(val) + ": non-integer value for integer column"
        elif self.type == Column.REAL:
            if isinstance(val, int):
                adjusted_val = float(val)
            elif not isinstance(val, float):
                raise str(val) + ": non-numeric value for real-valued column"
        elif self.type == Column.CHAR:
            val_str = str(val).replace('\'', '')
            val_str_len = len(val_str)
            if val_str_len < self.length:
                padding_len = self.length - val_str_len
                for i in range(0, padding_len):
                    val_str += " "
            elif val_str_len > self.length:
                val_str = val_str[0:self.length]
            adjusted_val = val_str
        elif self.type == Column.VARCHAR:
            val_str = str(val).replace('\'', '')
            if len(val_str) > self.length:
                val_str = val_str[0:self.length]
            adjusted_val = val_str
        return adjusted_val

    def get_value(self):
        """
        Returns the current value of this column, based on the row on which
        the corresponding table iterator is currently positioned.
        :return: the columns current value
        """
        if self.table_iter is None:
            return None
        return self.table_iter.get_column_val(self.index)

    def print_width(self):
        """
        Returns the maximum print width for values from this column.
        :return: the print width
        """
        val_width = 0

        if self.get_val_type() == CompareTerm.INTEGER:
            val_width = Column.MAX_WIDTH_INTEGER
        elif self.get_val_type() == CompareTerm.REAL:
            val_width = Column.MAX_WIDTH_REAL
        elif self.get_val_type() == CompareTerm.STRING:
            val_width = self.length
        else:
            raise AttributeError("unknown type")

        return max(val_width, len(self.name))

    def set_type(self, type):
        """
        Sets the type of this column -- which must be one of the constants
        specified in this class (INTEGER, REAL, CHAR, or VARCHAR).
        :param type: the column's type
        :return:
        """
        if type < Column.MINTYPE or type > Column.MAXTYPE:
            raise "invalid type: " + str(type)

        self.type = type
        if type == Column.INTEGER:
            self.set_val_type(CompareTerm.INTEGER)
            self.length = 4
        elif type == Column.REAL:
            self.set_val_type(CompareTerm.REAL)
            self.length = 8
        else:
            # length will be set using the setLength() method
            self.set_val_type(CompareTerm.STRING)
            self.length = 0

    def set_length(self, length):
        """
        Sets the length of this column.
        :param length: the columns length
        :return:
        """
        if length is None:
            raise AttributeError("length cannot be null")
        if length <= 0:
            raise AttributeError("length must be > 0")
        self.length = int(length)

    def apply_opts(self, opts):
        """
        Applies the specified column options to this column.
        :param opts: the column options
        :return:
        """
        if opts is None:
            return
        if opts.is_not_null():
            self.not_null = True
        if opts.is_primary_key():
            self.has_primary_key = True

    def set_index(self, i):
        """
        Sets the index of this column -- i.e., its index in the
        table to which it belongs.  The leftmost column has an index of 0.
        :param i: the index
        :return:
        """
        self.index = i

    def reset_update_val(self):
        """
        Removes any update value associated with this column, which means
        that an update command will not try to update it unless a new
        update value is specified using <code>setUpdateVal</code>.
        :return:
        """
        self.update_val = None
        self.should_update = False

    def should_update(self):
        """
        Determines whether this column should be updated by an update command
        -- i.e., if it has an up-to-date update value.
        :return: true if the column should be updated, and false otherwise
        """
        return self.should_update

    def set_update_val(self, val):
        """
        Sets the update value associated with this column, which means
        that any update commands will try to update it until the update
        value is removed using <code>resetUpdateVal</code>.
        :param val: the value the column should be assigned
        :return:
        """
        self.update_val = val
        self.should_update = True

    def use_col_info(self, other):
        """
        Gives the column specified by the called object the same
        metadata (type, length, etc.) as the specified other column.
        :param other: the column whose metadata we want to copy
        :return:
        """
        if other is None:
            raise AttributeError("parameter must be non-null")
        self.set_type(other.type)
        self.length = other.length
        self.not_null = other.not_null
        self.has_primary_key = other.has_primary_key
        self.index = other.index

    def set_table_iterator(self, iter):
        """
        Sets the table iterator used to obtain the values of this column.
        :param iter: the table iterator
        :return:
        """
        self.table_iter = iter

    def get_name(self):
        """
        Returns the name of the column.
        :return: The name of the column
        """
        return self.name

    def get_table(self):
        """
        Returns the table to which this column belongs.

        IMPORTANT: if you invoke this method on a column from the SELECT or
        WHERE clause with a name that isn't fully qualified (i.e., that
        doesn't include a table name), this method will return null.
        :return: the table
        """
        return self.table

    def get_type(self):
        """
        Returns the type of the column
        :return: the type (one of the constants defined in this class)
        """
        return self.type

    def get_length(self):
        """
        Returns the length of the column -- i.e., the number of bytes used to
        represent its values.  For VARCHAR columns, this returns the
        the maximum number of bytes.  For all other columns, it represents
        the exact number of bytes.
        :return: the length
        """
        return self.length

    def is_not_null(self):
        """
        Returns a boolean value indicating whether this column is prevented
        from taking on null values.
        :return: true if the column cannot take on null values, and false otherwise
        """
        return self.not_null

    def is_primary_key(self):
        """
        Returns a boolean value indicating whether this column is the
        primary key of its table
        :return: true if the column is the primary key, and false otherwise
        """
        return self.has_primary_key

    def get_index(self):
        """
        Returns the index of the column in the table to which it
        belongs.  The leftmost column has an index of 0.
        :return: the index
        """
        return self.index

    def get_update_val(self):
        """
        Returns the update value associated with this column -- i.e., the value
        it should be assigned by an update command.

        This method should <i>not</i> be used to determine whether a
        column should be updated, since this method will return
        <code>null</code> both when there is no update value and when
        there is an update value of <code>null</code>.  To determine if a
        column should be updated, use the <code>shouldUpdate()</code> method.
        :return: the update val
        """
        return self.update_val

    def get_table_iterator(self):
        """
        Returns the table iterator used to obtain values of this column

        IMPORTANT: if you invoke this method on a Column from a Table
        object's array of column information, it will return null.
        Only Column objects from the SELECT and WHERE clause have
        associated table iterators.  These are set when the TableIterator
        is created.
        :return: the table iterator
        """
        return self.table_iter

    def __repr__(self):
        str_ = ""

        if self.table is not None:
            str_ += self.table.get_name() + "."
        str_ += self.name

        return str_

    def __str__(self):
        str_ = ""

        if self.table is not None:
            str_ += self.table.get_name() + "."
        str_ += self.name

        return str_
