from relation_iterator import RelationIterator
from true_expression import TrueExpression
from operation_status import OperationStatus
from data_input_stream import DataInputStream
from insert_row import InsertRow
from column import Column
import simple_dbms


class TableIterator(RelationIterator, object):
    """
    A class that serves as an iterator over some or all of the rows in
    a stored table.  For a given table, there may be more than one
    TableIterator open at the same time -- for example, when performing the
    cross product of a table with itself.
    """

    def __init__(self, stmt, table, eval_where):
        """
        Constructs a TableIterator object for the subset of the specified
        table that is defined by the given SQLStatement.  If the
        SQLStatement has a WHERE clause and the evalWhere parameter has a
        value of true, the iterator will only visit rows that satisfy the
        WHERE clause.
        :param stmt: the SQL statement that defines the subset of the table
        :param table: the table to iterate over
        :param eval_where: should the WHERE clause in stmt be evaluated by this
                iterator?  If this iterator is being used by a higher-level
                iterator, then we can specify false so that the WHERE clause
                will not be evaluated at this level.
        """
        super(TableIterator, self).__init__()
        self.table = table

        # Make sure the table is open.
        if table.get_db() is None:
            raise Exception("table " + table.get_name() + " must be " +
                            "opened before attempting to create an iterator for it")

        # Find all columns from the SQL statement whose values will
        # be obtained using this table iterator, and update their
        # state so that we can get their values as needed.
        table_col = stmt_col = None
        for i in range(0, table.num_columns()):
            table_col = table.get_column(i)
            # check for a match in the SELECT clause
            for j in range(0, stmt.num_columns()):
                stmt_col = stmt.get_column(j)
                if stmt_col.name_matches(table_col, table):
                    stmt_col.use_col_info(table_col)
                    stmt_col.set_table_iterator(self)
            # check for a match in the WHERE clause
            for j in range(0, stmt.num_where_columns()):
                stmt_col = stmt.get_where_column(j)
                if stmt_col.name_matches(table_col, table):
                    stmt_col.use_col_info(table_col)
                    stmt_col.set_table_iterator(self)

        self.cursor = table.get_db().cursor(txn=simple_dbms.SimpleDBMS.get_txn(), flags=0)
        self.key = None
        self.data = None

        self.where = stmt.get_where()
        if not eval_where:
            self.where = None
        if self.where is None:
            self.where = TrueExpression()

        self.num_tuples = 0

    def close(self):
        """
        Closes the iterator, which closes any BDB handles that it is using.
        :return:
        """
        if self.cursor is not None:
            self.cursor.close()
        self.cursor = None

    def first(self):
        """
        Positions the iterator on the first tuple in the relation, without
        taking the a WHERE clause (if any) into effect.

        Because this method ignores the WHERE clause, it should
        ordinarily be used only when you need to reposition the cursor
        at the start of the relation after having completed a previous
        iteration.
        :return: true if the iterator was advanced to the first tuple, and false
                 if there are no tuples to visit
        """
        if self.cursor is None:
            raise Exception("This iterator has been closed")

        ret = self.cursor.first()  # this.cursor.getFirst(this.key, this.data, null);
        if ret == OperationStatus.NOTFOUND:
            return False

        # Only increment num_tuples if the WHERE clause isn't violated.
        if self.where.is_true():
            self.num_tuples += 1

        return True

    def next(self):
        """
        Advances the iterator to the next tuple in the relation.  If
        there is a WHERE clause that limits which tuples should be
        included in the relation, this method will advance the iterator
        to the next tuple that satisfies the WHERE clause.  If the
        iterator is newly created, this method will position it on the
        first tuple in the relation (that satisfies the WHERE clause).
        Provided that the iterator can be positioned on a tuple, the
        count of the number of tuples visited by the iterator is
        incremented.
        :return: true if the iterator was advanced to a new tuple, and false
                if there are no more tuples to visit
        """
        if self.cursor is None:
            raise Exception("this iterator has been closed")

        ret = self.cursor.next()
        if ret is None:
            return False
        self.key = ret[0]
        self.data = ret[1]

        while not self.where.is_true():
            ret = self.cursor.next()
            if ret is None:
                return False
            self.key = ret[0]
            self.data = ret[1]

        self.num_tuples += 1
        return True

    def get_column(self, col_index):
        """
        Gets the column at the specified index in the relation that
        this iterator iterates over.  The leftmost column has an index of 0.
        :param col_index:
        :return: the column
        """
        return self.table.get_column(col_index)

    def get_column_val(self, col_index):
        """
        Gets the value of the column at the specified index in the row
        on which this iterator is currently positioned.  The leftmost
        column has an index of 0.

        This method will unmarshall the relevant bytes from the
        key/data pair and return the corresponding Object -- i.e.,
        an object of type String for CHAR and VARCHAR values, an object
        of type Integer for INTEGER values, or an object of type Double
        for REAL values.
        :param col_index:
        :return: the value of the column
        """
        # Get the specified column and its type.
        col = self.table.get_column(col_index)
        col_type = col.get_type()

        # Create an input stream for the data item in the
        # current key/data pair, and mark the beginning of its buffer.
        data_in = DataInputStream(self.data)

        # Read the appropriate offset from the table of offsets,
        # and handle null values.
        offset_offset = col_index * 4  # the offset of the offset!
        data_in.skip(offset_offset)
        offset = data_in.read_int()
        if offset == InsertRow.IS_NULL:
            return None

        # Prepare the appropriate TupleInput object (for either the
        # key or the data item), and make the variable "in" refer
        # to that object. We also determine the size of the value.
        din = None
        size = -1
        if offset == InsertRow.IS_PKEY:
            din = DataInputStream(self.key)
            size = len(self.key)
        else:
            din = data_in

            if col_type == Column.VARCHAR:
                # Get the next positive offset from the data item,
                # so we can compute the size of the VARCHAR in bytes.
                # We need a loop so that we can skip over special offsets
                # for null values and primary keys.

                next_offset = data_in.read_int()
                while next_offset == InsertRow.IS_PKEY or next_offset == InsertRow.IS_NULL:
                    next_offset = data_in.read_int()
                size = next_offset - offset
            else:
                size = col.get_length()

            # Skip to the appropriate place in the data item.
            # We do this *after* we read all necessary offsets.
            din.reset()
            din.skip(offset)

        # Read the value, and return it as an object of the
        # appropriate type.
        if col_type == Column.INTEGER:
            return din.read_int()
        elif col_type == Column.REAL:
            return din.read_float()
        else:
            # CHAR and VARCHAR
            if size == 0:
                return ""
            else:
                x = bytearray()
                for i in range(0, size):
                    x.append( din.read_byte() )
                return str(x)

    def update_row(self):
        """
        # Updates the row on which the iterator is positioned, according to
        # the update values specified for the Column objects in the Table object
        # associated with this iterator.
        :return:
        """
        # Not yet implemented
        pass

    def delete_row(self):
        """
        Deletes the row on which the iterator is positioned.
        :return:
        """
        # not yet implemented
        pass

    def get_num_tuples(self):
        return self.num_tuples

    def get_num_columns(self):
        return self.table.num_columns()
